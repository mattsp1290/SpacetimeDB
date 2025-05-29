use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context;
use duct::cmd;
use itertools::Itertools;

pub(crate) fn build_python(project_path: &Path) -> anyhow::Result<PathBuf> {
    // Check if this is a valid Python project
    let has_pyproject = project_path.join("pyproject.toml").exists();
    let has_setup_py = project_path.join("setup.py").exists();
    let has_requirements = project_path.join("requirements.txt").exists();
    
    if !has_pyproject && !has_setup_py {
        anyhow::bail!("No pyproject.toml or setup.py found. This doesn't appear to be a Python project.");
    }
    
    // Check for main module file
    let src_dir = project_path.join("src");
    let has_src_structure = src_dir.exists() && src_dir.is_dir();
    
    if !has_src_structure {
        anyhow::bail!("No src/ directory found. Python modules should have their code in a src/ directory.");
    }
    
    // Validate Python installation
    let python_cmd = if cfg!(windows) { "python" } else { "python3" };
    match Command::new(python_cmd).arg("--version").output() {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout);
            eprintln!("Using Python: {}", version.trim());
        }
        _ => {
            anyhow::bail!("Python is not installed or not in PATH. Please install Python 3.8 or later.");
        }
    }
    
    // Note: Python-to-WASM compilation is not yet supported
    // This is a placeholder that validates the project structure
    eprintln!("Note: Python module compilation to WASM is not yet supported.");
    eprintln!("Python client code generation is available via 'spacetime generate --lang python'");
    
    // For now, we'll create a marker file to indicate this is a Python SpacetimeDB module
    let marker_path = project_path.join(".spacetimedb-python-module");
    std::fs::write(&marker_path, "This is a SpacetimeDB Python module project\n")
        .context("Failed to create module marker file")?;
    
    // Return the marker file as the "built" artifact
    Ok(marker_path)
}

pub(crate) fn python_format(files: impl IntoIterator<Item = PathBuf>) -> anyhow::Result<()> {
    let files: Vec<PathBuf> = files.into_iter().collect();
    if files.is_empty() {
        return Ok(());
    }

    // Try to use black for formatting
    if has_black() {
        format_with_black(&files)?;
    } else if has_autopep8() {
        format_with_autopep8(&files)?;
    } else {
        eprintln!("Warning: No Python formatter found. Install 'black' or 'autopep8' for automatic formatting.");
        return Ok(());
    }

    // Try to organize imports with isort
    if has_isort() {
        organize_imports_with_isort(&files)?;
    }

    Ok(())
}

fn has_black() -> bool {
    Command::new("black")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn has_autopep8() -> bool {
    Command::new("autopep8")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn has_isort() -> bool {
    Command::new("isort")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn format_with_black(files: &[PathBuf]) -> anyhow::Result<()> {
    cmd(
        "black",
        itertools::chain(
            ["--line-length", "88", "--target-version", "py38"]
                .into_iter()
                .map_into::<OsString>(),
            files.iter().map_into(),
        ),
    )
    .run()
    .context("Failed to run black formatter")?;
    Ok(())
}

fn format_with_autopep8(files: &[PathBuf]) -> anyhow::Result<()> {
    cmd(
        "autopep8",
        itertools::chain(
            ["--in-place", "--aggressive", "--aggressive"]
                .into_iter()
                .map_into::<OsString>(),
            files.iter().map_into(),
        ),
    )
    .run()
    .context("Failed to run autopep8 formatter")?;
    Ok(())
}

fn organize_imports_with_isort(files: &[PathBuf]) -> anyhow::Result<()> {
    cmd(
        "isort",
        itertools::chain(
            ["--profile", "black"].into_iter().map_into::<OsString>(),
            files.iter().map_into(),
        ),
    )
    .run()
    .context("Failed to run isort import organizer")?;
    Ok(())
}
