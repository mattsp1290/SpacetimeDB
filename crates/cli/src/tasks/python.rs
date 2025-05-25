use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context;
use duct::cmd;
use itertools::Itertools;

pub(crate) fn build_python(project_path: &Path) -> anyhow::Result<PathBuf> {
    // For Python modules, we don't build a binary like Rust/C#/Go
    // Instead, we look for the module definition or return the project path
    // This is a placeholder implementation - actual Python module building
    // would need to be implemented based on SpacetimeDB's Python module format
    
    // Check if this is a valid Python project
    if !project_path.join("pyproject.toml").exists() && !project_path.join("setup.py").exists() {
        anyhow::bail!("No pyproject.toml or setup.py found. This doesn't appear to be a Python project.");
    }
    
    // For now, return the project path as the "built" artifact
    // In a real implementation, this would compile/prepare the Python module
    Ok(project_path.to_path_buf())
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
