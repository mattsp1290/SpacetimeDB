# SpacetimeDB Python Bridge

A WASM-based bridge that enables Python to be used as a first-class language for SpacetimeDB server modules using the Pyodide runtime.

## Overview

This crate implements the foundational infrastructure for running Python code as SpacetimeDB server modules. It bridges the gap between SpacetimeDB's WASM module interface and Python code running in the Pyodide WebAssembly Python runtime.

## Architecture

```
┌─────────────────────────────────────┐
│ SpacetimeDB WASM Host               │
├─────────────────────────────────────┤
│ SpacetimeDB-Python Bridge (Rust)    │  ← This crate
├─────────────────────────────────────┤
│ Standard Pyodide Runtime (6.5MB)    │  ← Pyodide WASM
├─────────────────────────────────────┤
│ Python Server Code + Bindings       │  ← User's Python code
└─────────────────────────────────────┘
```

## Components

### Core Modules

- **`lib.rs`** - Main bridge implementation and SpacetimeDB WASM ABI
- **`pyodide_runtime.rs`** - Pyodide integration and Python execution
- **`bsatn_bridge.rs`** - BSATN ↔ Python serialization bridge
- **`error.rs`** - Comprehensive error handling and recovery

### Key Features

- **Pyodide Integration**: Embeds and manages the Pyodide Python runtime
- **BSATN Serialization**: Bidirectional conversion between SpacetimeDB's BSATN format and Python objects
- **SpacetimeDB ABI**: Implements required WASM exports for SpacetimeDB module interface
- **Python Decorators**: Provides `@table` and `@reducer` decorators for Python server modules
- **Error Handling**: Comprehensive error types with user-friendly messages and recovery suggestions

## Python API

The bridge provides a Python API that mirrors SpacetimeDB's concepts:

### Table Definition

```python
from spacetimedb_server import table, reducer, ReducerContext

@table
class Player:
    identity: str           # Primary key
    name: str
    score: int = 0         # Default value
    active: bool = True
```

### Reducer Definition

```python
@reducer
def join_game(ctx: ReducerContext, player_name: str):
    # Create new player
    player = Player(
        identity=str(ctx.sender),
        name=player_name,
        score=0,
        active=True
    )
    
    # Insert into database
    ctx.db.get_table(Player).insert(player)
    
    ctx.log(f"Player {player_name} joined")
```

### Database Operations

```python
# Insert
ctx.db.get_table(Player).insert(player)

# Find by primary key
player = ctx.db.get_table(Player).find_by_primary_key(identity)

# Scan table
players = ctx.db.get_table(Player).scan()

# Delete with filters
deleted = ctx.db.get_table(Player).delete(active=False)
```

## Implementation Status

### ✅ Completed (Phase 1)

- [x] Basic Rust bridge structure with WASM bindings
- [x] Pyodide runtime integration and initialization
- [x] Core BSATN ↔ Python serialization bridge
- [x] SpacetimeDB WASM ABI implementation
- [x] Python decorator system (`@table`, `@reducer`)
- [x] Basic database interface and context objects
- [x] Comprehensive error handling
- [x] Example Python server module

### 🚧 TODO (Future Phases)

- [ ] Advanced table operations (indexing, complex queries)
- [ ] Scheduled reducers and timer system
- [ ] CLI integration for Python module publishing
- [ ] Performance optimizations and caching
- [ ] Full BSATN compatibility and type safety
- [ ] Async Pyodide loading and initialization
- [ ] Python package distribution and installation

## Example Usage

See [`examples/simple_game.py`](examples/simple_game.py) for a complete example of a Python-based SpacetimeDB server module that implements a simple game with:

- Player management (join/leave)
- Score tracking and level progression
- Game state management
- Event logging
- Leaderboards and statistics

## Building

```bash
# From SpacetimeDB root directory
cd crates/python-bridge
cargo build --target wasm32-unknown-unknown
```

## Testing

```bash
# Run Rust tests
cargo test

# Run WASM tests (requires wasm-pack)
wasm-pack test --node
```

## Dependencies

### Rust Dependencies

- `spacetimedb-lib` - Core SpacetimeDB types and utilities
- `spacetimedb-sats` - SATS type system and BSATN serialization
- `wasm-bindgen` - Rust/WASM/JS interop
- `js-sys` - JavaScript API bindings
- `web-sys` - Web API bindings
- `serde` - Serialization framework

### Runtime Dependencies

- **Pyodide** - Python runtime for WebAssembly (6.5MB)
- **Python 3.11+** - Target Python version

## Design Decisions

### Why Pyodide?

1. **Complete Python Runtime**: Full standard library and package ecosystem
2. **WebAssembly Native**: Designed for WASM environments
3. **No Modifications Required**: Use Pyodide as-is without patches
4. **Package Support**: Can install Python packages via micropip
5. **Mature Project**: Well-tested and actively maintained

### BSATN Serialization Strategy

- Use JSON as intermediate format for development phase
- Plan migration to native BSATN for production
- Support all SpacetimeDB algebraic types
- Handle Python type hints and annotations

### Error Handling Philosophy

- Provide clear, actionable error messages
- Include context and suggestions for common issues
- Support error recovery where possible
- Log errors with appropriate detail levels

## Performance Considerations

### Bundle Size
- Pyodide runtime: ~6.5MB (acceptable for server modules)
- Bridge overhead: <100KB
- Python code: Variable based on module complexity

### Startup Time
- Target: <500ms initialization
- Pyodide loading: ~200ms
- Bridge initialization: ~50ms
- Python module loading: Variable

### Runtime Performance
- JavaScript/Python boundary overhead
- BSATN serialization cost
- Memory management across three runtimes (Rust/JS/Python)

## Security Considerations

- Sandboxed execution in WASM environment
- No direct file system or network access from Python
- All I/O mediated through SpacetimeDB APIs
- Memory isolation between modules

## Contributing

1. Follow Rust coding standards and conventions
2. Add tests for new functionality
3. Update documentation for API changes
4. Consider performance impact of changes
5. Test with example Python modules

## License

Licensed under the same terms as SpacetimeDB.
