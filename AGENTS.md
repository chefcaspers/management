# AI Agent Guide for Chef Casper's Management System Simulation

## Project Overview

This is a **mixed Rust/Python simulation project** that generates realistic data
for a fictional ghost kitchen management company. The simulation uses agent-based
modeling to simulate restaurant operations, customer behavior, and delivery logistics.

### Core Technologies
- **Rust**: Main simulation engine using Apache Arrow for data management
- **Python**: Data analysis, notebooks, and visualization
- **Protocol Buffers**: Data schema definitions and code generation
- **Apache Arrow**: Columnar data structures and operations
- **GeoArrow**: Geospatial data processing

## Project Structure

```
management/
├── crates/universe/           # Main Rust simulation engine
│   ├── src/models/           # Data models (some generated from protobuf)
│   ├── src/agents/           # Agent implementations
│   ├── src/state/            # Simulation state management
│   └── src/simulation/       # Simulation orchestration
├── proto/                    # Protocol buffer definitions
├── python/                   # Python bindings and utilities
├── notebooks/                # Jupyter/Marimo notebooks for analysis
└── data/                     # Sample/generated data
```

## Key Concepts

### Business Domain
- **Ghost Kitchens**: Delivery-only restaurant facilities
- **Vendors**: Companies that rent kitchen space
- **Brands**: Restaurant brands operated by vendors
- **Sites/Locations**: Physical buildings with multiple kitchens
- **Menu Items**: Food products with preparation requirements

### Simulation Architecture
- **Agent-Based**: Individual entities (customers, kitchen staff, drivers) act autonomously
- **Time-Based**: Discrete time steps (typically 1-minute increments)
- **Geospatial**: Uses H3 indexing for location-based routing and movement
- **Data-Driven**: Heavy use of Datafusion and Apache Arrow for efficient data operations

## Working with This Codebase

### 🚨 Critical Rules

1. **NEVER modify generated code** in `./crates/universe/src/models/gen/`
2. **Always regenerate protobuf code** after proto changes: `just generate`
3. **Use workspace dependencies** defined in root `Cargo.toml`
4. **Follow the module organization** described below

### Common Tasks

#### Code Generation
```bash
just generate          # Regenerate protobuf code and format
```

#### Building
```bash
cargo build            # Build Rust components
just build-py          # Build Python bindings
```

#### Running Simulation
```bash
just run               # Run simulation with default parameters
cargo run --bin caspers-universe -- --duration 500 --dry-run
```

## Data Flow

1. **Setup Phase**: Load configuration from JSON files (sites, brands, menu items)
2. **Initialization**: Create initial agent populations and state
3. **Simulation Loop**:
   - Agents make decisions based on current state
   - State updates are applied using Arrow operations
   - Snapshots are saved at intervals
4. **Output**: Results saved as Parquet files for analysis

## Performance Considerations

- **Arrow Operations**: Prefer vectorized operations over row-by-row processing
- **Memory Management**: Be mindful of large datasets in memory
- **Geospatial Queries**: Use H3 indexing for efficient spatial lookups
- **Time Complexity**: Consider simulation duration impact on performance

## Common Pitfalls

1. **Generated Code Modifications**: Will be overwritten on next generation
2. **Arrow Schema Mismatches**: Ensure consistent schemas across datasets
3. **Time Zone Handling**: Be explicit about time zones in temporal operations
4. **Memory Leaks**: Monitor memory usage with large simulations
5. **Dependency Conflicts**: Use workspace dependencies to maintain consistency

## Getting Help

1. **Architecture**: See `ARCHITECTURE.md` for high-level design
2. **Business Logic**: See `README.md` for entity relationships
3. **Build System**: See `justfile` for available commands
4. **Dependencies**: Check workspace `Cargo.toml` for version constraints

## File Naming Conventions

- **Rust modules**: `snake_case`
- **Proto files**: `snake_case.proto`
- **Generated files**: Follow protobuf naming conventions
- **Data files**: Use descriptive names with appropriate extensions (`.parquet`, `.json`)

Remember: This simulation generates realistic data for testing and development. Always consider the business context when making technical decisions.