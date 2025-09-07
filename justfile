set dotenv-load := true

# Show available commands
default:
    @just --list --justfile {{ justfile() }}

# run protobuf code geneartion
[group('build')]
generate:
    buf generate proto
    cargo clippy --fix --allow-dirty
    cargo fmt --all

[group('build')]
build-py:
    uvx --from 'maturin[zig]' maturin develop -m python/Cargo.toml

run:
    cargo run --bin caspers-universe -- --duration 100

# run marimo notebook server for interactive data exploration
scratch:
    uv run --directory {{ source_directory() }}/notebooks marimo edit explore.py
