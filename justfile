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
    cargo run --bin caspers-universe -- run --duration 100 --setup-path data/

# run marimo notebook server for interactive data exploration
scratch:
    uv run --directory {{ source_directory() }}/notebooks marimo edit explore.py

fmt:
    cargo fmt --all
    buf format proto/ --write
    uvx ruff format .

docs:
  npm -w docs run dev
