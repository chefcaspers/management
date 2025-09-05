set dotenv-load := true

# Show available commands
default:
    @just --list --justfile {{ justfile() }}

# run protobuf code geneartion
generate:
    buf generate proto
    cargo clippy --fix --allow-dirty
    cargo fmt --all

run:
    cargo run --bin caspers-universe -- --location-count 1

cred:
    curl -X POST -H "Authorization: Bearer $DATABRICKS_PAT" -H "Accept: application/json" \
    https://devrel-caspers.cloud.databricks.com/api/2.0/unity-catalog/temporary-table-credentials \
    -d '{"operation": "READ", "table_id": "65e0aeab-9d11-4818-8b51-a24e848b330a"}'

# run marimo notebook server for interactive data exploration
scratch:
    uv run --directory {{ source_directory() }}/notebooks marimo edit explore.py
