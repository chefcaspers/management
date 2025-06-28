set dotenv-load := true

# Show available commands
default:
    @just --list --justfile {{ justfile() }}

generate:
    buf generate proto

    # npx -y @redocly/cli bundle --remove-unused-components openapi/openapi.yaml > tmp.yaml
    # mv tmp.yaml openapi/openapi.yaml
    cargo clippy --fix --allow-dirty --allow-staged
    cargo fmt --all

run:
    cargo run --bin caspers-universe -- --location-count 1

ice:
    curl -X GET -H "Authorization: Bearer $DATABRICKS_PAT" -H "Accept: application/json" \
    https://devrel-caspers.cloud.databricks.com/api/2.1/unity-catalog/iceberg/v1/catalogs/caspers_abm/namespaces/experiments/tables/positions_iceberg

cred:
    curl -X POST -H "Authorization: Bearer $DATABRICKS_PAT" -H "Accept: application/json" \
    https://devrel-caspers.cloud.databricks.com/api/2.0/unity-catalog/temporary-table-credentials \
    -d '{"operation": "READ", "table_id": "65e0aeab-9d11-4818-8b51-a24e848b330a"}'  
