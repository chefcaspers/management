set dotenv-load := true

# Show available commands
default:
    @just --list --justfile {{ justfile() }}

# run protobuf code generation
[group('build')]
generate:
    buf generate proto
    cargo clippy --fix --allow-dirty
    cargo fmt --all

[group('build')]
build-py:
    uvx --from 'maturin[zig]' maturin develop -m python/Cargo.toml

run duration='100':
    cargo run --bin caspers -- run --duration {{duration}} --working-directory .caspers/

# run marimo notebook server for interactive data exploration
scratch:
    uv run --directory {{ source_directory() }}/notebooks marimo edit explore.py

fmt:
    cargo fmt --all
    buf format proto/ --write
    uvx ruff format .

pre-commit:
    uvx pre-commit run --all-files

docs:
  npm -w docs run dev

run-tracing:
    docker run -d --name jaeger \
      -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
      -e COLLECTOR_OTLP_ENABLED=true \
      -p 6831:6831/udp \
      -p 6832:6832/udp \
      -p 5778:5778 \
      -p 16686:16686 \
      -p 4317:4317 \
      -p 4318:4318 \
      -p 14250:14250 \
      -p 14268:14268 \
      -p 14269:14269 \
      -p 9411:9411 \
      jaegertracing/all-in-one:latest
