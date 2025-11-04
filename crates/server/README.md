# Chef Casper's Universe Server

An Axum-based web server that runs the Chef Casper's ghost kitchen simulation and serves the web UI.

## Features

- **RESTful API** for simulation control and monitoring
- **Static file serving** for the React UI
- **CORS support** for development
- **Request tracing** with configurable log levels

## Getting Started

### Development

1. **Install UI dependencies:**
   ```bash
   just install-ui
   ```

2. **Run UI dev server (with hot reload):**
   ```bash
   just dev-ui
   ```
   
   The UI will be available at `http://localhost:5173` with API proxying to the backend.

3. **Run the server (in a separate terminal):**
   ```bash
   just server
   ```
   
   The server will start on `http://localhost:3000`.

### Production

1. **Build the UI:**
   ```bash
   just build-ui
   ```

2. **Run the server:**
   ```bash
   just server
   ```

   The server will serve both the API and the built UI at `http://localhost:3000`.

## API Endpoints

### Health Check
```
GET /api/health
```

Returns server health status.

### Simulation Status
```
GET /api/simulation
```

Returns the current simulation status.

## Architecture

```
┌─────────────┐
│   Browser   │
└──────┬──────┘
       │
       │ HTTP
       │
┌──────▼──────────────────────┐
│     Axum Server             │
│  ┌────────────────────────┐ │
│  │  API Routes (/api/*)   │ │
│  └────────────────────────┘ │
│  ┌────────────────────────┐ │
│  │  Static Files (/)      │ │
│  └────────────────────────┘ │
└─────────────────────────────┘
```

## Dependencies

- **axum** - Web framework
- **tower-http** - HTTP middleware (CORS, file serving, tracing)
- **tokio** - Async runtime
- **tracing** - Structured logging

## Configuration

The server can be configured via environment variables:

- `RUST_LOG` - Log level (default: `capers_universe_server=debug,tower_http=debug`)

Example:
```bash
RUST_LOG=info cargo run --bin capers-universe-server
```

