use axum::{Router, response::Json, routing::get};
use caspers_universe::Result;
use serde_json::{Value, json};
use std::{net::SocketAddr, path::PathBuf};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use crate::ServerArgs;

pub(super) async fn handle(args: ServerArgs) -> Result<()> {
    // Get the assets directory path relative to the crate root
    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/ui");
    let index_path = assets_dir.join("index.html");
    let ui_service = ServeDir::new(&assets_dir).not_found_service(ServeFile::new(&index_path));
    tracing::info!(target: "caspers::server", "Serving ui static files from: {:?}", assets_dir);

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/docs");
    let index_path = assets_dir.join("index.html");
    let docs_service = ServeDir::new(&assets_dir).not_found_service(ServeFile::new(&index_path));
    tracing::info!(target: "caspers::server", "Serving docs static files from: {:?}", assets_dir);

    // Build application routes
    let app = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/simulation", get(simulation_status))
        .nest_service("/docs", docs_service)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .fallback_service(ui_service);

    let addr: SocketAddr = args
        .server
        .parse()
        .map_err(|e| caspers_universe::Error::Generic(Box::new(e)))?;
    tracing::info!(target: "caspers::server", "Starting server on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_check() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "caspers-universe-server"
    }))
}

async fn simulation_status() -> Json<Value> {
    Json(json!({
        "status": "idle",
        "message": "No simulation currently running"
    }))
}
