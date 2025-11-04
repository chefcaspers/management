use axum::{Router, response::Json, routing::get};
use serde_json::{Value, json};
use std::{net::SocketAddr, path::PathBuf};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub async fn main() {
    // Initialize tracing
    // tracing_subscriber::registry()
    //     .with(
    //         tracing_subscriber::EnvFilter::try_from_default_env()
    //             .unwrap_or_else(|_| "capers_universe_server=debug,tower_http=debug".into()),
    //     )
    //     .with(tracing_subscriber::fmt::layer())
    //     .init();

    // Get the assets directory path relative to the crate root
    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let index_path = assets_dir.join("index.html");

    tracing::info!("Serving static files from: {:?}", assets_dir);
    tracing::info!("Index file path: {:?}", index_path);

    // Create the static file service
    let serve_dir = ServeDir::new(&assets_dir).not_found_service(ServeFile::new(&index_path));

    // Build application routes
    let app = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/simulation", get(simulation_status))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .fallback_service(serve_dir);

    // Run server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("Starting server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
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
