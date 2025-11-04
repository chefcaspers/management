use axum::{
    routing::get,
    Router,
    response::Json,
};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tower_http::{
    cors::CorsLayer,
    services::ServeDir,
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "capers_universe_server=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Build application routes
    let app = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/simulation", get(simulation_status))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        // Serve static files from the UI build directory
        .nest_service("/", ServeDir::new("ui/dist"));

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
