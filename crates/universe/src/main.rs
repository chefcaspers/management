use clap::Parser;
use url::Url;

use caspers_universe::{Result, load_simulation_setup, run_simulation, run_simulation_from};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,

    #[arg(short, long, default_value_t = 1000)]
    population: u32,

    #[arg(short, long, default_value_t = 100)]
    duration: usize,

    #[arg(long)]
    /// Path where basic simulation setup is stored.
    setup_path: String,

    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let data_path = Url::parse("file:///Users/robert.pack/code/management/notebooks/data/")?;
    let routing_path = Url::parse("file:///Users/robert.pack/code/management/data/routing")?;

    let setup_path = std::fs::canonicalize(cli.setup_path)?;
    let path = Url::from_directory_path(setup_path).expect("Path to be valid directory");
    let setup = load_simulation_setup(&path, None::<(&str, &str)>).await?;

    run_simulation(setup, cli.duration, data_path, routing_path, cli.dry_run).await?;
    // run_simulation_from(setup, cli.duration, data_path, routing_path, cli.dry_run).await?;

    Ok(())
}
