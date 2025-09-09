use clap::Parser;
use url::Url;

use caspers_universe::{Result, load_simulation_setup, run_simulation};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,

    #[arg(short, long, default_value_t = 1000)]
    population: u32,

    #[arg(short, long, default_value_t = 100)]
    duration: usize,

    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let path = Url::parse("file:///Users/robert.pack/code/management/data")?;
    let setup = load_simulation_setup(&path, None::<(&str, &str)>).await?;

    let data_path = Url::parse("file:///Users/robert.pack/code/management/notebooks/data/")?;
    let routing_path = Url::parse("file:///Users/robert.pack/code/management/data/routing")?;
    run_simulation(setup, cli.duration, data_path, routing_path, cli.dry_run).await?;

    Ok(())
}
