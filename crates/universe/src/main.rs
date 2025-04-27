use chrono::Duration;
use clap::Parser;
use url::Url;

use caspers_universe::{Result, SimulationBuilder};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,

    #[arg(short, long, default_value_t = 1000)]
    population: u32,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let _cli = Cli::parse();

    let path = Url::parse("file:///Users/robert.pack/code/management/notebooks/data/")?;
    let mut simulation = SimulationBuilder::new();
    simulation
        .with_result_storage_location(path)
        .with_snapshot_interval(Duration::minutes(30));

    for brand in caspers_universe::init::generate_brands() {
        simulation.with_brand(brand);
    }

    {
        let (name, (lat, long)) = ("london", (51.518898098201326, -0.13381370382489707));
        simulation.with_site(name, lat, long);
    }
    let mut simulation = simulation.build()?;

    simulation.run(100)?;

    Ok(())
}
