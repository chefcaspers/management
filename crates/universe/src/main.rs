use chrono::Duration;
use clap::Parser;
use url::Url;

use caspers_universe::{Result, SimulationBuilder, Site, SiteId};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,

    #[arg(short, long, default_value_t = 1000)]
    population: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let _cli = Cli::parse();

    let path = Url::parse("file:///Users/robert.pack/code/management/notebooks/data/")?;
    let simulation = SimulationBuilder::new()
        .with_result_storage_location(path)
        .with_snapshot_interval(Duration::minutes(10))
        .with_time_increment(Duration::minutes(1));

    let simulation = caspers_universe::init::generate_brands()
        .into_iter()
        .fold(simulation, |sim, brand| sim.with_brand(brand));

    let sites = vec![("london", (51.518898098201326, -0.13381370382489707))];
    let simulation = sites
        .into_iter()
        .fold(simulation, |sim, (name, (lat, long))| {
            let site = Site {
                id: SiteId::from_uri_ref(format!("sites/{name}")).to_string(),
                name: name.to_string(),
                latitude: lat,
                longitude: long,
            };
            sim.with_site(site)
        });

    let mut simulation = simulation.build()?;

    simulation.run(500)?;

    Ok(())
}
