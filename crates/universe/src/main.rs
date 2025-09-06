use clap::Parser;
use url::Url;

use caspers_universe::{Result, Site, SiteId, run_simulation};

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
    let brands = caspers_universe::init::generate_brands();
    let sites = vec![("london", (51.518898098201326, -0.13381370382489707))]
        .into_iter()
        .map(|(name, (lat, long))| {
            let site = Site {
                id: SiteId::from_uri_ref(format!("sites/{name}")).to_string(),
                name: name.to_string(),
                latitude: lat,
                longitude: long,
            };
            site
        })
        .collect();

    run_simulation(sites, brands, 50, path)?;

    Ok(())
}
