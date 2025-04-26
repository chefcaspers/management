use chrono::{DateTime, Duration, Utc};
use clap::Parser;
use tabled::Tabled;
use url::Url;

use caspers_universe::{KitchenStats, SimulationBuilder};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,

    #[arg(short, long, default_value_t = 1000)]
    population: u32,
}

#[derive(Debug, Clone, Tabled)]
struct Report {
    timestamp: DateTime<Utc>,
    #[tabled(inline)]
    stats: KitchenStats,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let path = Url::parse("file:///Users/robert.pack/code/management/notebooks/data/")?;
    let mut simulation = SimulationBuilder::new();
    simulation
        .with_result_storage_location(path)
        .with_snapshot_interval(Duration::minutes(30));

    for brand in caspers_universe::init::generate_brands() {
        simulation.with_brand(brand);
    }

    for (name, (lat, long)) in [("london", (51.518898098201326, -0.13381370382489707))] {
        simulation.with_site(name, lat, long);
    }

    let mut simulation = simulation.build()?;
    simulation.run(100)?;

    // let table = Table::new(stats)
    //     .with(Style::modern_rounded())
    //     .modify(Columns::single(0), Color::FG_RED)
    //     .modify(Columns::single(1), Color::FG_BLUE)
    //     .modify(Columns::new(2..), Color::FG_GREEN)
    //     .modify(Rows::new(0..), Height::limit(5))
    //     .to_string();

    Ok(())
}
