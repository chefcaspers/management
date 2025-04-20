use chrono::{DateTime, Utc};
use clap::Parser;
use tabled::{
    Table, Tabled,
    settings::{
        Color, Height, Style,
        object::{Columns, Rows},
    },
};

use caspers_universe::KitchenStats;

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

    let mut simulation = caspers_universe::SimulationBuilder::new();

    for brand in caspers_universe::init::generate_brands() {
        simulation.with_brand(brand);
    }

    let mut simulation = simulation.build()?;
    simulation.run(10)?;

    // let table = Table::new(stats)
    //     .with(Style::modern_rounded())
    //     .modify(Columns::single(0), Color::FG_RED)
    //     .modify(Columns::single(1), Color::FG_BLUE)
    //     .modify(Columns::new(2..), Color::FG_GREEN)
    //     .modify(Rows::new(0..), Height::limit(5))
    //     .to_string();

    Ok(())
}
