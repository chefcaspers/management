use chrono::{DateTime, Utc};
use clap::Parser;
use tabled::{
    Table, Tabled,
    settings::{
        Color, Height, Style,
        object::{Columns, Rows},
    },
};

use caspers_universe::{KitchenStats, Simulatable, state};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,
}

#[derive(Debug, Clone, Tabled)]
struct Report {
    timestamp: DateTime<Utc>,
    #[tabled(inline)]
    stats: KitchenStats,
}

fn main() {
    let cli = Cli::parse();

    let brands = caspers_universe::init::generate_brands();
    let mut location = caspers_universe::init::generate_site("site-1", brands.as_ref());

    let mut state = state::State::try_new().unwrap();

    let mut stats = Vec::new();
    for it in 0..1000 {
        if it % 100 == 0 {
            stats.push(Report {
                timestamp: state.current_time(),
                stats: location.total_kitchen_stats(),
            });
        }
        location.step(&state);
        state.step();
    }

    let table = Table::new(stats)
        .with(Style::modern_rounded())
        .modify(Columns::single(0), Color::FG_RED)
        .modify(Columns::single(1), Color::FG_BLUE)
        .modify(Columns::new(2..), Color::FG_GREEN)
        .modify(Rows::new(0..), Height::limit(5))
        .to_string();

    println!("{}", table);
}
