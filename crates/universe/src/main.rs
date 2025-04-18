use clap::Parser;
use tabled::{
    Table,
    settings::{
        Color, Height, Style,
        object::{Columns, Rows},
    },
};

use caspers_universe::{Simulatable, state};

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    location_count: u32,
}

fn main() {
    let cli = Cli::parse();

    let brands = state::get_brands();
    let mut location = state::generate_location("location-1", brands.as_ref());

    let mut state = state::State::try_new().unwrap();

    for _ in 0..1000 {
        location.step(&state);
        state.step();
    }

    let table = Table::new(location.kitchen_stats())
        .with(Style::modern_rounded())
        .modify(Columns::single(0), Color::FG_RED)
        .modify(Columns::single(1), Color::FG_BLUE)
        .modify(Columns::new(2..), Color::FG_GREEN)
        .modify(Rows::new(0..), Height::limit(5))
        .to_string();

    println!("{}", table);
}
