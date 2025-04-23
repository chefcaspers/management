use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use self::schemas::OrderDataStats;
use crate::KitchenStats;
use crate::agents::SiteRunner;
use crate::error::Result;
use crate::idents::{SiteId, TypedId};

pub use self::builder::SimulationBuilder;
pub use self::events::*;
pub use self::state::State;

mod builder;
mod events;
mod execution;
pub mod schemas;
pub mod state;

/// Core trait that any simulatable entity must implement
pub trait Entity: Send + Sync + 'static {
    type Id: TypedId;

    /// Unique identifier for the entity
    fn id(&self) -> &Self::Id;
}

/// Trait for entities that need to be updated each simulation step
pub trait Simulatable: Entity {
    /// Update the entity state based on the current simulation context
    fn step(&mut self, context: &state::State) -> Result<Vec<Event>>;
}

/// Configuration for the simulation engine
pub struct SimulationConfig {
    /// all ghost kitchen sites.
    simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    time_increment: Duration,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        SimulationConfig {
            simulation_start: Utc::now(),
            time_increment: Duration::seconds(60),
        }
    }
}

/// The main simulation engine
///
/// Single entry point to run simulations.
/// THis will drive progress in all entities and make sure results are reported.
pub struct Simulation {
    /// Global simulation state
    state: State,

    /// all ghost kitchen sites.
    sites: HashMap<SiteId, SiteRunner>,
}

impl Simulation {
    /// Advance the simulation by one time step
    fn step(&mut self) -> Result<()> {
        for site in self.sites.values_mut() {
            site.step(&self.state)?;
        }
        self.state.step();
        Ok(())
    }

    /// Run the simulation for a specified number of steps
    pub fn run(&mut self, steps: usize) -> Result<()> {
        for _ in 0..steps {
            self.step()?;
        }
        Ok(())
    }

    fn snapshot(&self) {
        let total_kitchen_stats: KitchenStats = self
            .sites
            .values()
            .map(|site| site.total_kitchen_stats())
            .fold(KitchenStats::default(), |acc, stats| acc + stats);
        let order_data_stats = self
            .sites
            .values()
            .map(|site| site.order_data().stats())
            .fold(OrderDataStats::default(), |acc, stats| acc + stats);
        println!("{order_data_stats:#?}");
        println!("{total_kitchen_stats:#?}");
    }

    pub fn state(&self) -> &State {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use arrow_cast::pretty::print_batches;

    use super::*;

    #[test_log::test]
    fn test_inner_simulation() -> Result<(), Box<dyn std::error::Error>> {
        let mut simulation = SimulationBuilder::new();

        for brand in crate::init::generate_brands() {
            simulation.with_brand(brand);
        }

        for (name, (lat, long)) in [("london", (51.518898098201326, -0.13381370382489707))] {
            simulation.with_site(name, lat, long);
        }

        let mut simulation = simulation.build()?;
        for _ in 0..5 {
            simulation.run(10)?;
            simulation.snapshot();
        }

        // print_batches(&[simulation.state().objects().project(&[2, 4]).unwrap()]).unwrap();

        // print_batches(&[simulation.state().people().clone()]).unwrap();

        Ok(())
    }
}
