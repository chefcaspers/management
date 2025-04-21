use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;

use crate::agents::Site;
use crate::error::Result;
use crate::idents::{SiteId, TypedId};

pub use self::builder::SimulationBuilder;
pub use self::state::State;

mod builder;
mod execution;
pub mod schemas;
pub mod state;

/// Core trait that any simulatable entity must implement
pub trait Entity: Send + Sync + 'static {
    type Id: TypedId;

    /// Unique identifier for the entity
    fn id(&self) -> &Self::Id;

    /// Human-readable name of the entity
    fn name(&self) -> &str;
}

/// Trait for entities that need to be updated each simulation step
pub trait Simulatable: Entity {
    /// Update the entity state based on the current simulation context
    fn step(&mut self, context: &state::State) -> Result<()>;
}

struct SimulationConfig {
    /// all ghost kitchen sites.
    simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    time_increment: Duration,
}

/// The main simulation engine
///
/// Single entry point to run simulations.
/// THis will drive progress in all entities and make sure results are reported.
pub struct Simulation {
    config: SimulationConfig,

    /// Global simulation state
    state: State,

    /// all ghost kitchen sites.
    sites: HashMap<SiteId, Site>,
}

impl Simulation {
    /// Advance the simulation by one time step
    fn step(&mut self) {
        for site in self.sites.values_mut() {
            let orders = self.state.orders_for_location(site.id()).collect_vec();
            for items in orders {
                site.queue_order(
                    items.into_iter().map(|(order, item)| {
                        (order, uuid::Uuid::parse_str(&item.id).unwrap().into())
                    }),
                );
            }
            site.step(&self.state);
        }
        self.state.step();
    }

    /// Run the simulation for a specified number of steps
    pub fn run(&mut self, steps: usize) -> Result<()> {
        for _ in 0..steps {
            self.step();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn test_inner_simulation() -> Result<(), Box<dyn std::error::Error>> {
        let mut simulation = SimulationBuilder::new();

        for brand in crate::init::generate_brands() {
            simulation.with_brand(brand);
        }

        let mut simulation = simulation.build()?;
        simulation.run(10)?;

        Ok(())
    }
}
