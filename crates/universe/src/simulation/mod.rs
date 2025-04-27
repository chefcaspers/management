use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use url::Url;

use crate::agents::SiteRunner;
use crate::error::Result;
use crate::idents::{SiteId, TypedId};

pub use self::builder::SimulationBuilder;
pub use self::events::*;
pub use self::state::State;

mod builder;
pub mod events;
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
    fn step(&mut self, context: &state::State) -> Result<Vec<EventPayload>>;
}

/// Configuration for the simulation engine
pub(crate) struct SimulationConfig {
    /// all ghost kitchen sites.
    simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    time_increment: Duration,

    /// location to store simulation results
    result_storage_location: Option<Url>,

    snapshot_interval: Option<Duration>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        SimulationConfig {
            simulation_start: Utc::now(),
            time_increment: Duration::seconds(60),
            result_storage_location: None,
            snapshot_interval: None,
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

    last_snapshot_time: DateTime<Utc>,
}

impl Simulation {
    /// Advance the simulation by one time step
    fn step(&mut self) -> Result<()> {
        let mut events = Vec::new();

        // move people
        let movements = self.state.move_people()?;
        tracing::info!(target: "simulation", "Moved {} people.", movements.len());

        // advance all sites and collect events
        for site in self.sites.values_mut() {
            if let Ok(site_events) = site.step(&self.state) {
                events.extend(site_events);
            } else {
                tracing::error!("Failed to step site {:?}", site.id());
            }
        }

        tracing::info!(target: "simulation", "Collected {} events.", events.len());

        // update the state with the collected events
        self.state.step(events)?;

        // snapshot the state if the time is right
        if let (Some(base_url), Some(interval)) = (
            self.state().config().result_storage_location.as_ref(),
            self.state().config().snapshot_interval,
        ) {
            if (self.state.current_time() - self.last_snapshot_time).num_seconds()
                > interval.num_seconds()
            {
                for site in self.sites.values() {
                    site.snapshot(&self.state, base_url)?;
                }

                self.state().snapshot(base_url)?;
                self.last_snapshot_time = self.state.current_time();
            }
        }

        Ok(())
    }

    /// Run the simulation for a specified number of steps
    pub fn run(&mut self, steps: usize) -> Result<()> {
        for _ in 0..steps {
            self.step()?;
        }
        Ok(())
    }

    pub fn state(&self) -> &State {
        &self.state
    }
}
