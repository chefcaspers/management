use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use self::state::State;
use crate::models::Brand;
use crate::{
    Site,
    idents::{SiteId, TypedId},
};

pub mod execution;
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
    fn step(&mut self, context: &state::State) -> Option<()>;
}

struct InitialConditions {
    /// All vendors that will be included in the simulation
    vendors: Vec<(String, Vec<Brand>)>,

    /// all ghost kitchen sites.
    sites: Vec<String>,
}

struct SimulationConfig {
    /// all ghost kitchen sites.
    simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    time_increment: Duration,
}

pub struct SimulationBuilder {
    brands: Vec<Brand>,
}

/// The main simulation engine
///
/// Single entry point to run simulations.
/// THis will drive progress in all entities and make sure results are reported.
pub struct Simulation {
    config: SimulationConfig,

    /// Global simulation state
    context: State,

    /// all ghost kitchen sites.
    sites: HashMap<SiteId, Site>,
}

impl Simulation {
    /// Create a new simulation with default parameters
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            config,
            context: state::State::try_new().unwrap(),
            sites: HashMap::new(),
        }
    }

    /// Advance the simulation by one time step
    fn step(&mut self) {
        // Advance simulation time
        self.context.step();
    }

    /// Run the simulation for a specified number of steps
    pub fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.step();
        }
    }
}
