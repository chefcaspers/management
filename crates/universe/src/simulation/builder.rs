use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;

use super::{Entity, Simulation, SimulationConfig, State};
use crate::error::Result;
use crate::idents::BrandId;
use crate::init::generate_site;
use crate::models::Brand;

pub struct SimulationBuilder {
    brands: Vec<Brand>,

    /// Size of the simulated population
    population_size: usize,

    /// Time resolution for simulation steps
    time_increment: Duration,

    /// Start time for the simulation
    start_time: DateTime<Utc>,
}

impl Default for SimulationBuilder {
    fn default() -> Self {
        Self {
            brands: Vec::new(),
            population_size: 1000,
            time_increment: Duration::seconds(60),
            start_time: Utc::now(),
        }
    }
}

impl SimulationBuilder {
    /// Create a new simulation builder with default parameters
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a brand to the simulation
    pub fn with_brand(&mut self, brand: Brand) -> &mut Self {
        self.brands.push(brand);
        self
    }

    /// Set the population size for the simulation
    pub fn with_population_size(&mut self, population_size: usize) -> &mut Self {
        self.population_size = population_size;
        self
    }

    /// Set the start time for the simulation
    pub fn with_start_time(&mut self, start_time: DateTime<Utc>) -> &mut Self {
        self.start_time = start_time;
        self
    }

    /// Set the time increment for the simulation
    pub fn with_time_increment(&mut self, time_increment: Duration) -> &mut Self {
        self.time_increment = time_increment;
        self
    }

    /// Build the simulation with the given initial conditions
    pub fn build(self) -> Result<Simulation> {
        let brands: HashMap<BrandId, _> = self
            .brands
            .into_iter()
            .map(|brand| {
                let brand_id = uuid::Uuid::try_parse(&brand.id)?.into();
                Ok::<_, Box<dyn std::error::Error>>((brand_id, brand))
            })
            .try_collect()?;

        let sites = Some(generate_site("site-1", brands.clone()))
            .map(|site| (site.id().clone(), site))
            .into_iter()
            .collect();

        Ok(Simulation {
            config: SimulationConfig {
                simulation_start: self.start_time,
                time_increment: self.time_increment,
            },
            state: State::try_new(brands)?,
            sites,
        })
    }
}
