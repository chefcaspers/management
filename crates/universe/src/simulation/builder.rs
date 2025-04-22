use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;

use super::{Simulation, SimulationConfig, State};
use crate::SiteRunner;
use crate::error::Result;
use crate::idents::{BrandId, SiteId};
use crate::models::{Brand, Site};

#[derive(Debug, thiserror::Error)]
enum BuilderError {
    #[error("Duplicate brand: {0}")]
    DuplicateBrand(String),

    #[error("Invalid location: {0} / {1}")]
    InvalidLocation(f64, f64),
}

pub struct SimulationBuilder {
    brands: Vec<Brand>,

    sites: Vec<(String, f64, f64)>,

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
            sites: Vec::new(),
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

    /// Add a site to the simulation
    pub fn with_site(&mut self, name: impl ToString, longitude: f64, latitude: f64) -> &mut Self {
        self.sites.push((name.to_string(), longitude, latitude));
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
                let brand_id = BrandId::from_uri_ref(format!("brands/{}", brand.name));
                Ok::<_, Box<dyn std::error::Error>>((brand_id, brand))
            })
            .try_collect()?;

        let sites = self
            .sites
            .into_iter()
            .map(|(name, latitude, longitude)| {
                (
                    SiteId::from_uri_ref(&format!("sites/{}", name)),
                    Site {
                        id: SiteId::from_uri_ref(&format!("sites/{}", name)).to_string(),
                        name: name.to_string(),
                        latitude,
                        longitude,
                    },
                )
            })
            .collect_vec();

        let state = State::try_new(brands, sites)?;

        let site_runners = state
            .vendors
            .sites()?
            .map(|id| Ok::<_, Box<dyn std::error::Error>>((id, SiteRunner::try_new(id, &state)?)))
            .try_collect()?;

        Ok(Simulation {
            config: SimulationConfig {
                simulation_start: self.start_time,
                time_increment: self.time_increment,
            },
            state,
            sites: site_runners,
        })
    }
}
