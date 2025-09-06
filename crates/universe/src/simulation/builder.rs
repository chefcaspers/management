use std::collections::HashMap;
use std::str::FromStr;

use arrow_array::RecordBatchReader;
use arrow_select::concat::concat_batches;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;
use uuid::Uuid;

use crate::error::Result;
use crate::idents::BrandId;
use crate::models::{Brand, Site};
use crate::simulation::{Simulation, SimulationConfig};
use crate::state::{EntityView, RoutingData, State};
use crate::{Error, SiteRunner};

/// Builder for creating a simulation instance.
pub struct SimulationBuilder {
    brands: Vec<Brand>,

    sites: Vec<Site>,

    /// Size of the simulated population
    population_size: usize,

    /// Time resolution for simulation steps
    time_increment: Duration,

    /// Start time for the simulation
    start_time: DateTime<Utc>,

    /// location to store simulation results
    result_storage_location: Option<Url>,

    /// Interval at which to take snapshots of the simulation state
    snapshot_interval: Option<Duration>,

    /// Routing nodes and edges
    routing_nodes: Option<Url>,
    routing_edges: Option<Url>,
}

impl Default for SimulationBuilder {
    fn default() -> Self {
        Self {
            brands: Vec::new(),
            sites: Vec::new(),
            population_size: 1000,
            time_increment: Duration::seconds(60),
            start_time: Utc::now(),
            result_storage_location: None,
            snapshot_interval: None,
            routing_nodes: None,
            routing_edges: None,
        }
    }
}

impl SimulationBuilder {
    /// Create a new simulation builder with default parameters
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a brand to the simulation
    pub fn with_brand(mut self, brand: Brand) -> Self {
        self.brands.push(brand);
        self
    }

    /// Add a site to the simulation
    pub fn with_site(mut self, site: Site) -> Self {
        self.sites.push(site);
        self
    }

    /// Set the population size for the simulation
    pub fn with_population_size(mut self, population_size: usize) -> Self {
        self.population_size = population_size;
        self
    }

    /// Set the start time for the simulation
    pub fn with_start_time(mut self, start_time: DateTime<Utc>) -> Self {
        self.start_time = start_time;
        self
    }

    /// Set the time increment for the simulation
    pub fn with_time_increment(mut self, time_increment: Duration) -> Self {
        self.time_increment = time_increment;
        self
    }

    /// Set the result storage location for the simulation
    pub fn with_result_storage_location(mut self, result_storage_location: impl Into<Url>) -> Self {
        self.result_storage_location = Some(result_storage_location.into());
        self
    }

    pub fn with_snapshot_interval(mut self, snapshot_interval: Duration) -> Self {
        self.snapshot_interval = Some(snapshot_interval);
        self
    }

    pub fn with_routing_nodes(mut self, routing_nodes: Url) -> Self {
        self.routing_nodes = Some(routing_nodes);
        self
    }

    pub fn with_routing_edges(mut self, routing_edges: Url) -> Self {
        self.routing_edges = Some(routing_edges);
        self
    }

    /// Build the simulation with the given initial conditions
    pub fn build(self) -> Result<Simulation> {
        let brands: HashMap<BrandId, _> = self
            .brands
            .into_iter()
            .map(|brand| {
                let brand_id = BrandId::from_uri_ref(format!("brands/{}", brand.name));
                Ok::<_, Error>((brand_id, brand))
            })
            .try_collect()?;

        let sites = self
            .sites
            .into_iter()
            .map(|site| (Uuid::from_str(&site.id).unwrap().into(), site))
            .collect_vec();

        let file = std::fs::File::open(
            "/Users/robert.pack/code/management/notebooks/sites/edges/london.parquet",
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let schema = reader.schema();
        let batches: Vec<_> = reader.into_iter().try_collect()?;
        let edges = concat_batches(&schema, &batches)?;

        // let Some(node_file) = self.routing_nodes else {
        //     return Err(Error::MissingInput(
        //         "Routing nodes file not found".to_string(),
        //     ));
        // };

        let file = std::fs::File::open(
            "/Users/robert.pack/code/management/notebooks/sites/nodes/london.parquet",
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let schema = reader.schema();
        let batches: Vec<_> = reader.into_iter().try_collect()?;
        let nodes = concat_batches(&schema, &batches)?;

        let routing = RoutingData::try_new(nodes, edges)?;

        let config = SimulationConfig {
            simulation_start: self.start_time,
            time_increment: self.time_increment,
            result_storage_location: self.result_storage_location,
            snapshot_interval: self.snapshot_interval,
        };
        let state = State::try_new(brands, sites, routing, Some(config))?;

        let site_runners = state
            .objects()
            .sites()?
            .map(|site| Ok::<_, Error>((site.id(), SiteRunner::try_new(site.id(), &state)?)))
            .try_collect()?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Generic(Box::new(e)))?;

        Ok(Simulation {
            rt,
            initialized: false,
            last_snapshot_time: state.current_time(),
            state,
            sites: site_runners,
        })
    }
}
