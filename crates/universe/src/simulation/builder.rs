use std::collections::HashMap;

use arrow::compute::filter_record_batch;
use arrow_array::{Scalar, StringArray};
use arrow_ord::cmp::eq;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use url::Url;
use uuid::Uuid;

use crate::error::Result;
use crate::simulation::{Simulation, SimulationConfig};
use crate::state::{EntityView, RoutingData, State};
use crate::{Error, SimulationSetup, SiteId, SiteRunner, read_parquet_dir};

/// Builder for creating a simulation instance.
pub struct SimulationBuilder {
    setup: Option<SimulationSetup>,

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

    /// Path where routing data is stored
    routing_path: Option<Url>,

    /// Whether to run the simulation in dry run mode
    dry_run: bool,
}

impl Default for SimulationBuilder {
    fn default() -> Self {
        Self {
            setup: None,
            population_size: 1000,
            time_increment: Duration::seconds(60),
            start_time: Utc::now(),
            result_storage_location: None,
            snapshot_interval: None,
            routing_path: None,
            dry_run: false,
        }
    }
}

impl SimulationBuilder {
    /// Create a new simulation builder with default parameters
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a brand to the simulation
    pub fn with_setup(mut self, setup: SimulationSetup) -> Self {
        self.setup = Some(setup);
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

    pub fn with_routing_data_path(mut self, mut routing_path: Url) -> Self {
        if !routing_path.path().ends_with('/') {
            routing_path.set_path(&format!("{}/", routing_path.path()));
        }
        self.routing_path = Some(routing_path);
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Load the prepared street network data into routing data objects.
    async fn routing_data(&self) -> Result<HashMap<SiteId, RoutingData>> {
        let Some(setup) = &self.setup else {
            return Err(Error::MissingInput("Setup file not found".to_string()));
        };

        let Some(routing_path) = &self.routing_path else {
            return Err(Error::MissingInput(
                "Routing data path is required".to_string(),
            ));
        };

        let nodes_path = routing_path.join("nodes/").unwrap();
        let edge_path = routing_path.join("edges/").unwrap();

        let nodes = read_parquet_dir(&nodes_path, None).await?;
        let edges = read_parquet_dir(&edge_path, None).await?;

        tracing::info!(
            target: "builder",
            "Loaded routing data with {} nodes and {} edges", nodes.num_rows(), edges.num_rows()
        );

        let mut routers = HashMap::new();
        for site in &setup.sites {
            let site_name = Scalar::new(StringArray::from(vec![
                site.info.as_ref().map(|i| i.name.clone()),
            ]));
            // filter nodes for site
            let node_filter = eq(nodes.column(0), &site_name)?;
            let filtered_nodes = filter_record_batch(&nodes, &node_filter)?;

            // filter edges for site
            let edge_filter = eq(edges.column(0), &site_name)?;
            let filtered_edges = filter_record_batch(&edges, &edge_filter)?;
            let site_id = site
                .info
                .as_ref()
                .and_then(|i| Uuid::parse_str(&i.id).ok())
                .ok_or(Error::InvalidData("Expected site info".into()))?;

            tracing::info!(
                target: "builder",
                "Creating router for site {} with {} nodes and {} edges",
                site_id,
                filtered_nodes.num_rows(),
                filtered_edges.num_rows()
            );

            routers.insert(
                site_id.into(),
                RoutingData::try_new(filtered_nodes, filtered_edges)?,
            );
        }

        Ok(routers)
    }

    /// Build the simulation with the given initial conditions
    pub async fn build(self) -> Result<Simulation> {
        let config = SimulationConfig {
            simulation_start: self.start_time,
            time_increment: self.time_increment,
            result_storage_location: self.result_storage_location.clone(),
            snapshot_interval: self.snapshot_interval,
            dry_run: self.dry_run,
        };
        let routing = self.routing_data().await?;

        let state = if let Some(setup) = self.setup {
            State::try_new(setup, routing, Some(config))?
        } else {
            todo!()
        };

        let site_runners = state
            .objects()
            .sites()?
            .map(|site| Ok::<_, Error>((site.id(), SiteRunner::try_new(site.id(), &state)?)))
            .try_collect()?;

        Ok(Simulation {
            initialized: false,
            last_snapshot_time: state.current_time(),
            state,
            sites: site_runners,
        })
    }
}
