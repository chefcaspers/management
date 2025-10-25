use std::collections::HashMap;

use arrow::array::{Scalar, StringArray};
use arrow::compute::filter_record_batch;
use arrow_ord::cmp::eq;
use chrono::{DateTime, Duration, Utc};
use clap::ValueEnum;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::Result;
use crate::simulation::Simulation;
use crate::simulation::stats::EventStatsBuffer;
use crate::state::{EntityView, RoutingData, State};
use crate::{
    Error, EventTracker, PopulationRunner, SimulationSetup, SiteId, SiteRunner, read_parquet_dir,
};

/// Execution mode for the simulation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum SimulationMode {
    /// Run the simulation for the specified time horizon.
    Backfill,
    /// Align time passed in simulation with time passed in real time.
    Realtime,
    /// Continue simulation from last snapshot up to current time, then switch to real time.
    Catchup,
}

/// Configuration for the simulation engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SimulationConfig {
    /// all ghost kitchen sites.
    pub(crate) simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    pub(crate) time_increment: Duration,

    /// location to store simulation results
    pub(crate) result_storage_location: Option<Url>,

    pub(crate) snapshot_interval: Option<Duration>,

    pub(crate) dry_run: bool,

    pub(crate) write_events: bool,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        SimulationConfig {
            simulation_start: Utc::now(),
            time_increment: Duration::seconds(60),
            result_storage_location: None,
            snapshot_interval: None,
            dry_run: false,
            write_events: false,
        }
    }
}

/// Builder for creating a simulation instance.
pub struct SimulationBuilder {
    /// Setup configuration for the simulation
    setup: Option<SimulationSetup>,

    /// Snapshot location for the simulation
    snapshot_location: Option<Url>,

    /// Snapshot version to start simulation from
    snapshot_version: Option<u64>,

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

    /// Whether to write events to the event tracker
    write_events: bool,
}

impl Default for SimulationBuilder {
    fn default() -> Self {
        Self {
            setup: None,
            snapshot_location: None,
            snapshot_version: None,
            population_size: 1000,
            time_increment: Duration::minutes(1),
            start_time: Utc::now(),
            result_storage_location: None,
            snapshot_interval: None,
            routing_path: None,
            dry_run: false,
            write_events: false,
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

    pub fn with_snapshot_location(mut self, snapshot_location: impl Into<Url>) -> Self {
        self.snapshot_location = Some(snapshot_location.into());
        self
    }

    /// Set the snapshot version for the simulation
    pub fn with_snapshot_version(mut self, snapshot_version: u64) -> Self {
        self.snapshot_version = Some(snapshot_version);
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

    pub fn with_write_events(mut self, write_events: bool) -> Self {
        self.write_events = write_events;
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
            write_events: self.write_events,
        };
        let routing = self.routing_data().await?;

        let state = if let (Some(sn_path), Some(sn_id)) =
            (&self.snapshot_location, self.snapshot_version)
        {
            State::load_snapshot(Some(config), routing, sn_path, sn_id).await?
        } else if let Some(setup) = self.setup {
            State::try_new(setup, routing, Some(config))?
        } else {
            return Err(Error::MissingInput(
                "Either simulation setup (new sim) or snapshot details are required.".into(),
            ));
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
            population: PopulationRunner::new(),
            event_tracker: EventTracker::new(),
            stats_buffer: EventStatsBuffer::new(),
        })
    }
}
