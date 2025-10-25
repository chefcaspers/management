use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::concat_batches;
use chrono::{DateTime, Duration, Utc};
use datafusion::catalog::MemTable;
use datafusion::prelude::{col, lit};
use itertools::Itertools;
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::Result;
use crate::simulation::Simulation;
use crate::simulation::session::{SimulationContext, simulation_context};
use crate::simulation::stats::EventStatsBuffer;
use crate::state::{EntityView, RoutingData, State};
use crate::{
    Error, EventTracker, ObjectData, OrderData, PopulationDataBuilder, PopulationRunner,
    SimulationSetup, SiteRunner,
};

/// Execution mode for the simulation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

    async fn build_context(&self) -> Result<SimulationContext> {
        let Some(setup) = &self.setup else {
            return Err(Error::MissingInput(
                "Simulation setup not found".to_string(),
            ));
        };
        let Some(routing_path) = &self.routing_path else {
            return Err(Error::MissingInput(
                "Routing data path is required".to_string(),
            ));
        };

        let objects = setup.object_data()?;
        let provider = Arc::new(MemTable::try_new(objects.schema(), vec![vec![objects]])?);

        simulation_context(routing_path, provider).await
    }

    /// Load the prepared street network data into routing data objects.
    async fn build_state(
        &self,
        ctx: &SimulationContext,
        config: SimulationConfig,
    ) -> Result<State> {
        let Some(setup) = &self.setup else {
            return Err(Error::MissingInput("Setup file not found".to_string()));
        };

        let objects = ctx.system().objects().await?.collect().await?;
        let objects = ObjectData::try_new(concat_batches(objects[0].schema_ref(), &objects)?)?;

        let mut routers = HashMap::new();
        for site in setup.sites.iter().filter_map(|s| s.info.as_ref()) {
            let site_nodes = ctx
                .system()
                .routing_nodes()
                .await?
                .filter(col("location").eq(lit(&site.name)))?
                .collect()
                .await?;
            let site_nodes = concat_batches(site_nodes[0].schema_ref(), &site_nodes)?;

            let site_edges = ctx
                .system()
                .routing_edges()
                .await?
                .filter(col("location").eq(lit(&site.name)))?
                .collect()
                .await?;
            let site_edges = concat_batches(site_edges[0].schema_ref(), &site_edges)?;

            routers.insert(
                Uuid::parse_str(&site.id)?.into(),
                RoutingData::try_new(site_nodes, site_edges)?,
            );
        }

        let mut builder = PopulationDataBuilder::new();
        for site in objects.sites()? {
            let n_people = rand::rng().random_range(500..1500);
            let info = site.properties()?;
            builder.add_site(n_people, info.latitude, info.longitude)?;
        }
        let population = builder.finish()?;

        Ok(State::new(
            config,
            objects,
            population,
            OrderData::empty(),
            routers,
        ))
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

        let ctx = self.build_context().await?;
        let state = self.build_state(&ctx, config).await?;

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
