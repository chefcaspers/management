use std::collections::HashMap;

use arrow::compute::concat_batches;
use chrono::{DateTime, Duration, Utc};
use datafusion::prelude::{col, lit};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::agents::{PopulationRunner, SiteRunner};
use crate::state::{EntityView, RoutingData, State};
use crate::{Error, EventTracker, ObjectData, OrderData, PopulationData, Result};

use super::{EventStatsBuffer, Simulation, SimulationContext};

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
pub struct SimulationConfig {
    /// all ghost kitchen sites.
    pub(crate) simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    pub(crate) time_increment: Duration,

    /// location to store simulation results
    pub(crate) result_storage_location: Option<Url>,

    pub(crate) dry_run: bool,

    pub(crate) write_events: bool,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        SimulationConfig {
            simulation_start: Utc::now(),
            time_increment: Duration::seconds(60),
            result_storage_location: None,
            dry_run: false,
            write_events: false,
        }
    }
}

/// Builder for creating a simulation instance.
pub struct SimulationBuilder {
    ctx: Option<SimulationContext>,

    /// Snapshot location for the simulation
    snapshot_location: Option<Url>,

    /// Time resolution for simulation steps
    time_increment: Duration,

    /// Start time for the simulation
    start_time: DateTime<Utc>,

    /// location to store simulation results
    working_directory: Option<Url>,

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
            ctx: None,
            snapshot_location: None,
            time_increment: Duration::minutes(1),
            start_time: Utc::now(),
            working_directory: None,
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

    /// Set the simulation context for the simulation
    pub fn with_context(mut self, ctx: SimulationContext) -> Self {
        self.ctx = Some(ctx);
        self
    }

    pub fn with_snapshot_location(mut self, snapshot_location: impl Into<Url>) -> Self {
        let mut snapshot_location = snapshot_location.into();
        if !snapshot_location.path().ends_with('/') {
            snapshot_location.set_path(&format!("{}/", snapshot_location.path()));
        }
        self.snapshot_location = Some(snapshot_location);
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
    pub fn with_working_directory(mut self, result_storage_location: impl Into<Url>) -> Self {
        self.working_directory = Some(result_storage_location.into());
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
        SimulationContext::builder()
            .with_routing_location(self.routing_path.clone())
            .with_snapshots_location(self.snapshot_location.clone())
            .with_working_directory(self.working_directory.clone())
            .build()
            .await
    }

    /// Load the prepared street network data into routing data objects.
    async fn build_state(
        &self,
        ctx: &SimulationContext,
        config: &SimulationConfig,
    ) -> Result<State> {
        let objects = ctx.snapshots().objects().await?.collect().await?;
        let objects = ObjectData::try_new(concat_batches(objects[0].schema_ref(), &objects)?)?;

        let mut routers = HashMap::new();
        for site in objects.sites()? {
            let info = site.properties()?;

            let site_nodes = ctx
                .system()
                .routing_nodes()
                .await?
                .filter(col("location").eq(lit(&info.name)))?
                .collect()
                .await?;
            let site_nodes = concat_batches(site_nodes[0].schema_ref(), &site_nodes)?;

            let site_edges = ctx
                .system()
                .routing_edges()
                .await?
                .filter(col("location").eq(lit(&info.name)))?
                .collect()
                .await?;
            let site_edges = concat_batches(site_edges[0].schema_ref(), &site_edges)?;

            routers.insert(site.id(), RoutingData::try_new(site_nodes, site_edges)?);
        }

        let population = PopulationData::try_new(ctx).await?;
        let orders = OrderData::try_new(ctx).await?;

        Ok(State::new(config, objects, population, orders, routers))
    }

    /// Build the simulation with the given initial conditions
    pub async fn build(mut self) -> Result<Simulation> {
        let config = SimulationConfig {
            simulation_start: self.start_time,
            time_increment: self.time_increment,
            result_storage_location: self.working_directory.clone(),
            dry_run: self.dry_run,
            write_events: self.write_events,
        };

        let ctx = if let Some(ctx) = std::mem::take(&mut self.ctx) {
            ctx
        } else {
            self.build_context().await?
        };

        let state = self.build_state(&ctx, &config).await?;

        let sites = state
            .objects()
            .sites()?
            .map(|site| Ok::<_, Error>((site.id(), SiteRunner::try_new(site.id(), &state)?)))
            .try_collect()?;

        Ok(Simulation {
            ctx,
            config,
            state,
            sites,
            population: PopulationRunner::new(),
            event_tracker: EventTracker::new(),
            stats_buffer: EventStatsBuffer::new(),
        })
    }
}
