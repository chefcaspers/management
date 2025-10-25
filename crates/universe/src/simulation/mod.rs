use std::collections::HashMap;

use chrono::{DateTime, Utc};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use itertools::Itertools as _;
use rand::distr::{Distribution, Uniform};
use tracing::{Level, Span, field, instrument};

use crate::PopulationRunner;
use crate::agents::SiteRunner;
use crate::error::Result;
use crate::idents::{SiteId, TypedId};
use crate::simulation::execution::EventDataBuilder;
use crate::simulation::stats::EventStatsBuffer;
use crate::state::State;

pub use self::builder::*;
pub use self::events::*;
pub(crate) use self::session::*;

mod builder;
pub mod events;
mod execution;
mod session;
mod stats;

/// Core trait that any simulatable entity must implement
pub trait Entity: Send + Sync + 'static {
    type Id: TypedId;

    /// Unique identifier for the entity
    fn id(&self) -> &Self::Id;
}

/// Trait for entities that need to be updated each simulation step
pub trait Simulatable: Entity {
    /// Update the entity state based on the current simulation context
    fn step(&mut self, events: &[EventPayload], context: &State) -> Result<Vec<EventPayload>>;
}

/// The main simulation engine
///
/// Single entry point to run simulations.
/// This will drive progress in all entities and make sure results are reported.
pub struct Simulation {
    /// Global simulation state
    state: State,

    /// all ghost kitchen sites.
    sites: HashMap<SiteId, SiteRunner>,

    population: PopulationRunner,

    last_snapshot_time: DateTime<Utc>,

    /// whether the simulation has been initialized
    ///
    /// This is used to ensure that the simulation is only initialized once.
    /// e.g. we only create a population once, and load it in subsequent runs.
    initialized: bool,

    /// The event stats for the simulation
    event_tracker: EventTracker,

    stats_buffer: EventStatsBuffer,
}

impl Simulation {
    /// Advance the simulation by one time step
    #[instrument(skip(self), fields(caspers.total_events_generated = field::Empty))]
    async fn step(&mut self) -> Result<()> {
        // move people
        let mut events = self.state.move_people()?;

        // advance all sites and collect events
        for (site_id, site) in self.sites.iter_mut() {
            // query population to get new orders for the site
            let population_events = self.population.step(site_id, &self.state)?.collect_vec();

            // update the site state with new orders
            let interactions_events = self.state.process_population_events(&population_events)?;
            events.extend(population_events);

            // advance the site and collect events
            if let Ok(site_events) = site.step(&interactions_events, &self.state) {
                events.extend(interactions_events);
                self.state.process_site_events(&site_events)?;
                events.extend(site_events);
            } else {
                tracing::error!(target: "simulation", "Failed to step site {:?}", site.id());
            }
        }

        let stats = self.event_tracker.process_events(&events, &self.state);
        let span = Span::current();
        span.record("caspers.total_events_generated", stats.num_orders_created);

        self.stats_buffer
            .push_stats(self.state.current_time(), &stats)?;

        // update the state with the collected events
        self.state.step(&events)?;

        self.write_events(events).await?;

        Ok(())
    }

    #[instrument(skip_all, level = Level::TRACE)]
    async fn write_event_stats(&mut self) -> Result<()> {
        let base_url = self.state.config().result_storage_location.as_ref();
        // we have no place to store results
        let Some(base_path) = base_url else {
            return Ok(());
        };

        let ts = self.state.current_time().timestamp();
        let events_path = base_path
            .join(&format!("stats/events/snapshot-{ts}.parquet"))
            .unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_batch(self.stats_buffer.flush()?)?;
        df.write_parquet(events_path.as_str(), DataFrameWriteOptions::new(), None)
            .await?;

        Ok(())
    }

    #[instrument(skip_all, level = Level::TRACE)]
    async fn write_events(&self, events: impl IntoIterator<Item = EventPayload>) -> Result<()> {
        let range = Uniform::new(0.0_f32, 0.9999_f32).unwrap();
        let events = events.into_iter().map(|payload| {
            let multiplier = range.sample(&mut rand::rng());
            let timestamp = self.state.current_time() + self.state.time_step().mul_f32(multiplier);
            Event { timestamp, payload }
        });

        let mut builder = EventDataBuilder::new();
        for event in events {
            builder.add_event(&event)?;
        }
        let batch = builder.build()?;

        let base_url = self.state.config().result_storage_location.as_ref();
        // we have no place to store results
        let Some(base_path) = base_url else {
            return Ok(());
        };
        let ts = self.state.current_time().timestamp();
        let events_path = base_path
            .join(&format!("events/snapshot-{ts}.json"))
            .unwrap();

        let ctx = self.state.snapshot_session()?;
        let df = ctx.read_batch(batch)?;
        df.write_json(events_path.as_str(), DataFrameWriteOptions::new(), None)
            .await?;

        Ok(())
    }

    /// Run the simulation for a specified number of steps
    #[instrument(skip(self))]
    pub async fn run(&mut self, steps: usize) -> Result<()> {
        for step in 0..steps {
            self.step().await?;
            self.snapshot_stats().await?;
            if step % 8192 == 0 && step != 0 {
                self.write_event_stats().await?;
            };
        }

        self.write_event_stats().await?;

        // snapshot the state
        if !self.state.config().dry_run {
            self.snapshot().await?;
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn snapshot_stats(&self) -> Result<()> {
        let base_url = self
            .state
            .config()
            .result_storage_location
            .as_ref()
            .unwrap();

        let ctx = self.state.snapshot_session()?;

        let ts = self.state.current_time().timestamp();
        let path = |name: &str| {
            base_url
                .join(&format!("{name}/snapshot-{ts}.parquet"))
                .unwrap()
        };

        let timestamp = self.state.current_time().to_rfc3339();
        let query = format!(
            r#"
            SELECT '{timestamp}'::timestamp(6) as timestamp, status, count(*) as count
            FROM orders
            GROUP BY status
        "#
        );

        let df = ctx.sql(&query).await?;
        let order_stats_path = path("stats/orders");
        df.write_parquet(
            order_stats_path.as_str(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

        Ok(())
    }

    /// Snapshot the state of the simulation
    #[instrument(skip(self))]
    async fn snapshot(&mut self) -> Result<()> {
        let base_url = self.state.config().result_storage_location.as_ref();
        let interval = self.state.config().snapshot_interval;

        // we have no place to store results
        let (Some(base_path), Some(interval)) = (base_url, interval) else {
            return Ok(());
        };

        // we don't need to snapshot more often than the interval
        let should_snapshot = self.state.current_time() - self.last_snapshot_time >= interval;

        // create helper functions to create paths and queries
        let ts = self.state.current_time().timestamp();
        let path = |name: &str| {
            base_path
                .join(&format!("{name}/snapshot-{ts}.parquet"))
                .unwrap()
        };

        let timestamp = self.state.current_time().to_rfc3339();
        let query =
            |name: &str| format!("SELECT '{timestamp}'::timestamp(6) as timestamp, * FROM {name}");

        // create storage paths for each table
        let people_path = path("population/people");
        let positions_path = path("population/positions");
        let objects_path = path("objects");
        let orders_path = path("orders");
        let order_lines_path = path("order_lines");

        let ctx = self.state.snapshot_session()?;

        // people are only written once
        if !self.initialized {
            let df = ctx.sql(&query("population")).await?;
            df.write_parquet(people_path.as_str(), DataFrameWriteOptions::new(), None)
                .await?;

            // TODO: once we allow adding more brands etc. we need to make this more dynamic.
            // or rather this information should be written from outside the simulation.
            let df = ctx.sql(&query("objects")).await?;
            df.write_parquet(objects_path.as_str(), DataFrameWriteOptions::new(), None)
                .await?;
        }

        if should_snapshot {
            let df = ctx.sql(&query("orders")).await?;
            df.write_parquet(orders_path.as_str(), DataFrameWriteOptions::new(), None)
                .await?;

            let df = ctx.sql(&query("order_lines")).await?;
            df.write_parquet(
                order_lines_path.as_str(),
                DataFrameWriteOptions::new(),
                None,
            )
            .await?;
        }

        // write courier positions at every call.
        let df = ctx
                .sql(&format!(
                    "SELECT id, '{timestamp}'::timestamp(6) as timestamp, position FROM population WHERE role = 'courier'"
                ))
                .await?;
        df.write_parquet(positions_path.as_str(), DataFrameWriteOptions::new(), None)
            .await?;

        if should_snapshot {
            self.last_snapshot_time = self.state.current_time();
        }

        self.initialized = true;

        Ok(())
    }
}
