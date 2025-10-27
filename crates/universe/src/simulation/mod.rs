use std::collections::HashMap;

use itertools::Itertools as _;
use rand::distr::{Distribution, Uniform};
use tracing::{Level, Span, field, instrument};

use crate::agents::{PopulationRunner, SiteRunner};
use crate::error::Result;
use crate::idents::SiteId;
use crate::simulation::execution::EventDataBuilder;
use crate::simulation::stats::EventStatsBuffer;
use crate::state::State;

pub use self::builder::*;
pub use self::context::*;
pub use self::events::*;

mod builder;
mod context;
mod events;
mod execution;
mod stats;

/// Trait for entities that need to be updated each simulation step
pub trait Simulatable {
    /// Update the entity state based on the current simulation context
    fn step(&mut self, events: &[EventPayload], context: &State) -> Result<Vec<EventPayload>>;
}

/// The main simulation engine
///
/// Single entry point to run simulations.
/// This will drive progress in all entities and make sure results are reported.
pub struct Simulation {
    ctx: SimulationContext,

    config: SimulationConfig,

    /// Global simulation state
    state: State,

    /// all ghost kitchen sites.
    sites: HashMap<SiteId, SiteRunner>,

    population: PopulationRunner,

    /// The event stats for the simulation
    event_tracker: EventTracker,

    stats_buffer: EventStatsBuffer,
}

impl Simulation {
    pub fn builder() -> SimulationBuilder {
        SimulationBuilder::new()
    }

    pub fn config(&self) -> &SimulationConfig {
        &self.config
    }

    /// Run the simulation for a specified number of steps
    #[instrument(skip(self))]
    pub async fn run(&mut self, steps: usize) -> Result<()> {
        tracing::info!(
            target: "caspers::simulation",
            "statrting simulation run for {} steps ({} / {})",
            steps,
            self.ctx.simulation_id(),
            self.ctx.snapshot_id()
        );

        for step in 0..steps {
            self.step().await?;
            if step % 8192 == 0 && step != 0 {
                self.write_event_stats().await?;
            };
        }

        self.write_event_stats().await?;

        // snapshot the state
        if !self.config().dry_run {
            self.snapshot().await?;
        }
        Ok(())
    }

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
            .push_stats(self.state.current_time(), "simulation", &stats)?;

        // update the state with the collected events
        self.state.step(&events)?;

        self.write_events(events).await?;

        Ok(())
    }

    #[instrument(skip_all, level = Level::TRACE)]
    async fn write_event_stats(&mut self) -> Result<()> {
        tracing::info!(
            target: "caspers::simulation",
            "writing event stats at {} ({})",
            self.state.current_time().to_rfc3339(),
            self.ctx.simulation_id()
        );

        let data = self.ctx.ctx().read_batch(self.stats_buffer.flush()?)?;
        self.ctx.results().write_metrics(data).await
    }

    #[instrument(skip_all, level = Level::TRACE)]
    async fn write_events(&self, events: impl IntoIterator<Item = EventPayload>) -> Result<()> {
        tracing::info!(
            target: "caspers::simulation",
            "writing events at {} ({})",
            self.state.current_time().to_rfc3339(),
            self.ctx.simulation_id()
        );

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
        let data = self.ctx.ctx().read_batch(builder.build()?)?;
        self.ctx.results().write_events(data).await
    }

    /// Snapshot the state of the simulation
    #[instrument(skip(self))]
    async fn snapshot(&mut self) -> Result<()> {
        tracing::info!(
            target: "caspers::simulation",
            "creating new snapshot at {} ({})",
            self.state.current_time().to_rfc3339(),
            self.ctx.simulation_id()
        );
        self.ctx.write_snapshot(&self.state).await
    }
}
