use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use datafusion::dataframe::DataFrameWriteOptions;
use geo_traits::to_geo::ToGeoPoint;
use h3o::{LatLng, Resolution};
use itertools::Itertools;
use rand::Rng;
use rand::distr::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::agents::SiteRunner;
use crate::error::Result;
use crate::idents::{BrandId, MenuItemId, SiteId, TypedId};
use crate::simulation::execution::EventDataBuilder;
use crate::state::{EntityView, PersonRole, State};

pub use self::builder::SimulationBuilder;
pub use self::events::*;

mod builder;
pub mod events;
mod execution;

/// Core trait that any simulatable entity must implement
pub trait Entity: Send + Sync + 'static {
    type Id: TypedId;

    /// Unique identifier for the entity
    fn id(&self) -> &Self::Id;
}

/// Trait for entities that need to be updated each simulation step
pub trait Simulatable: Entity {
    /// Update the entity state based on the current simulation context
    fn step(&mut self, context: &State) -> Result<Vec<EventPayload>>;
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
}

impl Default for SimulationConfig {
    fn default() -> Self {
        SimulationConfig {
            simulation_start: Utc::now(),
            time_increment: Duration::seconds(60),
            result_storage_location: None,
            snapshot_interval: None,
            dry_run: false,
        }
    }
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

    last_snapshot_time: DateTime<Utc>,

    /// whether the simulation has been initialized
    ///
    /// This is used to ensure that the simulation is only initialized once.
    /// e.g. we only create a population once, and load it in subsequent runs.
    initialized: bool,
}

impl Simulation {
    /// Advance the simulation by one time step
    async fn step(&mut self) -> Result<()> {
        let mut events = Vec::new();

        // move people
        let movements = self.state.move_people()?;

        // generate orders for each site
        let orders: HashMap<_, _> = self
            .sites
            .iter()
            .flat_map(|(site_id, _)| {
                self.orders_for_site(site_id)
                    .ok()
                    .map(|orders| (*site_id, orders.collect_vec()))
            })
            .collect();

        // process orders for each site
        let orders: HashMap<_, _> = orders
            .into_iter()
            .flat_map(|(site_id, orders)| Some((site_id, self.state.process_orders(&orders).ok()?)))
            .collect();

        // advance all sites and collect events
        for (site_id, site) in self.sites.iter_mut() {
            // send new orders to the site for processing
            let orders = orders.get(site_id).unwrap();
            let orders = orders
                .iter()
                .map(|order_id| self.state.orders().order(order_id).unwrap());
            site.receive_orders(orders)?;

            // advance the site and collect events
            if let Ok(site_events) = site.step(&self.state) {
                let order_line_updates = site_events.iter().filter_map(|event| match event {
                    EventPayload::OrderLineUpdated(payload) => Some(payload),
                    _ => None,
                });
                self.state.update_order_lines(order_line_updates)?;

                let order_updates = site_events.iter().filter_map(|event| match event {
                    EventPayload::OrderUpdated(payload) => Some(payload),
                    _ => None,
                });
                self.state.update_orders(order_updates)?;

                events.extend(site_events);
            } else {
                tracing::error!(target: "simulation", "Failed to step site {:?}", site.id());
            }
        }

        tracing::debug!(target: "simulation", "Collected {} events.", events.len());

        // update the state with the collected events
        self.state.step(&events)?;
        self.write_events(events).await?;

        // snapshot the state if the time is right
        if !self.state.config().dry_run {
            self.snapshot().await?;
        }

        Ok(())
    }

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
    pub async fn run(&mut self, steps: usize) -> Result<()> {
        for _ in 0..steps {
            self.step().await?;
        }
        Ok(())
    }

    /// Snapshot the state of the simulation
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

    fn orders_for_site(
        &self,
        site_id: &SiteId,
    ) -> Result<impl Iterator<Item = OrderCreatedPayload>> {
        let site = self.state.objects().site(site_id)?;
        let props = site.properties()?;
        let lat_lng = LatLng::new(props.latitude, props.longitude)?;

        Ok(self
            .state
            .population()
            // NB: resolution 6 corresponds to a cell size of approximately 36 km2
            .idle_people_in_cell(lat_lng.to_cell(Resolution::Six), &PersonRole::Customer)
            .filter_map(|person| create_order(&self.state).map(|items| (person, items)))
            .flat_map(|(person, items)| {
                Some(OrderCreatedPayload {
                    site_id: *site_id,
                    person_id: *person.id(),
                    items,
                    destination: person.position().ok()?.to_point(),
                })
            }))
    }
}

fn create_order(state: &State) -> Option<Vec<(BrandId, MenuItemId)>> {
    let mut rng = rand::rng();

    // TODO: compute probability from person state
    rng.random_bool(1.0 / 50.0).then(|| {
        state
            .objects()
            .sample_menu_items(None, &mut rng)
            .into_iter()
            .map(|menu_item| (menu_item.brand_id().try_into().unwrap(), menu_item.id()))
            .collect()
    })
}
