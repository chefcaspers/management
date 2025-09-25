//! Internal state management for the simulation.
//!
//! This module provides structures and utilities to manage the internal state of the simulation.
//! Whenever feasible, state is tracked as Arrow RecordBatches for seamless introp with
//! external data storages that might be used to store the state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{RecordBatch, cast::AsArray as _};
use arrow::compute::concat_batches;
use chrono::{DateTime, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use geo::Point;
use geo_traits::PointTrait;
use itertools::Itertools;
use rand::Rng;
use url::Url;
use uuid::Uuid;

use crate::error::Result;
use crate::idents::*;
use crate::{
    Error, OrderCreatedPayload, OrderLineUpdatedPayload, OrderUpdatedPayload, SimulationSetup,
};

use self::movement::JourneyPlanner;
use super::{EventPayload, SimulationConfig};

pub(crate) use self::movement::RoutingData;
pub(crate) use self::objects::{ObjectData, ObjectDataBuilder, ObjectLabel};
pub(crate) use self::orders::{
    OrderData, OrderDataBuilder, OrderLineStatus, OrderStatus, OrderView,
};
pub(crate) use self::population::{
    PersonRole, PersonStatus, PopulationData, PopulationDataBuilder,
};

mod movement;
mod objects;
mod orders;
mod population;

#[derive(Debug, thiserror::Error)]
enum StateError {
    // inconsistent data
    #[error("Inconsistent data")]
    InconsistentData,
}

impl From<StateError> for Error {
    fn from(err: StateError) -> Self {
        Error::InternalError(err.to_string())
    }
}

pub struct State {
    config: SimulationConfig,

    /// Current simulation time
    time: DateTime<Utc>,

    /// Time increment per simulation step
    time_step: Duration,

    /// Population data
    population: PopulationData,

    /// Vendor data
    objects: ObjectData,

    /// Routing data
    routing: HashMap<SiteId, JourneyPlanner>,

    /// Order data
    orders: OrderData,
}

impl State {
    pub(crate) fn try_new(
        setup: SimulationSetup,
        routing: HashMap<SiteId, RoutingData>,
        config: Option<SimulationConfig>,
    ) -> Result<Self> {
        let mut builder = PopulationDataBuilder::new();

        for site in &setup.sites {
            let n_people = rand::rng().random_range(500..1500);
            let info = site
                .info
                .as_ref()
                .ok_or(Error::invalid_data("expected site info"))?;
            builder.add_site(info, n_people)?;
        }

        let brands: HashMap<_, _> = setup
            .brands
            .into_iter()
            .map(|brand| Ok::<_, Error>((Uuid::parse_str(&brand.id)?.into(), brand)))
            .try_collect()?;

        let vendors = crate::init::generate_objects(&brands, setup.sites)?;

        let config = config.unwrap_or_default();
        Ok(State {
            time_step: Duration::from_secs(config.time_increment.num_seconds() as u64),
            time: config.simulation_start,
            population: builder.finish()?,
            objects: ObjectData::try_new(vendors)?,
            routing: routing
                .into_iter()
                .map(|(id, data)| (id, data.into_trip_planner()))
                .collect(),
            config,
            orders: OrderData::empty(),
        })
    }

    pub(crate) fn config(&self) -> &SimulationConfig {
        &self.config
    }

    pub fn people(&self) -> &RecordBatch {
        self.population.people()
    }

    pub fn objects(&self) -> &ObjectData {
        &self.objects
    }

    pub fn population(&self) -> &PopulationData {
        &self.population
    }

    pub fn orders(&self) -> &OrderData {
        &self.orders
    }

    pub fn trip_planner(&self, site_id: &SiteId) -> Option<&JourneyPlanner> {
        self.routing.get(site_id)
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn time_step(&self) -> Duration {
        self.time_step
    }

    pub(crate) fn next_time(&self) -> DateTime<Utc> {
        self.time + self.time_step
    }

    pub(crate) fn update_order_lines<'a>(
        &mut self,
        updates: impl IntoIterator<Item = &'a OrderLineUpdatedPayload>,
    ) -> Result<()> {
        self.orders.update_order_lines(
            updates
                .into_iter()
                .map(|payload| (payload.order_line_id, &payload.status)),
        )?;
        Ok(())
    }

    pub(crate) fn update_orders<'a>(
        &mut self,
        updates: impl IntoIterator<Item = &'a OrderUpdatedPayload>,
    ) -> Result<()> {
        self.orders.update_orders(
            updates
                .into_iter()
                .map(|payload| (payload.order_id, &payload.status)),
        )?;
        Ok(())
    }

    pub(crate) fn process_orders<'a>(
        &mut self,
        orders: impl IntoIterator<Item = &'a OrderCreatedPayload>,
    ) -> Result<Vec<OrderId>> {
        let order_data = orders
            .into_iter()
            .fold(OrderDataBuilder::new(), |builder, order| {
                builder.add_order(
                    order.site_id,
                    order.person_id,
                    order.destination.coord().unwrap().try_into().unwrap(),
                    &order.items,
                )
            })
            .finish()?;

        tracing::debug!(
            target: "state",
            "Processing {} orders with {} lines",
            order_data.batch_orders().num_rows(),
            order_data.batch_lines().num_rows()
        );

        let order_ids = order_data.all_orders().map(|o| *o.id()).collect_vec();
        self.orders = self.orders.merge(order_data)?;
        Ok(order_ids)
    }

    /// Advance people's journeys and update their statuses on arrival at their destination.
    pub(super) fn move_people(&mut self) -> Result<Vec<(PersonId, Vec<Point>)>> {
        let (movements, status_updates) = self.population.update_journeys(self.time_step)?;
        tracing::debug!(
            target: "state",
            "Moved {} people with {} status updates",
            movements.len(),
            status_updates.len()
        );

        // update person statuses after positions have been updated.
        for (person_id, status) in status_updates.into_iter() {
            self.population.update_person_status(&person_id, &status)?;
        }
        Ok(movements)
    }

    pub(super) fn step<'a>(
        &mut self,
        events: impl IntoIterator<Item = &'a EventPayload>,
    ) -> Result<()> {
        for event in events.into_iter() {
            if let EventPayload::PersonUpdated(payload) = event {
                self.population
                    .update_person_status(&payload.person_id, &payload.status)?;
            }
        }

        self.time += self.time_step;

        Ok(())
    }

    /// Create a new session context with the current state of the simulation.
    pub(crate) fn snapshot_session(&self) -> Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_batch("population", self.population.people_full().clone())?;
        ctx.register_batch("objects", self.objects.objects().clone())?;
        ctx.register_batch("orders", self.orders.batch_orders().clone())?;
        ctx.register_batch("order_lines", self.orders.batch_lines().clone())?;
        Ok(ctx)
    }
}

pub trait EntityView {
    type Id: TypedId;
    type Properties: serde::de::DeserializeOwned;

    fn data(&self) -> &ObjectData;

    fn valid_index(&self) -> usize;

    fn id(&self) -> Self::Id {
        Uuid::from_slice(
            self.data()
                .objects()
                .column_by_name("id")
                .expect("object data schema should be validated")
                .as_fixed_size_binary()
                .value(self.valid_index()),
        )
        .unwrap()
        .into()
    }

    fn properties(&self) -> Result<Self::Properties> {
        let raw = self
            .data()
            .objects()
            .column_by_name("properties")
            .ok_or(StateError::InconsistentData)?
            .as_string::<i64>()
            .value(self.valid_index());
        Ok(serde_json::from_str(raw)?)
    }
}

pub(crate) async fn read_parquet_dir(
    table_path: &Url,
    ctx: Option<SessionContext>,
) -> Result<RecordBatch> {
    let ctx = ctx.unwrap_or_else(SessionContext::new);
    let session_state = ctx.state();

    // Parse the path
    let table_path = ListingTableUrl::parse(table_path)?;

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    // Resolve the schema
    let resolved_schema = listing_options
        .infer_schema(&session_state, &table_path)
        .await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema.clone());

    // Create a new TableProvider
    let provider = Arc::new(ListingTable::try_new(config)?);

    // This provider can now be read as a dataframe:
    let df = ctx.read_table(provider.clone())?;

    let batches = df.collect().await?;

    Ok(concat_batches(&resolved_schema, &batches)?)
}
