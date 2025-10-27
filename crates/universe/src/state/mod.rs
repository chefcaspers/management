//! Internal state management for the simulation.
//!
//! This module provides structures and utilities to manage the internal state of the simulation.
//! Whenever feasible, state is tracked as Arrow RecordBatches for seamless introp with
//! external data storages that might be used to store the state.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::{RecordBatch, cast::AsArray as _};
use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use geo_traits::PointTrait;
use itertools::Itertools as _;
use uuid::Uuid;

use crate::idents::*;
use crate::{
    Error, EventPayload, OrderLineUpdatedPayload, OrderUpdatedPayload, Result, SimulationConfig,
};

use self::movement::JourneyPlanner;

pub(crate) use self::movement::RoutingData;
pub(crate) use self::objects::ObjectDataBuilder;
pub use self::objects::{ObjectData, ObjectLabel};
pub use self::orders::OrderData;
pub(crate) use self::orders::{
    OrderBuilder, OrderDataBuilder, OrderLineBuilder, OrderLineStatus, OrderStatus,
};
pub use self::population::{PersonRole, PersonStatus, PopulationData, PopulationDataBuilder};

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
    pub(crate) fn new(
        config: &SimulationConfig,
        objects: ObjectData,
        population: PopulationData,
        orders: OrderData,
        routing: HashMap<SiteId, RoutingData>,
    ) -> Self {
        Self {
            time_step: Duration::from_secs(config.time_increment.num_seconds() as u64),
            time: config.simulation_start,
            population,
            objects,
            orders,
            routing: routing
                .into_iter()
                .map(|(id, data)| (id, data.into_trip_planner()))
                .collect(),
        }
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

    pub(crate) fn process_site_events(&mut self, events: &[EventPayload]) -> Result<()> {
        let order_line_updates = events.iter().filter_map(|event| match event {
            EventPayload::OrderLineUpdated(payload) => Some(payload),
            _ => None,
        });
        self.update_order_lines(order_line_updates)?;
        let order_updates = events.iter().filter_map(|event| match event {
            EventPayload::OrderUpdated(payload) => Some(payload),
            _ => None,
        });
        self.update_orders(order_updates)?;

        Ok(())
    }

    pub(crate) fn process_population_events(
        &mut self,
        events: &[EventPayload],
    ) -> Result<Vec<EventPayload>> {
        let new_orders = events.iter().filter_map(|event| match event {
            EventPayload::OrderCreated(payload) => Some(payload),
            _ => None,
        });

        let order_data = new_orders
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

        let order_ids = order_data
            .all_orders()
            .map(|o| {
                EventPayload::OrderUpdated(OrderUpdatedPayload {
                    order_id: *o.id(),
                    status: OrderStatus::Submitted,
                    actor_id: None,
                })
            })
            .collect_vec();
        self.orders = self.orders.merge(order_data)?;
        Ok(order_ids)
    }

    fn update_order_lines<'a>(
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

    fn update_orders<'a>(
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

    /// Advance people's journeys and update their statuses on arrival at their destination.
    pub(super) fn move_people(&mut self) -> Result<Vec<EventPayload>> {
        self.population
            .update_journeys(&self.time, self.time_step, &self.orders)
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
        ctx.register_batch("population", self.population.snapshot().clone())?;
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
