use geo::Point;

use crate::idents::{BrandId, KitchenId, MenuItemId, OrderId, OrderLineId, PersonId, SiteId};
use crate::state::{OrderLineStatus, OrderStatus, PersonStatus};

#[derive(Debug, Clone)]
pub struct OrderCreatedPayload {
    pub site_id: SiteId,
    pub person_id: PersonId,
    pub items: Vec<(BrandId, MenuItemId)>,
    pub destination: Point,
}

#[derive(Debug, Clone)]
pub struct PersonUpdatedPayload {
    pub person_id: PersonId,
    pub status: PersonStatus,
}

#[derive(Debug, Clone)]
pub struct OrderUpdatedPayload {
    pub order_id: OrderId,
    pub status: OrderStatus,
    pub actor_id: Option<PersonId>,
}

#[derive(Debug, Clone)]
pub struct OrderLineUpdatedPayload {
    pub order_line_id: OrderLineId,
    pub status: OrderLineStatus,
    pub kitchen_id: Option<KitchenId>,
    pub actor_id: Option<PersonId>,
}

#[derive(Debug, Clone)]
pub enum EventPayload {
    PersonUpdated(PersonUpdatedPayload),
    OrderUpdated(OrderUpdatedPayload),
    OrderLineUpdated(OrderLineUpdatedPayload),
    OrderCreated(OrderCreatedPayload),
}

impl EventPayload {
    pub fn person_updated(person_id: PersonId, status: PersonStatus) -> Self {
        Self::PersonUpdated(PersonUpdatedPayload { person_id, status })
    }

    pub fn order_updated(
        order_id: OrderId,
        status: OrderStatus,
        actor_id: Option<PersonId>,
    ) -> Self {
        Self::OrderUpdated(OrderUpdatedPayload {
            order_id,
            status,
            actor_id,
        })
    }

    pub fn order_line_updated(
        order_line_id: OrderLineId,
        status: OrderLineStatus,
        kitchen_id: Option<KitchenId>,
        actor_id: Option<PersonId>,
    ) -> Self {
        Self::OrderLineUpdated(OrderLineUpdatedPayload {
            order_line_id,
            status,
            kitchen_id,
            actor_id,
        })
    }

    pub fn order_failed(order_id: OrderId, actor_id: Option<PersonId>) -> Self {
        Self::OrderUpdated(OrderUpdatedPayload {
            order_id,
            status: OrderStatus::Failed,
            actor_id,
        })
    }
}
