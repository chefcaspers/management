use crate::idents::{KitchenId, OrderId, OrderLineId, PersonId};
use crate::state::{OrderLineStatus, OrderStatus, PersonStatus};

pub struct PersonUpdatedPayload {
    pub person_id: PersonId,
    pub status: PersonStatus,
}

pub struct OrderUpdatedPayload {
    pub order_id: OrderId,
    pub status: OrderStatus,
    pub actor_id: Option<PersonId>,
}

pub struct OrderLineUpdatedPayload {
    pub order_line_id: OrderLineId,
    pub status: OrderLineStatus,
    pub kitchen_id: Option<KitchenId>,
    pub actor_id: Option<PersonId>,
}

pub enum EventPayload {
    PersonUpdated(PersonUpdatedPayload),
    OrderUpdated(OrderUpdatedPayload),
    OrderLineUpdated(OrderLineUpdatedPayload),
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
