use chrono::{DateTime, Utc};

use crate::idents::{OrderId, OrderLineId, PersonId};

pub struct OrderCreatedPayload {
    pub timestamp: DateTime<Utc>,
    pub order_id: OrderId,
    pub customer_id: PersonId,
}

pub struct OrderLineReadyPayload {
    pub timestamp: DateTime<Utc>,
    pub order_line_id: OrderLineId,
}

pub enum Event {
    OrderCreated(OrderCreatedPayload),
    OrderLineReady(OrderLineReadyPayload),
}
