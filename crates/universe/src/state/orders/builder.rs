use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_array::builder::{
    FixedSizeBinaryBuilder, FixedSizeListBuilder, Float64Builder, StringBuilder,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use h3o::LatLng;

use super::{OrderData, OrderLineStatus, OrderStatus};
use crate::error::Result;
use crate::idents::{BrandId, MenuItemId, OrderId, OrderLineId, PersonId};

pub struct OrderDataBuilder {
    orders: OrderBuilder,
    lines: OrderLineBuilder,
}

impl OrderDataBuilder {
    pub fn new() -> Self {
        Self {
            orders: OrderBuilder::new(),
            lines: OrderLineBuilder::new(),
        }
    }

    pub fn add_order(
        mut self,
        person_id: &PersonId,
        destination: LatLng,
        order: &[(BrandId, MenuItemId)],
    ) -> Self {
        let order_id = self.orders.add_order(*person_id, destination).unwrap();
        for (brand_id, menu_item_id) in order {
            self.lines
                .add_line(order_id, brand_id, menu_item_id)
                .unwrap();
        }
        self
    }

    pub fn finish(self) -> Result<OrderData> {
        let orders = self.orders.finish()?;
        let lines = self.lines.finish()?;
        OrderData::try_new(orders, lines)
    }
}

pub(super) static ORDER_LINE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("order_id", DataType::FixedSizeBinary(16), false),
        Field::new("brand_id", DataType::FixedSizeBinary(16), false),
        Field::new("menu_item_id", DataType::FixedSizeBinary(16), false),
        // status column MUST be the last column - or update the order data update method.
        Field::new("status", DataType::Utf8, false),
    ]))
});

struct OrderLineBuilder {
    ids: FixedSizeBinaryBuilder,
    order_ids: FixedSizeBinaryBuilder,
    brand_ids: FixedSizeBinaryBuilder,
    menu_item_ids: FixedSizeBinaryBuilder,
    statuses: StringBuilder,
}

impl OrderLineBuilder {
    pub fn new() -> Self {
        Self {
            ids: FixedSizeBinaryBuilder::new(16),
            order_ids: FixedSizeBinaryBuilder::new(16),
            brand_ids: FixedSizeBinaryBuilder::new(16),
            menu_item_ids: FixedSizeBinaryBuilder::new(16),
            statuses: StringBuilder::new(),
        }
    }

    pub fn add_line(
        &mut self,
        order_id: impl AsRef<[u8]>,
        brand_id: impl AsRef<[u8]>,
        menu_item_id: impl AsRef<[u8]>,
    ) -> Result<OrderLineId, ArrowError> {
        let id = OrderLineId::new();
        self.ids.append_value(id)?;
        self.order_ids.append_value(order_id)?;
        self.brand_ids.append_value(brand_id)?;
        self.menu_item_ids.append_value(menu_item_id)?;
        self.statuses.append_value(OrderLineStatus::Submitted);
        Ok(id)
    }

    pub fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            ORDER_LINE_SCHEMA.clone(),
            vec![
                Arc::new(self.ids.finish()),
                Arc::new(self.order_ids.finish()),
                Arc::new(self.brand_ids.finish()),
                Arc::new(self.menu_item_ids.finish()),
                Arc::new(self.statuses.finish()),
            ],
        )
    }
}

pub(super) static ORDER_DESTINATION_IDX: usize = 2;
pub(super) static ORDER_STATUS_IDX: usize = 3;
pub(super) static ORDER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = Vec::with_capacity(4);
    fields.push(Field::new("id", DataType::FixedSizeBinary(16), false));
    fields.push(Field::new(
        "customer_id",
        DataType::FixedSizeBinary(16),
        false,
    ));
    fields.push(Field::new_fixed_size_list(
        "destination",
        Field::new("item", DataType::Float64, false),
        2,
        false,
    ));
    fields.push(Field::new("status", DataType::Utf8, false));
    SchemaRef::new(Schema::new(fields))
});

struct OrderBuilder {
    ids: FixedSizeBinaryBuilder,
    customer_ids: FixedSizeBinaryBuilder,
    destination: FixedSizeListBuilder<Float64Builder>,
    statuses: StringBuilder,
}

impl OrderBuilder {
    pub fn new() -> Self {
        Self {
            ids: FixedSizeBinaryBuilder::new(16),
            customer_ids: FixedSizeBinaryBuilder::new(16),
            destination: FixedSizeListBuilder::new(Float64Builder::new(), 2)
                .with_field(Field::new("item", DataType::Float64, false)),
            statuses: StringBuilder::new(),
        }
    }

    pub fn add_order(
        &mut self,
        customer_id: impl AsRef<[u8]>,
        destination: LatLng,
    ) -> Result<OrderId, ArrowError> {
        let id = OrderId::new();
        self.ids.append_value(id)?;
        self.customer_ids.append_value(customer_id)?;
        self.destination.values().append_value(destination.lat());
        self.destination.values().append_value(destination.lng());
        self.destination.append(true);
        self.statuses.append_value(OrderStatus::Submitted.as_ref());
        Ok(id)
    }

    pub fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            ORDER_SCHEMA.clone(),
            vec![
                Arc::new(self.ids.finish()),
                Arc::new(self.customer_ids.finish()),
                Arc::new(self.destination.finish()),
                Arc::new(self.statuses.finish()),
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_schema() {
        let schema = ORDER_SCHEMA.clone();

        let destination = schema.field(ORDER_DESTINATION_IDX);
        assert_eq!(destination.name(), "destination");

        let status = schema.field(ORDER_STATUS_IDX);
        assert_eq!(status.name(), "status");
        assert_eq!(status.data_type(), &DataType::Utf8);
        assert!(schema.fields().len() == ORDER_STATUS_IDX + 1);
    }
}
