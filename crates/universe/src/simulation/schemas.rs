use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::types::Float64Type;
use arrow_array::{
    RecordBatch, StringArray,
    builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Float64Builder, StringBuilder},
    cast::AsArray,
};
use arrow_ord::partition::partition;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::concat::concat_batches;
use counter::Counter;
use h3o::LatLng;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use strum::{AsRefStr, Display, EnumString};

use crate::idents::{OrderId, OrderLineId};
use crate::{
    error::{Error, Result},
    idents::{BrandId, MenuItemId},
};

use super::state::PersonView;

pub static OBJECT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("parent_id", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
        Field::new(
            "name",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            false,
        ),
        Field::new("properties", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ]))
});

pub static POPULATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("cc_number", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, false),
    ]))
});

pub(crate) static ORDER_DESTINATION_IDX: usize = 2;
pub(crate) static ORDER_STATUS_IDX: usize = 3;
static ORDER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumString, Display, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum OrderStatus {
    /// Customer submitted the order
    Submitted,
    /// Order is being processed
    Processing,
    /// Order is ready for pickup
    Ready,
    /// Order is picked up
    PickedUp,
    /// Order is delivered
    Delivered,
    /// Order is cancelled
    Cancelled,
    /// Order failed to be processed
    Failed,

    /// Catch-all for unknown statuses to avoid panics
    #[strum(default)]
    Unknown(String),
}

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
        Ok(RecordBatch::try_new(
            ORDER_SCHEMA.clone(),
            vec![
                Arc::new(self.ids.finish()),
                Arc::new(self.customer_ids.finish()),
                Arc::new(self.destination.finish()),
                Arc::new(self.statuses.finish()),
            ],
        )?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum OrderLineStatus {
    Submitted,
    Assigned,
    Processing,
    Ready,
    Delivered,
}

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
        self.statuses
            .append_value(OrderLineStatus::Submitted.to_string());
        Ok(id)
    }

    pub fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        Ok(RecordBatch::try_new(
            ORDER_LINE_SCHEMA.clone(),
            vec![
                Arc::new(self.ids.finish()),
                Arc::new(self.order_ids.finish()),
                Arc::new(self.brand_ids.finish()),
                Arc::new(self.menu_item_ids.finish()),
                Arc::new(self.statuses.finish()),
            ],
        )?)
    }
}

pub static ORDER_LINE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("order_id", DataType::FixedSizeBinary(16), false),
        Field::new("brand_id", DataType::FixedSizeBinary(16), false),
        Field::new("menu_item_id", DataType::FixedSizeBinary(16), false),
        // status column MUST be the last column - or update the order data update method.
        Field::new("status", DataType::Utf8, false),
    ]))
});

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
        person: &PersonView,
        destination: LatLng,
        order: &[(BrandId, MenuItemId)],
    ) -> Self {
        let order_id = self.orders.add_order(person.id(), destination).unwrap();
        for (brand_id, menu_item_id) in order {
            self.lines
                .add_line(&order_id, brand_id, menu_item_id)
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

#[derive(Debug, Clone, Default)]
pub struct OrderDataStats {
    total_orders: usize,
    total_lines: usize,
    status: Counter<OrderStatus>,
}

impl std::ops::Add for OrderDataStats {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        OrderDataStats {
            total_orders: self.total_orders + other.total_orders,
            total_lines: self.total_lines + other.total_lines,
            status: self.status + other.status,
        }
    }
}

pub struct OrderData {
    orders: RecordBatch,
    lines: RecordBatch,
    /// Track the index into orders data and corresponding slice of lines
    ///
    /// The slice is expressed as tuple (offset, length)
    index: IndexMap<OrderId, (usize, (usize, usize))>,
    lines_index: IndexSet<OrderLineId>,
}

impl OrderData {
    fn try_new(orders: RecordBatch, lines: RecordBatch) -> Result<Self> {
        if orders.schema().as_ref() != ORDER_SCHEMA.as_ref() {
            return Err(Error::invalid_data("expected orders to have schema").into());
        }
        if lines.schema().as_ref() != ORDER_LINE_SCHEMA.as_ref() {
            return Err(Error::invalid_data("expected lines to have schema").into());
        }
        if orders.num_rows() == 0 && lines.num_rows() > 0 {
            return Err(Error::invalid_data("non-empty lines for empty orders").into());
        }

        if orders.num_rows() == 0 && lines.num_rows() == 0 {
            return Ok(Self {
                orders,
                lines,
                index: IndexMap::new(),
                lines_index: IndexSet::new(),
            });
        }

        let Some((order_id_idx, _)) = lines.schema().column_with_name("order_id") else {
            return Err(Error::invalid_data("expected column 'order_id'").into());
        };

        // partition order lines by their order ids
        let partitions = partition(&lines.columns()[order_id_idx..order_id_idx + 1])?;
        if partitions.len() != orders.num_rows() {
            return Err(Error::invalid_data("expected all orders to have matching lines").into());
        }

        let order_id_col = orders.column_by_name("id").unwrap().as_fixed_size_binary();
        let index = partitions
            .ranges()
            .into_iter()
            .enumerate()
            .map(|(i, range)| {
                let order_id = order_id_col.value(i).try_into()?;
                Ok((order_id, (i, (range.start, (range.end - range.start)))))
            })
            .try_collect::<_, _, Error>()?;

        let lines_index: IndexSet<_> = lines
            .column_by_name("id")
            .unwrap()
            .as_fixed_size_binary()
            .iter()
            .filter_map(|raw| raw.map(TryInto::try_into))
            .try_collect()?;

        if lines_index.len() != lines.num_rows() {
            return Err(Error::invalid_data("expected all lines to have matching ids").into());
        }

        Ok(Self {
            orders,
            lines,
            index,
            lines_index,
        })
    }

    pub(crate) fn batch_orders(&self) -> &RecordBatch {
        &self.orders
    }

    pub(crate) fn batch_lines(&self) -> &RecordBatch {
        &self.lines
    }

    pub fn num_orders(&self) -> usize {
        self.orders.num_rows()
    }

    pub fn num_lines(&self) -> usize {
        self.lines.num_rows()
    }

    pub fn stats(&self) -> OrderDataStats {
        let mut stats = Counter::new();
        for order in self.orders() {
            for line in order.lines() {
                let status = line.status().parse().unwrap();
                stats[&status] += 1;
            }
        }
        OrderDataStats {
            total_orders: self.num_orders(),
            total_lines: self.num_lines(),
            status: stats,
        }
    }

    pub fn order(&self, order_id: &OrderId) -> Option<OrderView<'_>> {
        self.index
            .get_key_value(order_id)
            .map(|(id, (idx, _))| OrderView::new(id, self, *idx))
    }

    pub fn orders(&self) -> impl Iterator<Item = OrderView<'_>> {
        self.index
            .iter()
            .map(|(id, (idx, _))| OrderView::new(id, self, *idx))
    }

    pub fn orders_with_status(&self, status: &OrderStatus) -> impl Iterator<Item = OrderView<'_>> {
        self.orders()
            .filter(|order| order.status() == status.as_ref())
    }

    pub fn into_parts(self) -> (RecordBatch, RecordBatch) {
        (self.orders, self.lines)
    }

    pub(crate) fn merge(&self, other: Self) -> Result<Self> {
        let orders = concat_batches(&ORDER_SCHEMA, &[self.orders.clone(), other.orders])?;
        let lines = concat_batches(&ORDER_LINE_SCHEMA, &[self.lines.clone(), other.lines])?;
        Self::try_new(orders, lines)
    }

    /// Update the status of order lines.
    ///
    /// This will update the status of the order lines and recompute the order status
    /// based on the aggregate status of the order lines.
    pub(crate) fn update_order_lines(
        &mut self,
        updates: impl IntoIterator<Item = (OrderLineId, OrderLineStatus)>,
    ) -> Result<()> {
        let mut current = self
            .lines
            .column_by_name("status")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .filter_map(|s| s.map(|s| s.to_string()))
            .collect_vec();
        if current.len() != self.lines.num_rows() {
            return Err(Error::invalid_data("order line status mismatch").into());
        }
        for (id, status) in updates {
            let Some(idx) = self.lines_index.get_index_of(&id) else {
                return Err(Error::invalid_data("order line not found").into());
            };
            current[idx] = status.to_string();
        }
        // TODO: we assume the status column is always the last column in the schema.
        let new_array = Arc::new(StringArray::from(current));
        let mut arrays = self
            .lines
            .columns()
            .into_iter()
            .cloned()
            .take(self.lines.num_columns() - 1)
            .collect_vec();
        arrays.push(new_array);
        self.lines = RecordBatch::try_new(ORDER_LINE_SCHEMA.clone(), arrays)?;

        let statuses = self
            .orders()
            .map(|order| order.compute_status().to_string());
        let status_arr = Arc::new(StringArray::from(statuses.collect_vec()));
        let mut arrays = self
            .orders
            .columns()
            .into_iter()
            .cloned()
            .take(self.orders.num_columns() - 1)
            .collect_vec();
        arrays.push(status_arr);
        self.orders = RecordBatch::try_new(ORDER_SCHEMA.clone(), arrays)?;

        Ok(())
    }

    /// Update the status of orders.
    pub(crate) fn update_orders(
        &mut self,
        updates: impl IntoIterator<Item = (OrderId, OrderStatus)>,
    ) -> Result<()> {
        let update_map: HashMap<OrderId, OrderStatus> = updates.into_iter().collect();
        let mut statuses = Vec::with_capacity(self.orders.num_rows());
        for order in self.orders() {
            if let Some(status) = update_map.get(order.id()) {
                statuses.push(status.to_string());
            } else {
                statuses.push(order.status().to_string());
            }
        }
        let status_arr = Arc::new(StringArray::from(statuses));
        let mut arrays = self
            .orders
            .columns()
            .into_iter()
            .cloned()
            .take(self.orders.num_columns() - 1)
            .collect_vec();
        arrays.push(status_arr);
        self.orders = RecordBatch::try_new(ORDER_SCHEMA.clone(), arrays)?;
        Ok(())
    }
}

pub struct OrderView<'a> {
    order_id: &'a OrderId,
    data: &'a OrderData,
    valid_index: usize,
}

impl<'a> OrderView<'a> {
    fn new(order_id: &'a OrderId, data: &'a OrderData, valid_index: usize) -> Self {
        Self {
            order_id,
            data,
            valid_index,
        }
    }

    pub fn id(&self) -> &OrderId {
        self.order_id
    }

    pub fn status(&self) -> &str {
        self.data
            .orders
            .column(ORDER_STATUS_IDX)
            .as_string::<i32>()
            .value(self.valid_index)
    }

    fn compute_status(&self) -> OrderStatus {
        let status = self
            .status()
            .parse()
            .unwrap_or(OrderStatus::Unknown(self.status().to_string()));
        match status {
            OrderStatus::Submitted => self
                .is_processing()
                .then(|| OrderStatus::Processing)
                .unwrap_or(status),
            OrderStatus::Processing => self
                .is_ready()
                .then(|| OrderStatus::Ready)
                .unwrap_or(status),
            _ => status,
        }
    }

    pub fn lines(&self) -> impl Iterator<Item = OrderLineView<'_>> {
        let (_order_idx, (offset, len)) = self.data.index.get(self.order_id).unwrap();
        self.data
            .lines_index
            .iter()
            .skip(*offset)
            .take(*len)
            .map(|line_id| OrderLineView::new(self.order_id, line_id, self.data))
    }

    pub fn is_processing(&self) -> bool {
        self.lines()
            .any(|line| line.status() == OrderLineStatus::Processing.as_ref())
    }

    pub fn is_ready(&self) -> bool {
        self.lines()
            .all(|line| line.status() == OrderLineStatus::Ready.as_ref())
    }

    pub fn line(&self, line_id: &OrderLineId) -> Option<OrderLineView<'_>> {
        let (_order_idx, (offset, len)) = self.data.index.get(self.order_id)?;
        let (line_idx, line_id) = self.data.lines_index.get_full(line_id)?;
        (line_idx >= *offset && line_idx < *offset + *len)
            .then(|| OrderLineView::new(self.order_id, line_id, self.data))
    }

    pub fn destination(&self) -> Result<LatLng> {
        let (order_idx, _) = self.data.index.get(self.order_id).unwrap();
        let pos = self
            .data
            .orders
            .column(ORDER_DESTINATION_IDX)
            .as_fixed_size_list()
            .value(*order_idx);
        let vals = pos.as_primitive::<Float64Type>();
        Ok(LatLng::new(vals.value(0), vals.value(1))?)
    }
}

pub struct OrderLineView<'a> {
    order_id: &'a OrderId,
    line_id: &'a OrderLineId,
    data: &'a OrderData,
}

impl<'a> OrderLineView<'a> {
    fn new(order_id: &'a OrderId, line_id: &'a OrderLineId, data: &'a OrderData) -> Self {
        Self {
            order_id,
            line_id,
            data,
        }
    }

    pub fn id(&self) -> &OrderLineId {
        self.line_id
    }

    pub fn order_id(&self) -> &OrderId {
        self.order_id
    }

    pub fn brand_id(&self) -> &[u8] {
        let line_id = self.data.lines_index.get_index_of(self.line_id).unwrap();
        get_id(&self.data.lines, "brand_id", line_id)
    }

    pub fn menu_item_id(&self) -> &[u8] {
        let line_id = self.data.lines_index.get_index_of(self.line_id).unwrap();
        get_id(&self.data.lines, "menu_item_id", line_id)
    }

    pub fn status(&self) -> &str {
        let line_id = self.data.lines_index.get_index_of(self.line_id).unwrap();
        self.data
            .lines
            .column_by_name("status")
            .unwrap()
            .as_string::<i32>()
            .value(line_id)
    }
}

fn get_id<'a>(batch: &'a RecordBatch, name: &str, idx: usize) -> &'a [u8] {
    batch
        .column_by_name(name)
        .unwrap()
        .as_fixed_size_binary()
        .value(idx)
}
