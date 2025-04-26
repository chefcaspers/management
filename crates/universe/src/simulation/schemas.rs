use std::sync::{Arc, LazyLock};

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
use strum::{Display, EnumString};

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

static ORDER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("customer_id", DataType::FixedSizeBinary(16), false),
        Field::new_fixed_size_list(
            "destination",
            Field::new("item", DataType::Float64, false),
            2,
            false,
        ),
    ]))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display)]
#[strum(serialize_all = "snake_case")]
pub enum OrderStatus {
    Submitted,
    Processing,
    Ready,
    Delivered,
}

struct OrderBuilder {
    ids: FixedSizeBinaryBuilder,
    customer_ids: FixedSizeBinaryBuilder,
    destination: FixedSizeListBuilder<Float64Builder>,
}

impl OrderBuilder {
    pub fn new() -> Self {
        Self {
            ids: FixedSizeBinaryBuilder::new(16),
            customer_ids: FixedSizeBinaryBuilder::new(16),
            destination: FixedSizeListBuilder::new(Float64Builder::new(), 2)
                .with_field(Field::new("item", DataType::Float64, false)),
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
        Ok(id)
    }

    pub fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        Ok(RecordBatch::try_new(
            ORDER_SCHEMA.clone(),
            vec![
                Arc::new(self.ids.finish()),
                Arc::new(self.customer_ids.finish()),
                Arc::new(self.destination.finish()),
            ],
        )?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display)]
#[strum(serialize_all = "snake_case")]
pub enum OrderLineStatus {
    Submitted,
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
        let Some((id, _)) = self.index.get_key_value(order_id) else {
            return None;
        };
        Some(OrderView::new(id, self))
    }

    pub fn orders(&self) -> impl Iterator<Item = OrderView<'_>> {
        self.index.iter().map(|(id, _)| OrderView::new(id, self))
    }

    pub fn into_parts(self) -> (RecordBatch, RecordBatch) {
        (self.orders, self.lines)
    }

    pub(crate) fn merge(&self, other: Self) -> Result<Self> {
        let orders = concat_batches(&ORDER_SCHEMA, &[self.orders.clone(), other.orders])?;
        let lines = concat_batches(&ORDER_LINE_SCHEMA, &[self.lines.clone(), other.lines])?;
        Self::try_new(orders, lines)
    }

    pub(crate) fn update_order_line_status(
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
        Ok(())
    }
}

pub struct OrderView<'a> {
    order_id: &'a OrderId,
    data: &'a OrderData,
}

impl<'a> OrderView<'a> {
    fn new(order_id: &'a OrderId, data: &'a OrderData) -> Self {
        Self { order_id, data }
    }

    pub fn id(&self) -> &OrderId {
        self.order_id
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

    pub fn line(&self, line_id: &OrderLineId) -> Option<OrderLineView<'_>> {
        let (_order_idx, (offset, len)) = self.data.index.get(self.order_id)?;
        let (line_idx, line_id) = self.data.lines_index.get_full(line_id)?;
        (line_idx >= *offset && line_idx < *offset + *len)
            .then(|| OrderLineView::new(self.order_id, line_id, self.data))
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
