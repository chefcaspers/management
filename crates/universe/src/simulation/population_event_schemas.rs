use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::{
    prelude::{DataFrame, Expr, cast, col, concat, lit, named_struct},
    scalar::ScalarValue,
};

use crate::{
    OrderLineStatus, Result, SimulationContext,
    functions::{uuid_to_string, uuidv7},
};

pub(crate) struct EventsHelper {}

impl EventsHelper {
    pub(crate) fn empty(ctx: &SimulationContext) -> Result<DataFrame> {
        let empty_events = RecordBatch::new_empty(EVENT_SCHEMA.clone());
        Ok(ctx.ctx().read_batch(empty_events)?)
    }

    pub(crate) fn orders_created(orders: DataFrame) -> Result<DataFrame> {
        Ok(orders.select([
            uuidv7().call(vec![col("submitted_at")]).alias("id"),
            concat(vec![
                lit("/population/"),
                uuid_to_string().call(vec![col("person_id")]),
            ])
            .alias("source"),
            lit("1.0").alias("specversion"),
            SimulationEvent::OrderCreated.event_type_lit(),
            cast(col("submitted_at"), DataType::LargeUtf8).alias("time"),
            named_struct(vec![
                lit("order_created"),
                ORDER_CREATED_EXPR.clone(),
                lit("order_line_updated"),
                ORDER_LINE_UPDATED_NULL.clone(),
                lit("step_started"),
                ORDER_LINE_STEP_STARTED_NULL.clone(),
                lit("step_finished"),
                ORDER_LINE_STEP_FINISHED_NULL.clone(),
                lit("check_in"),
                SITE_CHECK_IN_NULL.clone(),
                lit("check_out"),
                SITE_CHECK_OUT_NULL.clone(),
            ])
            .alias("data"),
        ])?)
    }

    pub(crate) fn step_started(order_lines: DataFrame) -> Result<DataFrame> {
        Ok(order_lines.select([
            uuidv7().call(vec![col("timestamp")]).alias("id"),
            concat(vec![
                lit("/stations/"),
                uuid_to_string().call(vec![col("station_id")]),
            ])
            .alias("source"),
            lit("1.0").alias("specversion"),
            SimulationEvent::OrderLineStepStarted.event_type_lit(),
            cast(col("timestamp"), DataType::LargeUtf8).alias("time"),
            named_struct(vec![
                lit("order_created"),
                ORDER_CREATED_NULL.clone(),
                lit("order_line_updated"),
                ORDER_LINE_UPDATED_NULL.clone(),
                lit("step_started"),
                ORDER_LINE_STEP_EXPR.clone(),
                lit("step_finished"),
                ORDER_LINE_STEP_FINISHED_NULL.clone(),
                lit("check_in"),
                SITE_CHECK_IN_NULL.clone(),
                lit("check_out"),
                SITE_CHECK_OUT_NULL.clone(),
            ])
            .alias("data"),
        ])?)
    }

    pub(crate) fn step_finished(order_lines: DataFrame) -> Result<DataFrame> {
        Ok(order_lines.select([
            uuidv7().call(vec![col("timestamp")]).alias("id"),
            concat(vec![
                lit("/stations/"),
                uuid_to_string().call(vec![col("station_id")]),
            ])
            .alias("source"),
            lit("1.0").alias("specversion"),
            SimulationEvent::OrderLineStepFinished.event_type_lit(),
            cast(col("timestamp"), DataType::LargeUtf8).alias("time"),
            named_struct(vec![
                lit("order_created"),
                ORDER_CREATED_NULL.clone(),
                lit("order_line_updated"),
                ORDER_LINE_UPDATED_NULL.clone(),
                lit("step_started"),
                ORDER_LINE_STEP_STARTED_NULL.clone(),
                lit("step_finished"),
                ORDER_LINE_STEP_EXPR.clone(),
                lit("check_in"),
                SITE_CHECK_IN_NULL.clone(),
                lit("check_out"),
                SITE_CHECK_OUT_NULL.clone(),
            ])
            .alias("data"),
        ])?)
    }

    pub(crate) fn order_line_ready(order_lines: DataFrame) -> Result<DataFrame> {
        Ok(order_lines.select([
            uuidv7().call(vec![col("timestamp")]).alias("id"),
            concat(vec![
                lit("/kitchen/"),
                uuid_to_string().call(vec![col("kitchen_id")]),
            ])
            .alias("source"),
            lit("1.0").alias("specversion"),
            SimulationEvent::OrderLineUpdated.event_type_lit(),
            cast(col("timestamp"), DataType::LargeUtf8).alias("time"),
            named_struct(vec![
                lit("order_created"),
                ORDER_CREATED_NULL.clone(),
                lit("order_line_updated"),
                ORDER_LINE_UPDATED_COMPLETED_EXPR.clone(),
                lit("step_started"),
                ORDER_LINE_STEP_STARTED_NULL.clone(),
                lit("step_finished"),
                ORDER_LINE_STEP_FINISHED_NULL.clone(),
                lit("check_in"),
                SITE_CHECK_IN_NULL.clone(),
                lit("check_out"),
                SITE_CHECK_OUT_NULL.clone(),
            ])
            .alias("data"),
        ])?)
    }
}

pub enum SimulationEvent {
    OrderCreated,
    OrderPickedUp,
    OrderDelivered,
    OrderLineStepStarted,
    OrderLineStepFinished,
    OrderLineUpdated,
    SiteCheckIn,
    SiteCheckOut,
}

impl SimulationEvent {
    pub fn event_type(&self) -> &'static str {
        use SimulationEvent::*;
        match self {
            OrderCreated => "caspers.universe.order_created",
            OrderPickedUp => "caspers.universe.order_picked_up",
            OrderDelivered => "caspers.universe.order_delivered",
            OrderLineStepStarted => "caspers.universe.order_line_step_started",
            OrderLineStepFinished => "caspers.universe.order_line_step_finished",
            OrderLineUpdated => "caspers.universe.order_line_updated",
            SiteCheckIn => "caspers.universe.site_check_in",
            SiteCheckOut => "caspers.universe.site_check_out",
        }
    }

    fn event_type_lit(&self) -> Expr {
        lit(self.event_type()).alias("type")
    }
}

static ORDER_CREATED_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "order_created",
        DataType::Struct(
            vec![
                Field::new("order_id", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "submitted_at",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "destination",
                    DataType::Struct(
                        vec![
                            Field::new("x", DataType::Float64, false),
                            Field::new("y", DataType::Float64, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::FixedSizeBinary(16), false)),
                            2,
                        ),
                        false,
                    ))),
                    false,
                ),
            ]
            .into(),
        ),
        true,
    ))
});

static ORDER_CREATED_EXPR: LazyLock<Expr> = LazyLock::new(|| {
    named_struct(vec![
        lit("order_id"),
        col("order_id"),
        lit("submitted_at"),
        col("submitted_at"),
        lit("destination"),
        col("destination"),
        lit("items"),
        col("items"),
    ])
    .alias("order_crated")
});

static ORDER_CREATED_NULL: LazyLock<Expr> = LazyLock::new(|| {
    let data_type = match ORDER_CREATED_FIELD.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias("order_created")
});

static ORDER_LINE_STEP_STARTED_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "step_started",
        DataType::Struct(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new("order_line_id", DataType::FixedSizeBinary(16), false),
                Field::new("step_index", DataType::Int32, false),
            ]
            .into(),
        ),
        true,
    ))
});

static ORDER_LINE_STEP_EXPR: LazyLock<Expr> = LazyLock::new(|| {
    named_struct(vec![
        lit("timestamp"),
        col("timestamp"),
        lit("order_line_id"),
        col("order_line_id"),
        lit("step_index"),
        col("step_index"),
    ])
});

static ORDER_LINE_STEP_STARTED_NULL: LazyLock<Expr> = LazyLock::new(|| {
    let data_type = match ORDER_LINE_STEP_STARTED_FIELD.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias("step_started")
});

static ORDER_LINE_STEP_FINISHED_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "step_finished",
        DataType::Struct(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new("order_line_id", DataType::FixedSizeBinary(16), false),
                Field::new("step_index", DataType::Int32, false),
            ]
            .into(),
        ),
        true,
    ))
});

static ORDER_LINE_STEP_FINISHED_NULL: LazyLock<Expr> = LazyLock::new(|| {
    let data_type = match ORDER_LINE_STEP_FINISHED_FIELD.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias("step_finished")
});

static ORDER_LINE_UPDATED_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "order_line_updated",
        DataType::Struct(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new("order_line_id", DataType::FixedSizeBinary(16), false),
                Field::new("status", DataType::Utf8, false),
            ]
            .into(),
        ),
        true,
    ))
});

static ORDER_LINE_UPDATED_NULL: LazyLock<Expr> = LazyLock::new(|| {
    let data_type = match ORDER_LINE_UPDATED_FIELD.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias("order_line_updated")
});

static ORDER_LINE_UPDATED_COMPLETED_EXPR: LazyLock<Expr> = LazyLock::new(|| {
    named_struct(vec![
        lit("timestamp"),
        col("timestamp"),
        lit("order_line_id"),
        col("order_line_id"),
        lit("status"),
        lit(OrderLineStatus::Ready.as_ref()),
    ])
});

static SITE_CHECK_IN_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "check_in",
        DataType::Struct(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new("site_id", DataType::FixedSizeBinary(16), false),
            ]
            .into(),
        ),
        true,
    ))
});

static SITE_CHECK_IN_NULL: LazyLock<Expr> = LazyLock::new(|| {
    let data_type = match SITE_CHECK_IN_FIELD.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias("check_in")
});

static SITE_CHECK_OUT_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "check_out",
        DataType::Struct(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "orders",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::FixedSizeBinary(16),
                        false,
                    ))),
                    false,
                ),
            ]
            .into(),
        ),
        true,
    ))
});

static SITE_CHECK_OUT_NULL: LazyLock<Expr> = LazyLock::new(|| {
    let data_type = match SITE_CHECK_OUT_FIELD.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias("check_out")
});

static EVENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("source", DataType::Utf8, false),
        Field::new("specversion", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "data",
            DataType::Struct(
                vec![
                    ORDER_CREATED_FIELD.clone(),
                    ORDER_LINE_UPDATED_FIELD.clone(),
                    ORDER_LINE_STEP_STARTED_FIELD.clone(),
                    ORDER_LINE_STEP_FINISHED_FIELD.clone(),
                    SITE_CHECK_IN_FIELD.clone(),
                    SITE_CHECK_OUT_FIELD.clone(),
                ]
                .into(),
            ),
            false,
        ),
    ]))
});
