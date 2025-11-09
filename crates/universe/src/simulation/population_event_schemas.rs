use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::prelude::{Expr, col, lit, named_struct};

static ORDER_DATA_EXPR: LazyLock<Expr> = LazyLock::new(|| {
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
    .alias("data")
});

pub enum SimulationEvent {
    OrderCreated,
}

impl SimulationEvent {
    pub fn event_type(&self) -> &'static str {
        use SimulationEvent::*;
        match self {
            OrderCreated => "caspers.universe.order_created",
        }
    }

    pub fn event_type_lit(&self) -> Expr {
        lit(self.event_type()).alias("type")
    }

    pub fn data_expr(&self) -> Expr {
        use SimulationEvent::*;
        match self {
            OrderCreated => ORDER_DATA_EXPR.clone(),
        }
    }
}

static ORDER_CREATED_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "order_created",
        DataType::Struct(
            vec![
                Field::new("order_id", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "timestamp",
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

static SITE_CHECK_OUT_FIELD: LazyLock<FieldRef> = LazyLock::new(|| {
    FieldRef::new(Field::new(
        "check_in",
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

static EVENTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        ORDER_CREATED_FIELD.clone(),
        SITE_CHECK_IN_FIELD.clone(),
        SITE_CHECK_OUT_FIELD.clone(),
    ]))
});
