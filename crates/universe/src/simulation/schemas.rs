use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

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
    ]))
});

pub static ORDER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("customer_id", DataType::FixedSizeBinary(16), false),
        Field::new("delivery_address", DataType::Utf8, false),
    ]))
});

pub static ORDER_LINE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("order_id", DataType::FixedSizeBinary(16), false),
        Field::new("menu_item_id", DataType::FixedSizeBinary(16), false),
        Field::new("status", DataType::Utf8, false),
    ]))
});
