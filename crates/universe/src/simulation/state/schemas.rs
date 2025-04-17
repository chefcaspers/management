use std::sync::{Arc, LazyLock};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use uuid::Uuid;

static LOCATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("zip_code", DataType::Utf8, false),
        Field::new("street_address", DataType::Utf8, false),
    ]))
});

static BRAND_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
});

static VENDOR_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
});

static KITCHEN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
});

pub(super) fn generate_locations() -> RecordBatch {
    RecordBatch::try_new(
        LOCATION_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
            ])),
            Arc::new(StringArray::from(vec![
                "Location 1",
                "Location 2",
                "Location 3",
            ])),
            Arc::new(StringArray::from(vec![
                "New York",
                "Los Angeles",
                "Chicago",
            ])),
            Arc::new(StringArray::from(vec!["NY", "CA", "IL"])),
            Arc::new(StringArray::from(vec!["10001", "90001", "60601"])),
            Arc::new(StringArray::from(vec![
                "123 Main St",
                "456 Elm St",
                "789 Oak St",
            ])),
        ],
    )
    .unwrap()
}

pub(super) fn generate_brands() -> RecordBatch {
    RecordBatch::try_new(
        BRAND_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
            ])),
            Arc::new(StringArray::from(vec!["Brand 1", "Brand 2", "Brand 3"])),
            Arc::new(StringArray::from(vec![
                "Description 1",
                "Description 2",
                "Description 3",
            ])),
        ],
    )
    .unwrap()
}

pub(super) fn generate_vendors() -> RecordBatch {
    RecordBatch::try_new(
        VENDOR_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
            ])),
            Arc::new(StringArray::from(vec!["Vendor 1", "Vendor 2", "Vendor 3"])),
            Arc::new(StringArray::from(vec![
                "Description 1",
                "Description 2",
                "Description 3",
            ])),
        ],
    )
    .unwrap()
}

pub(super) fn generate_kitchens() -> RecordBatch {
    RecordBatch::try_new(
        KITCHEN_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
            ])),
            Arc::new(StringArray::from(vec![
                "Kitchen 1",
                "Kitchen 2",
                "Kitchen 3",
            ])),
            Arc::new(StringArray::from(vec![
                "Description 1",
                "Description 2",
                "Description 3",
            ])),
        ],
    )
    .unwrap()
}
