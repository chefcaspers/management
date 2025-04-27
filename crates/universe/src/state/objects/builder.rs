use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_array::builder::{
    FixedSizeBinaryBuilder, ListBuilder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::error::Result;
use crate::idents::{BrandId, KitchenId, MenuItemId, SiteId, StationId};
use crate::models::{Brand, KitchenStation, Site, Station};
use crate::state::ObjectLabel;

static OBJECT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

pub struct ObjectDataBuilder {
    id: FixedSizeBinaryBuilder,
    parent_id: FixedSizeBinaryBuilder,
    name: ListBuilder<StringBuilder>,
    label: StringBuilder,
    properties: StringBuilder,
    created_at: TimestampMillisecondBuilder,
    updated_at: TimestampMillisecondBuilder,
}

impl Default for ObjectDataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectDataBuilder {
    pub fn new() -> Self {
        Self {
            id: FixedSizeBinaryBuilder::new(16),
            parent_id: FixedSizeBinaryBuilder::new(16),
            name: ListBuilder::new(StringBuilder::new()),
            label: StringBuilder::new(),
            properties: StringBuilder::new(),
            created_at: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            updated_at: TimestampMillisecondBuilder::new().with_timezone("UTC"),
        }
    }

    pub fn append_brand(&mut self, brand_id: &BrandId, brand: &Brand) {
        self.id.append_value(brand_id).unwrap();
        self.parent_id.append_null();
        self.label.append_value(ObjectLabel::Brand);
        self.name.append_value([Some("brands"), Some(&brand.name)]);
        self.properties.append_null();
        self.created_at
            .append_value(chrono::Utc::now().timestamp_millis());
        self.updated_at.append_null();

        for item in &brand.items {
            let item_name = format!("brands/{}/items/{}", brand.name, item.name);
            let item_id = MenuItemId::from_uri_ref(&item_name);
            self.id.append_value(item_id).unwrap();
            self.parent_id.append_value(brand_id).unwrap();
            self.label.append_value(ObjectLabel::MenuItem);
            self.name.append_value([
                Some("brands"),
                Some(&brand.name),
                Some("items"),
                Some(&item.name),
            ]);
            self.properties
                .append_value(serde_json::to_string(&item).unwrap());
            self.created_at
                .append_value(chrono::Utc::now().timestamp_millis());
            self.updated_at.append_null();
        }
    }

    pub fn append_site(&mut self, site_id: SiteId, site: &Site) {
        self.id.append_value(site_id).unwrap();
        self.parent_id.append_null();
        self.label.append_value(ObjectLabel::Site);
        self.name.append_value([Some("sites"), Some(&site.name)]);
        self.properties
            .append_value(serde_json::to_string(&site).unwrap());
        self.created_at
            .append_value(chrono::Utc::now().timestamp_millis());
        self.updated_at.append_null();

        for idx in 0..=5 {
            let kitchen_name = format!("kitchen-{}", idx);
            let kitchen_id =
                KitchenId::from_uri_ref(format!("sites/{}/kitchens/{}", site.name, kitchen_name));
            self.id.append_value(kitchen_id).unwrap();
            self.parent_id.append_value(site_id).unwrap();
            self.label.append_value(ObjectLabel::Kitchen);
            self.name.append_value([
                Some("sites"),
                Some(&site.name),
                Some("kitchens"),
                Some(&kitchen_name),
            ]);
            self.properties.append_null();
            self.created_at
                .append_value(chrono::Utc::now().timestamp_millis());
            self.updated_at.append_null();

            for station in [
                KitchenStation::Workstation,
                KitchenStation::Oven,
                KitchenStation::Stove,
            ] {
                let station_name = station.as_str_name().to_lowercase();
                let station_id = StationId::from_uri_ref(format!(
                    "sites/{}/kitchens/{}/stations/{}",
                    site.name, kitchen_name, station_name
                ));
                self.id.append_value(station_id).unwrap();
                self.parent_id.append_value(kitchen_id).unwrap();
                self.label.append_value(ObjectLabel::Station);
                self.name.append_value([
                    Some("sites"),
                    Some(&site.name),
                    Some("kitchens"),
                    Some(&kitchen_name),
                    Some("stations"),
                    Some(&station_name),
                ]);

                let station_props = Station {
                    id: Some(station_id.to_string()),
                    name: station_name.to_string(),
                    station_type: station as i32,
                };
                self.properties
                    .append_value(serde_json::to_string(&station_props).unwrap());

                self.created_at
                    .append_value(chrono::Utc::now().timestamp_millis());
                self.updated_at.append_null();
            }
        }
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let id = Arc::new(self.id.finish());
        let parent_id = Arc::new(self.parent_id.finish());
        let label = Arc::new(self.label.finish());
        let name = Arc::new(self.name.finish());
        let properties = Arc::new(self.properties.finish());
        let created_at = Arc::new(self.created_at.finish());
        let updated_at = Arc::new(self.updated_at.finish());

        Ok(RecordBatch::try_new(
            OBJECT_SCHEMA.clone(),
            vec![
                id, parent_id, label, name, properties, created_at, updated_at,
            ],
        )?)
    }
}
