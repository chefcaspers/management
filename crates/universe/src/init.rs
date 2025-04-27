use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_array::builder::{
    FixedSizeBinaryBuilder, ListBuilder, StringBuilder, TimestampMillisecondBuilder,
};
use fake::Fake;
use geo::Centroid;
use geo::{BoundingRect, LineString, Point, Polygon};
use geoarrow::array::PointBuilder;
use geoarrow_schema::Dimension;
use h3o::{LatLng, Resolution};
use rand::distr::{Distribution, Uniform};
use rand::rngs::ThreadRng;

use crate::error::Result;
use crate::idents::{BrandId, KitchenId, MenuItemId, PersonId, SiteId, StationId};
use crate::models::{Brand, KitchenStation, MenuItem, Site, Station};
use crate::simulation::schemas::{OBJECT_SCHEMA, POPULATION_SCHEMA};
use crate::state::{ObjectLabel, PersonRole, PopulationData};

static BRANDS: LazyLock<Arc<Vec<Brand>>> = LazyLock::new(|| {
    let mut brands = Vec::new();

    let asian = include_str!("../../../data/menus/asian.json");
    let items: Vec<MenuItem> = serde_json::from_str(asian).unwrap();
    let brand_name = "asian".to_string();
    brands.push(Brand {
        id: Some(BrandId::from_uri_ref(format!("brands/{}", brand_name)).to_string()),
        name: brand_name.clone(),
        description: "Asian cuisine".to_string(),
        category: "Asian".to_string(),
        items: items
            .into_iter()
            .map(|mut it| {
                let item_name = format!("brands/{}/items/{}", brand_name, it.name);
                it.id = MenuItemId::from_uri_ref(&item_name).to_string();
                it
            })
            .collect(),
    });

    let mexican = include_str!("../../../data/menus/mexican.json");
    let items: Vec<MenuItem> = serde_json::from_str(mexican).unwrap();
    let brand_name = "mexican".to_string();
    brands.push(Brand {
        id: Some(BrandId::from_uri_ref(format!("brands/{}", brand_name)).to_string()),
        name: brand_name.clone(),
        description: "Mexican cuisine".to_string(),
        category: "Mexican".to_string(),
        items: items
            .into_iter()
            .map(|mut it| {
                let item_name = format!("brands/{}/items/{}", brand_name, it.name);
                it.id = MenuItemId::from_uri_ref(&item_name).to_string();
                it
            })
            .collect(),
    });

    let fast_food = include_str!("../../../data/menus/fast_food.json");
    let items: Vec<MenuItem> = serde_json::from_str(fast_food).unwrap();
    let brand_name = "fast-food".to_string();
    brands.push(Brand {
        id: Some(BrandId::from_uri_ref(format!("brands/{}", brand_name)).to_string()),
        name: brand_name.clone(),
        description: "Fast food".to_string(),
        category: "Fast Food".to_string(),
        items: items
            .into_iter()
            .map(|mut it| {
                let item_name = format!("brands/{}/items/{}", brand_name, it.name);
                it.id = MenuItemId::from_uri_ref(&item_name).to_string();
                it
            })
            .collect(),
    });

    Arc::new(brands)
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

pub fn generate_objects(
    brands: &HashMap<BrandId, Brand>,
    sites: impl IntoIterator<Item = (SiteId, Site)>,
) -> Result<RecordBatch> {
    let mut builder = ObjectDataBuilder::new();

    for (brand_id, brand) in brands.iter() {
        builder.append_brand(brand_id, brand);
    }

    for (site_id, site) in sites {
        builder.append_site(site_id, &site);
    }

    builder.finish()
}

pub fn generate_brands() -> Vec<Brand> {
    BRANDS.clone().as_ref().clone()
}

pub struct PopulationDataBuilder {
    ids: FixedSizeBinaryBuilder,
    first_names: StringBuilder,
    last_names: StringBuilder,
    emails: StringBuilder,
    cc_numbers: StringBuilder,
    roles: StringBuilder,
    positions: PointBuilder,

    rng: ThreadRng,
}

impl PopulationDataBuilder {
    pub fn new() -> Self {
        Self {
            ids: FixedSizeBinaryBuilder::new(16),
            first_names: StringBuilder::new(),
            last_names: StringBuilder::new(),
            emails: StringBuilder::new(),
            cc_numbers: StringBuilder::new(),
            roles: StringBuilder::new(),
            positions: PointBuilder::new(Dimension::XY),
            rng: rand::rng(),
        }
    }

    pub fn add_site(&mut self, site: &Site, n_people: usize) -> Result<()> {
        let gen_first_name = fake::faker::name::en::FirstName();
        let gen_last_name = fake::faker::name::en::LastName();
        let gen_email = fake::faker::internet::en::SafeEmail();
        let gen_cc = fake::faker::creditcard::en::CreditCardNumber();

        tracing::info!("Adding {} people", n_people);
        for _ in 0..n_people {
            let id = PersonId::new();
            self.ids.append_value(id)?;
            self.first_names
                .append_value(gen_first_name.fake_with_rng::<String, _>(&mut self.rng));
            self.last_names
                .append_value(gen_last_name.fake_with_rng::<String, _>(&mut self.rng));
            self.emails
                .append_value(gen_email.fake_with_rng::<String, _>(&mut self.rng));
            self.cc_numbers
                .append_value(gen_cc.fake_with_rng::<String, _>(&mut self.rng));
            self.roles.append_value(PersonRole::Customer.as_ref());
        }

        let latlng = LatLng::new(site.latitude, site.longitude)?;
        let cell_index = latlng.to_cell(Resolution::Six);
        let boundary: LineString = cell_index.boundary().into_iter().cloned().collect();
        let polygon = Polygon::new(boundary, Vec::new());

        let bounding_rect = polygon.bounding_rect().unwrap();
        let (maxx, maxy) = bounding_rect.max().x_y();
        let (minx, miny) = bounding_rect.min().x_y();

        let x_range = Uniform::new(minx, maxx)?;
        let y_range = Uniform::new(miny, maxy)?;
        x_range
            .sample_iter(rand::rng())
            .take(n_people)
            .zip(y_range.sample_iter(rand::rng()).take(n_people))
            .for_each(|(x, y)| {
                self.positions.push_point(Some(&Point::new(x, y)));
            });

        let n_couriers = n_people / 10;
        tracing::info!("Adding {} couriers", n_couriers);
        let loc = polygon.centroid().unwrap();
        for _ in 0..n_couriers {
            let id = PersonId::new();
            self.ids.append_value(id)?;
            self.first_names
                .append_value(gen_first_name.fake_with_rng::<String, _>(&mut self.rng));
            self.last_names
                .append_value(gen_last_name.fake_with_rng::<String, _>(&mut self.rng));
            self.emails
                .append_value(gen_email.fake_with_rng::<String, _>(&mut self.rng));
            self.cc_numbers
                .append_value(gen_cc.fake_with_rng::<String, _>(&mut self.rng));
            self.roles.append_value(PersonRole::Courier.as_ref());
            self.positions.push_point(Some(&loc));
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<PopulationData> {
        let ids = Arc::new(self.ids.finish());
        let first_names = Arc::new(self.first_names.finish());
        let last_names = Arc::new(self.last_names.finish());
        let emails = Arc::new(self.emails.finish());
        let cc_numbers = Arc::new(self.cc_numbers.finish());
        let roles = Arc::new(self.roles.finish());
        let people = RecordBatch::try_new(
            POPULATION_SCHEMA.clone(),
            vec![ids, first_names, last_names, emails, cc_numbers, roles],
        )?;

        PopulationData::try_new(people, self.positions.finish())
    }
}
