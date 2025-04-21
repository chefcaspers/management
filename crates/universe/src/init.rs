use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_array::builder::{
    FixedSizeBinaryBuilder, ListBuilder, StringBuilder, TimestampMillisecondBuilder,
};
use counter::Counter;
use fake::Fake;
use geo::Point;
use geoarrow::array::{PointArray, PointBuilder};
use geoarrow_schema::Dimension;
use rand::Rng;
use rand::distr::{Distribution, Uniform};
use uuid::Uuid;

use crate::error::Result;
use crate::idents::{BrandId, MenuItemId, PersonId};
use crate::models::{Brand, KitchenStation, MenuItem};
use crate::simulation::schemas::{OBJECT_SCHEMA, POPULATION_SCHEMA};
use crate::{Kitchen, Site};

static BRANDS: LazyLock<Arc<Vec<Brand>>> = LazyLock::new(|| {
    let mut brands = Vec::new();

    let asian = include_str!("../../../data/menus/asian.json");
    let items: Vec<MenuItem> = serde_json::from_str(asian).unwrap();
    let brand_name = "asian".to_string();
    brands.push(Brand {
        id: BrandId::from_uri_ref(&format!("brands/{}", brand_name)).to_string(),
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
        id: BrandId::from_uri_ref(&format!("brands/{}", brand_name)).to_string(),
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
        id: BrandId::from_uri_ref(&format!("brands/{}", brand_name)).to_string(),
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

pub enum ObjectLabel {
    Brand,
    MenuItem,
    Instruction,
    Ingredient,
    IngredientUse,
}

impl AsRef<str> for ObjectLabel {
    fn as_ref(&self) -> &str {
        match self {
            ObjectLabel::Brand => "brand",
            ObjectLabel::MenuItem => "menu_item",
            ObjectLabel::Instruction => "instruction",
            ObjectLabel::Ingredient => "ingredient",
            ObjectLabel::IngredientUse => "ingredient_use",
        }
    }
}

pub fn generate_objects(brands: &HashMap<BrandId, Brand>) -> Result<RecordBatch> {
    let mut id = FixedSizeBinaryBuilder::new(16);
    let mut parent_id = FixedSizeBinaryBuilder::new(16);
    let mut name = ListBuilder::new(StringBuilder::new());
    let mut label = StringBuilder::new();
    let mut properties = StringBuilder::new();
    let mut created_at = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut updated_at = TimestampMillisecondBuilder::new().with_timezone("UTC");

    for (brand_id, brand) in brands.iter() {
        id.append_value(brand_id).unwrap();
        parent_id.append_null();
        label.append_value(ObjectLabel::Brand);
        name.append_value([Some("brands"), Some(&brand.name)]);
        properties.append_null();
        created_at.append_value(chrono::Utc::now().timestamp_millis());
        updated_at.append_null();

        for item in &brand.items {
            let item_name = format!("brands/{}/items/{}", brand.name, item.name);
            let item_id = MenuItemId::from_uri_ref(&item_name);
            id.append_value(item_id).unwrap();
            parent_id.append_value(brand_id).unwrap();
            label.append_value(ObjectLabel::MenuItem);
            name.append_value([
                Some("brands"),
                Some(&brand.name),
                Some("items"),
                Some(&item.name),
            ]);
            properties.append_value(serde_json::to_string(&item).unwrap());
            created_at.append_value(chrono::Utc::now().timestamp_millis());
            updated_at.append_null();
        }
    }

    let id = Arc::new(id.finish());
    let parent_id = Arc::new(parent_id.finish());
    let label = Arc::new(label.finish());
    let name = Arc::new(name.finish());
    let properties = Arc::new(properties.finish());
    let created_at = Arc::new(created_at.finish());
    let updated_at = Arc::new(updated_at.finish());

    Ok(RecordBatch::try_new(
        OBJECT_SCHEMA.clone(),
        vec![
            id, parent_id, label, name, properties, created_at, updated_at,
        ],
    )?)
}

pub fn generate_brands() -> Vec<Brand> {
    BRANDS.clone().as_ref().clone()
}

pub fn generate_site(
    name: impl ToString,
    brands: impl IntoIterator<Item = (BrandId, Brand)>,
) -> Site {
    let location_name = name.to_string();

    let counters: HashMap<BrandId, Counter<KitchenStation>> = brands
        .into_iter()
        .map(|(_id, brand)| {
            let stations = brand
                .items
                .iter()
                .flat_map(|it| it.instructions.iter().map(|step| step.required_station()))
                .collect();
            (Uuid::try_parse(&brand.id).unwrap().into(), stations)
        })
        .collect();

    // Generate 5-10 kitchens for this location
    let num_kitchens = rand::rng().random_range(5..=10);
    let kitchens = generate_kitchens_for_site(&location_name, &counters, num_kitchens);

    // Add kitchens to the location
    let mut location = Site::new(format!("locations/{}", location_name));
    for kitchen in kitchens {
        location.add_kitchen(kitchen);
    }

    location
}

pub fn generate_kitchens_for_site(
    location_name: &str,
    brand_counters: &HashMap<BrandId, Counter<KitchenStation>>,
    num_kitchens: usize,
) -> Vec<Kitchen> {
    let mut kitchens = Vec::with_capacity(num_kitchens);
    let brand_ids: Vec<BrandId> = brand_counters.keys().cloned().collect();
    let mut rng = rand::rng();

    // Distribute brands across kitchens
    for i in 0..num_kitchens {
        let kitchen_name = format!("{}/kitchens/kitchen-{}", location_name, i + 1);
        let mut kitchen = Kitchen::new(&kitchen_name);

        // Randomly select brands for this kitchen (1-3 brands per kitchen)
        let num_brands = rng.random_range(1..=3);
        let selected_brands: HashSet<BrandId> = (0..num_brands)
            .map(|_| brand_ids[rng.random_range(0..brand_ids.len())])
            .collect();

        // Add the brands to the kitchen
        for brand_id in &selected_brands {
            kitchen.add_accepted_brand(brand_id.clone());
        }

        // Calculate the required stations for this kitchen based on selected brands
        let total_stations = selected_brands.iter().fold(Counter::new(), |mut acc, id| {
            acc.extend(&brand_counters[id]);
            acc
        });

        for (station, count) in total_stations {
            let type_name = station_type_to_name(station);
            for _ in 0..count {
                let name = format!("{}/stations/{}-{}", kitchen_name, type_name, count);
                kitchen.add_station(name, station);
            }
        }

        kitchens.push(kitchen);
    }

    kitchens
}

// Helper function to convert station type to readable name
fn station_type_to_name(station_type: KitchenStation) -> &'static str {
    match station_type {
        KitchenStation::Workstation => "workstation",
        KitchenStation::Oven => "oven",
        KitchenStation::Stove => "stove",
        // KitchenStation::Grill => "grill",
        // KitchenStation::Fryer => "fryer",
        // KitchenStation::Freezer => "freezer",
        _ => "unknown",
    }
}

pub(crate) fn generate_population(
    (minx, miny): (f64, f64),
    (maxx, maxy): (f64, f64),
    n_people: usize,
) -> Result<(RecordBatch, PointArray)> {
    // 16 bytes to store raw uuids
    let mut ids = FixedSizeBinaryBuilder::with_capacity(n_people, 16);
    let mut first_names = StringBuilder::new();
    let mut last_names = StringBuilder::new();
    let mut emails = StringBuilder::new();
    let mut cc_numbers = StringBuilder::new();

    let mut rng = rand::rng();

    let gen_first_name = fake::faker::name::en::FirstName();
    let gen_last_name = fake::faker::name::en::LastName();
    let gen_email = fake::faker::internet::en::SafeEmail();
    let gen_cc = fake::faker::creditcard::en::CreditCardNumber();

    for _ in 0..n_people {
        let id = PersonId::new();
        ids.append_value(&id)?;
        first_names.append_value(gen_first_name.fake_with_rng::<String, _>(&mut rng));
        last_names.append_value(gen_last_name.fake_with_rng::<String, _>(&mut rng));
        emails.append_value(gen_email.fake_with_rng::<String, _>(&mut rng));
        cc_numbers.append_value(gen_cc.fake_with_rng::<String, _>(&mut rng));
    }

    let ids = Arc::new(ids.finish());
    let first_names = Arc::new(first_names.finish());
    let last_names = Arc::new(last_names.finish());
    let emails = Arc::new(emails.finish());
    let cc_numbers = Arc::new(cc_numbers.finish());

    let people = RecordBatch::try_new(
        POPULATION_SCHEMA.clone(),
        vec![ids, first_names, last_names, emails, cc_numbers],
    )?;

    let x_range = Uniform::new(minx, maxx)?;
    let y_range = Uniform::new(miny, maxy)?;
    let positions = x_range
        .sample_iter(rand::rng())
        .take(n_people)
        .zip(y_range.sample_iter(rand::rng()).take(n_people))
        .fold(
            PointBuilder::with_capacity(Dimension::XY, n_people),
            |mut builder, (x, y)| {
                builder.push_point(Some(&Point::new(x, y)));
                builder
            },
        )
        .finish();

    Ok((people, positions))
}
