use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use counter::Counter;
use rand::Rng;
use uuid::Uuid;

use crate::agents::{Kitchen, Location};
use crate::idents::{BrandId, MenuItemId};
use crate::models::{Brand, KitchenStation, MenuItem};

pub static BRANDS: LazyLock<Arc<Vec<Brand>>> = LazyLock::new(|| {
    let mut brands = Vec::new();

    let asian = include_str!("../../../../../data/menus/asian.json");
    let items: Vec<MenuItem> = serde_json::from_str(asian).unwrap();
    let brand_name = "brands/asian".to_string();
    brands.push(Brand {
        id: BrandId::from_uri_ref(&brand_name).as_ref().to_string(),
        name: brand_name.clone(),
        description: "Asian cuisine".to_string(),
        category: "Asian".to_string(),
        items: items
            .into_iter()
            .map(|mut it| {
                let item_name = format!("{}/items/{}", brand_name, it.name);
                it.id = MenuItemId::from_uri_ref(&item_name).as_ref().to_string();
                it
            })
            .collect(),
    });

    let mexican = include_str!("../../../../../data/menus/mexican.json");
    let items: Vec<MenuItem> = serde_json::from_str(mexican).unwrap();
    let brand_name = "brands/mexican".to_string();
    brands.push(Brand {
        id: Uuid::new_v5(&Uuid::NAMESPACE_URL, brand_name.as_bytes()).to_string(),
        name: brand_name.clone(),
        description: "Mexican cuisine".to_string(),
        category: "Mexican".to_string(),
        items: items
            .into_iter()
            .map(|mut it| {
                let item_name = format!("{}/items/{}", brand_name, it.name);
                it.id = MenuItemId::from_uri_ref(&item_name).as_ref().to_string();
                it
            })
            .collect(),
    });

    let fast_food = include_str!("../../../../../data/menus/fast_food.json");
    let items: Vec<MenuItem> = serde_json::from_str(fast_food).unwrap();
    let brand_name = "brands/fast-food".to_string();
    brands.push(Brand {
        id: Uuid::new_v5(&Uuid::NAMESPACE_URL, brand_name.as_bytes()).to_string(),
        name: brand_name.clone(),
        description: "Fast food".to_string(),
        category: "Fast Food".to_string(),
        items: items
            .into_iter()
            .map(|mut it| {
                let item_name = format!("{}/items/{}", brand_name, it.name);
                it.id = MenuItemId::from_uri_ref(&item_name).as_ref().to_string();
                it
            })
            .collect(),
    });

    Arc::new(brands)
});

pub fn get_brands() -> Arc<Vec<Brand>> {
    BRANDS.clone()
}

pub fn generate_location(name: impl ToString, brands: &[Brand]) -> Location {
    let location_name = name.to_string();

    let counters: HashMap<BrandId, Counter<KitchenStation>> = brands
        .iter()
        .map(|brand| {
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
    let kitchens = generate_kitchens_for_location(&location_name, &counters, num_kitchens);

    // Add kitchens to the location
    let mut location = Location::new(format!("locations/{}", location_name));
    for kitchen in kitchens {
        location.add_kitchen(kitchen);
    }

    location
}

pub fn generate_kitchens_for_location(
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
