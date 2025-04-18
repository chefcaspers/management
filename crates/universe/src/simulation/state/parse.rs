use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use counter::Counter;
use uuid::Uuid;

use crate::agents::Location;
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
