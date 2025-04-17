use std::sync::{Arc, LazyLock};

use crate::models::{Brand, Menu, MenuItem};

pub static BRANDS: LazyLock<Arc<Vec<Brand>>> = LazyLock::new(|| {
    let mut brands = Vec::new();

    let asian = include_str!("../../../../../data/menus/asian.json");
    let items: Vec<MenuItem> = serde_json::from_str(asian).unwrap();
    brands.push(Brand {
        id: uuid::Uuid::new_v4().to_string(),
        name: "brands/asian".to_string(),
        description: "Asian cuisine".to_string(),
        menu: Some(Menu {
            id: uuid::Uuid::new_v4().to_string(),
            name: "brands/asian/menu".to_string(),
            description: "Asian cuisine".to_string(),
            category: "Asian".to_string(),
            items,
        }),
    });

    let mexican = include_str!("../../../../../data/menus/mexican.json");
    let items: Vec<MenuItem> = serde_json::from_str(mexican).unwrap();
    brands.push(Brand {
        id: uuid::Uuid::new_v4().to_string(),
        name: "brands/mexican".to_string(),
        description: "Mexican cuisine".to_string(),
        menu: Some(Menu {
            id: uuid::Uuid::new_v4().to_string(),
            name: "brands/mexican/menu".to_string(),
            description: "Mexican cuisine".to_string(),
            category: "Mexican".to_string(),
            items,
        }),
    });

    let fast_food = include_str!("../../../../../data/menus/fast_food.json");
    let items: Vec<MenuItem> = serde_json::from_str(fast_food).unwrap();
    brands.push(Brand {
        id: uuid::Uuid::new_v4().to_string(),
        name: "brands/fast-food".to_string(),
        description: "Fast food".to_string(),
        menu: Some(Menu {
            id: uuid::Uuid::new_v4().to_string(),
            name: "brands/fast-food/menu".to_string(),
            description: "Fast food".to_string(),
            category: "Fast Food".to_string(),
            items,
        }),
    });

    Arc::new(brands)
});

pub fn parse_brands() -> Arc<Vec<Brand>> {
    BRANDS.clone()
}
