use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;

use crate::SiteSetup;
use crate::error::Result;
use crate::idents::{BrandId, MenuItemId};
use crate::models::{Brand, MenuItem};
use crate::state::ObjectDataBuilder;

static BRANDS: LazyLock<Arc<Vec<Brand>>> = LazyLock::new(|| {
    let mut brands = Vec::new();

    let asian = include_str!("../../../data/menus/asian.json");
    let items: Vec<MenuItem> = serde_json::from_str(asian).unwrap();
    let brand_name = "asian".to_string();
    brands.push(Brand {
        id: BrandId::from_uri_ref(format!("brands/{brand_name}")).to_string(),
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
        id: BrandId::from_uri_ref(format!("brands/{brand_name}")).to_string(),
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
        id: BrandId::from_uri_ref(format!("brands/{brand_name}")).to_string(),
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

pub(crate) fn generate_objects(
    brands: &HashMap<BrandId, Brand>,
    sites: impl IntoIterator<Item = SiteSetup>,
) -> Result<RecordBatch> {
    let mut builder = ObjectDataBuilder::new();

    for (brand_id, brand) in brands.iter() {
        builder.append_brand(brand_id, brand);
    }

    for site in sites {
        builder.append_site_info(&site)?;
    }

    builder.finish()
}

pub fn generate_brands() -> Vec<Brand> {
    BRANDS.clone().as_ref().clone()
}
