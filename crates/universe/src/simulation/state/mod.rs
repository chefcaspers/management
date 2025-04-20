use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use rand::Rng;
use uuid::Uuid;

use self::population::PopulationData;
use crate::error::Result;
use crate::idents::*;
use crate::models::{Brand, MenuItemRef};

mod population;

pub struct State {
    ctx: SessionContext,
    brands: Arc<HashMap<BrandId, Brand>>,
    items: Arc<HashMap<(BrandId, MenuItemId), MenuItemRef>>,
    item_ids: Vec<(BrandId, MenuItemId)>,

    /// Current simulation time
    time: DateTime<Utc>,

    /// Time increment per simulation step
    time_step: Duration,

    /// Population data
    population: PopulationData,
}

impl State {
    pub fn try_new() -> Result<Self> {
        let ctx = SessionContext::new();

        // ctx.register_batch("locations", schemas::generate_locations())?;
        // ctx.register_batch("brands", schemas::generate_brands())?;
        // ctx.register_batch("vendors", schemas::generate_vendors())?;
        // ctx.register_batch("kitchens", schemas::generate_kitchens())?;

        let brands: HashMap<_, _> = crate::init::generate_brands()
            .as_ref()
            .clone()
            .into_iter()
            .map(|brand| {
                let brand_id = Uuid::try_parse(&brand.id).unwrap().into();
                (brand_id, brand)
            })
            .collect();

        let items: HashMap<_, _> = brands
            .iter()
            .flat_map(|(brand_id, brand)| brand.items.iter().map(|it| (*brand_id, it)))
            .map(|(brand_id, item)| {
                (
                    (brand_id, Uuid::try_parse(&item.id).unwrap().into()),
                    Arc::new(item.clone()),
                )
            })
            .collect();
        let item_ids = items.keys().cloned().collect();

        let n_people = rand::rng().random_range(100..1000);
        let population = PopulationData::from_site((0., 0.), (1., 1.), n_people)?;

        Ok(State {
            ctx,
            brands: Arc::new(brands),
            items: Arc::new(items),
            item_ids,
            time_step: Duration::from_secs(60),
            time: Utc::now(),
            population,
        })
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn menu_item(&self, id: &(BrandId, MenuItemId)) -> Result<MenuItemRef> {
        self.items.get(id).cloned().ok_or("Brand not found".into())
    }

    pub fn menu_items(&self) -> Arc<HashMap<(BrandId, MenuItemId), MenuItemRef>> {
        self.items.clone()
    }

    fn sample_menu_items(
        &self,
        count: Option<usize>,
        rng: &mut rand::rngs::ThreadRng,
    ) -> Vec<(BrandId, MenuItemRef)> {
        let count = count.unwrap_or_else(|| rng.random_range(1..11));
        let mut selected_items = Vec::with_capacity(count);
        for _ in 0..count {
            let item_index = rng.random_range(0..self.item_ids.len());
            if let Some(item) = self.items.get(&self.item_ids[item_index]) {
                selected_items.push((self.item_ids[item_index].0, item.clone()));
            }
        }
        selected_items
    }

    pub fn orders_for_location(
        &self,
        _location_id: &SiteId,
    ) -> impl Iterator<Item = Vec<(BrandId, MenuItemRef)>> {
        let mut rng = rand::rng();
        let order_count = rng.random_range(1..11);
        [0..order_count]
            .map(|_| self.sample_menu_items(Some(3), &mut rng))
            .into_iter()
    }

    pub fn time_step(&self) -> Duration {
        self.time_step
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn next_time(&self) -> DateTime<Utc> {
        self.time + self.time_step
    }

    pub fn step(&mut self) {
        self.time += self.time_step;
    }
}
