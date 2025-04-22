use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::mapref::one::Ref;
use datafusion::prelude::*;
use itertools::Itertools;
use rand::Rng;
use uuid::Uuid;

use crate::error::Result;
use crate::idents::*;
use crate::models::{Brand, MenuItem, MenuItemRef, Site};

mod population;
mod vendors;

pub(crate) use population::PopulationData;
pub(crate) use vendors::VendorData;

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

    /// Vendor data
    vendors: VendorData,
}

impl State {
    pub(crate) fn try_new(
        brands: impl IntoIterator<Item = (BrandId, Brand)>,
        sites: impl IntoIterator<Item = (SiteId, Site)>,
    ) -> Result<Self> {
        let brands: HashMap<_, _> = brands.into_iter().collect();
        let items: HashMap<_, _> = brands
            .iter()
            .flat_map(|(brand_id, brand)| brand.items.iter().map(|it| (*brand_id, it)))
            .map(|(brand_id, item)| {
                Ok::<_, Box<dyn std::error::Error>>((
                    (brand_id, Uuid::try_parse(&item.id)?.into()),
                    Arc::new(item.clone()),
                ))
            })
            .try_collect()?;

        let n_people = rand::rng().random_range(100..1000);
        let population = PopulationData::from_site((0., 0.), (1., 1.), n_people)?;

        let vendors = crate::init::generate_objects(&brands, sites)?;

        Ok(State {
            ctx: SessionContext::new(),
            brands: Arc::new(brands),
            item_ids: items.keys().cloned().collect(),
            items: Arc::new(items),
            time_step: Duration::from_secs(60),
            time: Utc::now(),
            population,
            vendors: VendorData::try_new(vendors)?,
        })
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn menu_item(&self, id: &(BrandId, MenuItemId)) -> Result<Ref<'_, MenuItemId, MenuItem>> {
        self.vendors.menu_item(id)
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

    pub(crate) fn orders_for_site(
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
