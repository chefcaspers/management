use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::*;
use uuid::Uuid;

use crate::error::Result;
use crate::models::{Brand, MenuItemRef};

mod parse;
mod schemas;

pub struct State {
    ctx: SessionContext,
    brands: Arc<Vec<Brand>>,
    items: Arc<HashMap<Uuid, MenuItemRef>>,
    time_step: Duration,
}

impl State {
    pub fn try_new() -> Result<Self> {
        let ctx = SessionContext::new();

        ctx.register_batch("locations", schemas::generate_locations())?;
        ctx.register_batch("brands", schemas::generate_brands())?;
        ctx.register_batch("vendors", schemas::generate_vendors())?;
        ctx.register_batch("kitchens", schemas::generate_kitchens())?;

        let brands = parse::get_brands();
        let items = brands
            .iter()
            .flat_map(|brand| brand.items.iter())
            .map(|item| (Uuid::try_parse(&item.id).unwrap(), Arc::new(item.clone())))
            .collect();

        Ok(State {
            ctx,
            brands,
            items: Arc::new(items),
            time_step: Duration::from_secs(60),
        })
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn menu_item(&self, id: &Uuid) -> Result<MenuItemRef> {
        self.items.get(id).cloned().ok_or("Brand not found".into())
    }

    pub fn time_step(&self) -> Duration {
        self.time_step
    }
}
