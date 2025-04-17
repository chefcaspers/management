use datafusion::prelude::*;
use uuid::Uuid;

use crate::error::Result;
use crate::models::MenuItem;

mod parse;
mod schemas;

pub struct State {
    ctx: SessionContext,
}

impl State {
    pub fn try_new() -> Result<Self> {
        let ctx = SessionContext::new();

        ctx.register_batch("locations", schemas::generate_locations())?;
        ctx.register_batch("brands", schemas::generate_brands())?;
        ctx.register_batch("vendors", schemas::generate_vendors())?;
        ctx.register_batch("kitchens", schemas::generate_kitchens())?;

        Ok(State { ctx })
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn menu_item(&self, id: &Uuid) -> Result<MenuItem> {
        todo!()
    }
}
