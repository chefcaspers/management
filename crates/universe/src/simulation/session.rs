use std::sync::Arc;

use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableProvider,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::{DataFrame, SessionContext};
use url::Url;

use crate::error::Result;

pub(crate) async fn simulation_context(
    routing_path: &Url,
    objects: Arc<dyn TableProvider>,
    population: Arc<dyn TableProvider>,
) -> Result<SimulationContext> {
    let ctx = SessionContext::new();

    let nodes_path = routing_path.join("nodes/").unwrap();
    let edge_path = routing_path.join("edges/").unwrap();
    let state = ctx.state();
    let routing_nodes = read_parquet_table(&nodes_path, &state).await?;
    let routing_edges = read_parquet_table(&edge_path, &state).await?;

    SimulationContext::try_new(ctx, routing_nodes, routing_edges, objects, population)
}

pub(crate) struct SimulationContext {
    ctx: SessionContext,
}

impl SimulationContext {
    pub(crate) fn try_new(
        ctx: SessionContext,
        routing_nodes: Arc<dyn TableProvider>,
        routing_edges: Arc<dyn TableProvider>,
        objects: Arc<dyn TableProvider>,
        population: Arc<dyn TableProvider>,
    ) -> Result<Self> {
        let system_schema = Arc::new(MemorySchemaProvider::new());
        system_schema.register_table("routing_nodes".into(), routing_nodes)?;
        system_schema.register_table("routing_edges".into(), routing_edges)?;
        system_schema.register_table("objects".into(), objects)?;
        system_schema.register_table("population".into(), population)?;

        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("system", system_schema)?;

        ctx.register_catalog("caspers", catalog);

        Ok(Self { ctx })
    }

    pub(crate) fn system(&self) -> SystemSchema<'_> {
        SystemSchema { ctx: self }
    }
}

pub(crate) struct SystemSchema<'a> {
    ctx: &'a SimulationContext,
}

impl SystemSchema<'_> {
    fn ctx(&self) -> &SessionContext {
        &self.ctx.ctx
    }

    pub(crate) async fn routing_nodes(&self) -> Result<DataFrame> {
        let query = r#"
            SELECT location, id, properties, geometry
            FROM caspers.system.routing_nodes
        "#;
        Ok(self.ctx().sql(query).await?)
    }

    pub(crate) async fn routing_edges(&self) -> Result<DataFrame> {
        let query = r#"
            SELECT location, source, target, properties, geometry
            FROM caspers.system.routing_edges
        "#;
        Ok(self.ctx().sql(query).await?)
    }

    pub(crate) async fn objects(&self) -> Result<DataFrame> {
        let query = r#"
            SELECT id, parent_id, label, name, properties
            FROM caspers.system.objects
        "#;
        Ok(self.ctx().sql(query).await?)
    }

    pub(crate) async fn population(&self) -> Result<DataFrame> {
        let query = r#"
            SELECT id, first_name, last_name, email, cc_number, role, position, state
            FROM caspers.system.population
        "#;
        Ok(self.ctx().sql(query).await?)
    }
}

async fn read_parquet_table(
    table_path: &Url,
    session: &dyn Session,
) -> Result<Arc<dyn TableProvider>> {
    let table_path = ListingTableUrl::parse(table_path)?;

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    // Resolve the schema
    let resolved_schema = listing_options.infer_schema(session, &table_path).await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema.clone());

    Ok(Arc::new(ListingTable::try_new(config)?))
}
