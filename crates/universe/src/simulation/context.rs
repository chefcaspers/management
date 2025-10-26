use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableProvider,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use url::Url;

use crate::error::Result;
use crate::{ObjectDataBuilder, OrderBuilder, OrderLineBuilder, PopulationDataBuilder};

mod system;

pub struct SimulationContext {
    ctx: SessionContext,
}

impl SimulationContext {
    pub async fn try_new_local(routing_path: &Url, snapshots_path: &Url) -> Result<Self> {
        let ctx = SessionContext::new();

        let system_schema = Arc::new(MemorySchemaProvider::new());
        register_routing(&ctx, system_schema.as_ref(), routing_path).await?;
        register_snapshots(system_schema.as_ref(), snapshots_path).await?;

        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("system", system_schema)?;
        ctx.register_catalog("caspers", catalog);

        Ok(Self { ctx })
    }

    pub fn system(&self) -> system::SystemSchema<'_> {
        system::SystemSchema { ctx: self }
    }
}

async fn register_routing(
    ctx: &SessionContext,
    schema: &dyn SchemaProvider,
    routing_path: &Url,
) -> Result<()> {
    let nodes_path = routing_path.join("nodes/")?;
    let edge_path = routing_path.join("edges/")?;

    let state = ctx.state();
    let routing_nodes = read_parquet_table(&nodes_path, &state).await?;
    let routing_edges = read_parquet_table(&edge_path, &state).await?;

    schema.register_table("routing_nodes".into(), routing_nodes)?;
    schema.register_table("routing_edges".into(), routing_edges)?;

    Ok(())
}

async fn register_snapshots(schema: &dyn SchemaProvider, snapshots_path: &Url) -> Result<()> {
    use self::system::{
        SNAPSHOT_OBJECTS_REF, SNAPSHOT_ORDER_LINES_REF, SNAPSHOT_ORDERS_REF,
        SNAPSHOT_POPULATION_REF,
    };

    let population_path = snapshots_path.join("population/")?;
    let population_snapshot =
        snapshot_provider(&population_path, PopulationDataBuilder::snapshot_schema())?;
    schema.register_table(
        SNAPSHOT_POPULATION_REF.table().to_string(),
        population_snapshot,
    )?;

    let objects_path = snapshots_path.join("objects/")?;
    let objects_snapshot = snapshot_provider(&objects_path, ObjectDataBuilder::snapshot_schema())?;
    schema.register_table(SNAPSHOT_OBJECTS_REF.table().to_string(), objects_snapshot)?;

    let orders_path = snapshots_path.join("orders/")?;
    let orders_snapshot = snapshot_provider(&orders_path, OrderBuilder::snapshot_schema())?;
    schema.register_table(SNAPSHOT_ORDERS_REF.table().to_string(), orders_snapshot)?;

    let order_lines_path = snapshots_path.join("order_lines/")?;
    let order_lines_snapshot =
        snapshot_provider(&order_lines_path, OrderLineBuilder::snapshot_schema())?;
    schema.register_table(
        SNAPSHOT_ORDER_LINES_REF.table().to_string(),
        order_lines_snapshot,
    )?;

    Ok(())
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

fn snapshot_provider(table_path: &Url, schema: SchemaRef) -> Result<Arc<dyn TableProvider>> {
    let table_path = ListingTableUrl::parse(table_path)?;

    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    Ok(Arc::new(ListingTable::try_new(config)?))
}
