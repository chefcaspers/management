use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableProvider,
};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;
use url::Url;

use crate::error::Result;
use crate::{ObjectDataBuilder, OrderBuilder, OrderLineBuilder, PopulationDataBuilder};

static SNAPSHOT_POPULATION_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "system", "snapshot_population"));
static SNAPSHOT_OBJECTS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "system", "snapshot_objects"));
static SNAPSHOT_ORDERS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "system", "snapshot_orders"));
static SNAPSHOT_ORDER_LINES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "system", "snapshot_order_lines"));

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

    pub fn system(&self) -> SystemSchema<'_> {
        SystemSchema { ctx: self }
    }
}

pub struct SystemSchema<'a> {
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

    pub async fn objects(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT id, parent_id, label, name, properties
                FROM {}
                "#,
                SNAPSHOT_OBJECTS_REF.to_string()
            )
        });
        Ok(self.ctx().sql(QUERY.as_str()).await?)
    }

    pub async fn write_objects(&self, data: RecordBatch) -> Result<()> {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        let df = self.ctx().read_batch(data)?;
        let _ = df
            .write_table(SNAPSHOT_OBJECTS_REF.to_string().as_str(), write_options)
            .await?;
        Ok(())
    }

    pub async fn population(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT id, first_name, last_name, email, cc_number, role, position, state
                FROM {}
                "#,
                SNAPSHOT_POPULATION_REF.to_string()
            )
        });
        Ok(self.ctx().sql(QUERY.as_str()).await?)
    }

    pub async fn write_population(&self, data: RecordBatch) -> Result<()> {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        let df = self.ctx().read_batch(data)?;
        let _ = df
            .write_table(SNAPSHOT_POPULATION_REF.to_string().as_str(), write_options)
            .await?;
        Ok(())
    }

    pub async fn orders(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT id, site_id, customer_id, destination, status
                FROM {}
                "#,
                SNAPSHOT_ORDERS_REF.to_string()
            )
        });
        Ok(self.ctx().sql(QUERY.as_str()).await?)
    }

    pub async fn write_orders(&self, data: RecordBatch) -> Result<()> {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        let df = self.ctx().read_batch(data)?;
        let _ = df
            .write_table(SNAPSHOT_ORDERS_REF.to_string().as_str(), write_options)
            .await?;
        Ok(())
    }

    pub async fn order_lines(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT id, order_id, brand_id, menu_item_id, status
                FROM {}
                "#,
                SNAPSHOT_ORDER_LINES_REF.to_string()
            )
        });
        Ok(self.ctx().sql(QUERY.as_str()).await?)
    }

    pub async fn write_order_lines(&self, data: RecordBatch) -> Result<()> {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        let df = self.ctx().read_batch(data)?;
        let _ = df
            .write_table(SNAPSHOT_ORDER_LINES_REF.to_string().as_str(), write_options)
            .await?;
        Ok(())
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
