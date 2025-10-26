use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field, SchemaBuilder};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableProvider,
};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use url::Url;
use uuid::Uuid;

use crate::error::Result;
use crate::simulation::context::snapshots::create_snapshot;
use crate::simulation::context::system::{
    SIMULATION_META_REF, SIMULATION_META_SCHEMA, SNAPSHOT_META_REF, SNAPSHOT_META_SCHEMA,
    SimulationMetaBuilder,
};
use crate::{
    Error, ObjectData, ObjectDataBuilder, OrderBuilder, OrderData, OrderLineBuilder,
    PopulationData, PopulationDataBuilder, State,
};

mod snapshots;
mod system;

#[derive(Default)]
pub struct SimulationContextBuilder {
    simulation_id: Option<Uuid>,
    snapshot_id: Option<Uuid>,

    routing_location: Option<Url>,
    snapshots_location: Option<Url>,
    working_directory: Option<Url>,

    object_data: Option<ObjectData>,
    population_data: Option<RecordBatch>,
}

impl SimulationContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_simulation_id(mut self, simulation_id: impl Into<Option<Uuid>>) -> Self {
        self.simulation_id = simulation_id.into();
        self
    }

    pub fn with_snapshot_id(mut self, snapshot_id: impl Into<Option<Uuid>>) -> Self {
        self.snapshot_id = snapshot_id.into();
        self
    }

    pub fn with_routing_location(mut self, routing_location: impl Into<Option<Url>>) -> Self {
        self.routing_location = routing_location.into();
        self
    }

    pub fn with_snapshots_location(mut self, snapshots_location: impl Into<Option<Url>>) -> Self {
        self.snapshots_location = snapshots_location.into();
        self
    }

    pub fn with_working_directory(mut self, working_directory: impl Into<Option<Url>>) -> Self {
        self.working_directory = working_directory.into();
        self
    }

    pub fn with_object_data(mut self, objects_provider: ObjectData) -> Self {
        self.object_data = Some(objects_provider);
        self
    }

    pub fn with_population_data(mut self, population_data: RecordBatch) -> Self {
        self.population_data = Some(population_data);
        self
    }

    fn session(&self) -> (SessionContext, Uuid) {
        let simulation_id = self.simulation_id.unwrap_or_else(Uuid::now_v7);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_session_id(simulation_id.to_string())
            .build();
        (SessionContext::new_with_state(state), simulation_id)
    }

    pub async fn load_snapshots(&self) -> Result<DataFrame> {
        let (ctx, _) = self.session();
        let Some(working_directory) = &self.working_directory else {
            return Err(Error::internal("System location not set"));
        };
        let system_location =
            working_directory.join(&format!("{}/", SNAPSHOT_META_REF.schema().unwrap()))?;
        let snapshots_location =
            system_location.join(&format!("{}/", SNAPSHOT_META_REF.table()))?;
        let snapshots = json_provider(&snapshots_location, SNAPSHOT_META_SCHEMA.clone())?;

        let df = ctx.read_table(snapshots)?;
        if let Some(simulation_id) = self.simulation_id {
            return Ok(df
                .filter(
                    col("simulation_id")
                        .eq(lit(ScalarValue::Utf8View(Some(simulation_id.to_string())))),
                )?
                .sort(vec![col("id").sort(false, false)])?);
        }
        Ok(df)
    }

    pub async fn load_simulations(&self) -> Result<DataFrame> {
        let (ctx, _) = self.session();

        let Some(working_directory) = &self.working_directory else {
            return Err(Error::internal("System location not set"));
        };
        let system_location =
            working_directory.join(&format!("{}/", SIMULATION_META_REF.schema().unwrap()))?;
        let simulations_location =
            system_location.join(&format!("{}/", SIMULATION_META_REF.table()))?;
        let simulations = json_provider(&simulations_location, SIMULATION_META_SCHEMA.clone())?;

        Ok(ctx.read_table(simulations)?)
    }

    pub async fn build(self) -> Result<SimulationContext> {
        let (ctx, simulation_id) = self.session();

        let system_schema = self.build_system(&ctx).await?;
        let snapshots_schema = self.build_snapshots(&ctx).await?;

        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("system", system_schema)?;
        catalog.register_schema("snapshots", snapshots_schema)?;
        ctx.register_catalog("caspers", catalog);

        let snapshot_id = if let Some(snapshot_id) = self.snapshot_id {
            snapshot_id
        } else {
            Uuid::now_v7()
        };

        let mut sim_ctx = SimulationContext {
            ctx,
            simulation_id,
            snapshot_id,
        };

        match (self.population_data, self.object_data) {
            (None, None) => (),
            (Some(population_data), Some(object_data)) => {
                let population = sim_ctx.ctx().read_batch(population_data)?;
                let population_data = PopulationData::try_new_with_frame(population).await?;
                let sim_state = State::new(
                    None,
                    object_data,
                    population_data,
                    OrderData::empty(),
                    Default::default(),
                );
                sim_ctx.write_snapshot(&sim_state).await?;
            }
            _ => {
                return Err(Error::internal(
                    "To initialize simulation, both population and object data are required",
                ));
            }
        }

        // if no id was assigned, we created a new simulation and now need to register it
        if self.simulation_id.is_none() {
            let mut builder = SimulationMetaBuilder::new();
            builder.add_simulation(&simulation_id, None);
            let batch = builder.build()?;
            let df = sim_ctx.ctx().read_batch(batch)?;
            let write_options =
                DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
            df.write_table(SIMULATION_META_REF.to_string().as_str(), write_options)
                .await?;
        }

        Ok(sim_ctx)
    }

    async fn build_system(&self, ctx: &SessionContext) -> Result<Arc<dyn SchemaProvider>> {
        let system_schema = Arc::new(MemorySchemaProvider::new());
        if let Some(routing_location) = &self.routing_location {
            register_system(ctx, system_schema.as_ref(), routing_location).await?;
        } else if let Some(working_directory) = &self.working_directory {
            let routing_location = working_directory.join("system/")?;
            register_system(ctx, system_schema.as_ref(), &routing_location).await?;
        } else {
            return Err(Error::internal("Routing location is not provided"));
        }
        Ok(system_schema)
    }

    async fn build_snapshots(&self, _ctx: &SessionContext) -> Result<Arc<dyn SchemaProvider>> {
        let snapshots_schema = Arc::new(MemorySchemaProvider::new());
        if let Some(snapshots_location) = &self.snapshots_location {
            register_snapshots(snapshots_schema.as_ref(), snapshots_location).await?;
        } else if let Some(working_directory) = &self.working_directory {
            let snapshots_location = working_directory.join("snapshots/")?;
            register_snapshots(snapshots_schema.as_ref(), &snapshots_location).await?;
        } else {
            return Err(Error::internal("Snapshots location is not provided"));
        }
        Ok(snapshots_schema)
    }
}

pub struct SimulationContext {
    simulation_id: Uuid,
    snapshot_id: Uuid,
    ctx: SessionContext,
}

impl SimulationContext {
    pub fn builder() -> SimulationContextBuilder {
        SimulationContextBuilder::default()
    }

    pub async fn load_state(&self) -> Result<State> {
        todo!("Implement the load_state method")
    }

    fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn system(&self) -> system::SystemSchema<'_> {
        system::SystemSchema { ctx: self }
    }

    pub fn snapshots(&self) -> snapshots::SnapshotsSchema<'_> {
        snapshots::SnapshotsSchema { ctx: self }
    }

    pub async fn write_snapshot(&mut self, state: &State) -> Result<()> {
        let snapshot_id = create_snapshot(state, self).await?;
        self.snapshot_id = snapshot_id;
        Ok(())
    }
}

async fn register_system(
    ctx: &SessionContext,
    schema: &dyn SchemaProvider,
    system_location: &Url,
) -> Result<()> {
    use self::system::{
        ROUTING_EDGES_REF, ROUTING_NODES_REF, SIMULATION_META_REF, SIMULATION_META_SCHEMA,
        SNAPSHOT_META_REF, SNAPSHOT_META_SCHEMA,
    };

    let state = ctx.state();

    let nodes_path = system_location.join(&format!("{}/", ROUTING_NODES_REF.table()))?;
    let routing_nodes = read_parquet_table(&nodes_path, &state).await?;
    schema.register_table(ROUTING_NODES_REF.table().into(), routing_nodes)?;

    let edge_path = system_location.join(&format!("{}/", ROUTING_EDGES_REF.table()))?;
    let routing_edges = read_parquet_table(&edge_path, &state).await?;
    schema.register_table(ROUTING_EDGES_REF.table().into(), routing_edges)?;

    let simulations_path = system_location.join(&format!("{}/", SIMULATION_META_REF.table()))?;
    let simulations = json_provider(&simulations_path, SIMULATION_META_SCHEMA.clone())?;
    schema.register_table(SIMULATION_META_REF.table().into(), simulations)?;

    let snapshots_path = system_location.join(&format!("{}/", SNAPSHOT_META_REF.table()))?;
    let snapshots = json_provider(&snapshots_path, SNAPSHOT_META_SCHEMA.clone())?;
    schema.register_table(SNAPSHOT_META_REF.table().into(), snapshots)?;

    Ok(())
}

async fn register_snapshots(schema: &dyn SchemaProvider, snapshots_path: &Url) -> Result<()> {
    use self::snapshots::{OBJECTS_REF, ORDER_LINES_REF, ORDERS_REF, POPULATION_REF};

    let population_path = snapshots_path.join("population/")?;
    let population_snapshot =
        snapshot_provider(&population_path, PopulationDataBuilder::snapshot_schema())?;
    schema.register_table(POPULATION_REF.table().to_string(), population_snapshot)?;

    let objects_path = snapshots_path.join("objects/")?;
    let objects_snapshot = snapshot_provider(&objects_path, ObjectDataBuilder::snapshot_schema())?;
    schema.register_table(OBJECTS_REF.table().to_string(), objects_snapshot)?;

    let orders_path = snapshots_path.join("orders/")?;
    let orders_snapshot = snapshot_provider(&orders_path, OrderBuilder::snapshot_schema())?;
    schema.register_table(ORDERS_REF.table().to_string(), orders_snapshot)?;

    let order_lines_path = snapshots_path.join("order_lines/")?;
    let order_lines_snapshot =
        snapshot_provider(&order_lines_path, OrderLineBuilder::snapshot_schema())?;
    schema.register_table(ORDER_LINES_REF.table().to_string(), order_lines_snapshot)?;

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

    let mut builder = SchemaBuilder::new();
    for field in schema.fields() {
        builder.push(field.clone());
    }
    builder.push(Field::new("simulation_id", DataType::Utf8View, false));
    builder.push(Field::new("snapshot_id", DataType::Utf8View, false));
    let schema = builder.finish();

    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema.into());

    Ok(Arc::new(ListingTable::try_new(config)?))
}

fn json_provider(table_path: &Url, schema: SchemaRef) -> Result<Arc<dyn TableProvider>> {
    let table_path = ListingTableUrl::parse(table_path)?;

    let file_format = JsonFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(".json");

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    Ok(Arc::new(ListingTable::try_new(config)?))
}
