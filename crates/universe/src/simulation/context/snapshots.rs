use std::sync::LazyLock;

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use itertools::Itertools;
use uuid::Uuid;

use crate::error::Result;
use crate::simulation::context::SNAPSHOT_META_REF;
use crate::simulation::context::system::SnapshotMetaBuilder;
use crate::{Error, State};

use super::SimulationContext;

pub struct SnapshotsSchema<'a> {
    pub(super) ctx: &'a SimulationContext,
}

pub(super) static POPULATION_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "population"));
pub(super) static OBJECTS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "objects"));
pub(super) static ORDERS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "orders"));
pub(super) static ORDER_LINES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "order_lines"));

impl SnapshotsSchema<'_> {
    fn ctx(&self) -> &SessionContext {
        &self.ctx.ctx
    }

    pub async fn objects(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 5] = &["id", "parent_id", "label", "name", "properties"];
        self.select_table(&OBJECTS_REF, COLUMNS).await
    }

    pub async fn population(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 8] = &[
            "id",
            "first_name",
            "last_name",
            "email",
            "cc_number",
            "role",
            "position",
            "state",
        ];
        self.select_table(&POPULATION_REF, COLUMNS).await
    }

    pub async fn orders(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["id", "site_id", "customer_id", "destination", "status"];
        self.select_table(&ORDERS_REF, COLUMNS).await
    }

    pub async fn order_lines(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["id", "order_id", "brand_id", "menu_item_id", "status"];
        self.select_table(&ORDER_LINES_REF, COLUMNS).await
    }

    async fn select_table(
        &self,
        table_ref: &TableReference,
        columns: &[&str],
    ) -> Result<DataFrame> {
        let schema = {
            let state = self.ctx().state_ref();
            state.read().schema_for_ref(table_ref.clone())?
        };
        let Some(table) = schema.table(table_ref.table()).await? else {
            return Err(Error::internal(format!(
                "Table '{}' not registered",
                table_ref
            )));
        };
        let predicate = col("simulation_id")
            .eq(lit(ScalarValue::Utf8View(Some(
                self.ctx.simulation_id.to_string(),
            ))))
            .and(col("snapshot_id").eq(lit(ScalarValue::Utf8View(Some(
                self.ctx.snapshot_id.to_string(),
            )))));
        Ok(self
            .ctx()
            .read_table(table)?
            .filter(predicate)?
            .select_columns(columns)?)
    }
}

pub(super) async fn create_snapshot(state: &State, ctx: &SimulationContext) -> Result<Uuid> {
    let snapshot_id = Uuid::now_v7();
    let id_val = ScalarValue::Utf8View(Some(snapshot_id.to_string()));
    let sim_id_val = ScalarValue::Utf8View(Some(ctx.simulation_id.to_string()));

    let append_cols = |df: DataFrame| -> Result<DataFrame> {
        Ok(df
            .with_column("simulation_id", lit(sim_id_val.clone()))?
            .with_column("snapshot_id", lit(id_val.clone()))?)
    };

    let mut tasks_defs = vec![];

    let batch_objects = state.objects().objects();
    if batch_objects.num_rows() > 0 {
        let df_objects = ctx.ctx().read_batch(batch_objects.clone())?;
        let df_objects = append_cols(df_objects)?;
        tasks_defs.push((OBJECTS_REF.to_string(), df_objects))
    }

    let batch_population = state.population().snapshot();
    if batch_population.num_rows() > 0 {
        let df_population = ctx.ctx().read_batch(batch_population)?;
        let df_population = append_cols(df_population)?;
        tasks_defs.push((POPULATION_REF.to_string(), df_population))
    }

    let batch_orders = state.orders().batch_orders();
    if batch_orders.num_rows() > 0 {
        let df_orders = ctx.ctx().read_batch(batch_orders.clone())?;
        let df_orders = append_cols(df_orders)?;
        tasks_defs.push((ORDERS_REF.to_string(), df_orders))
    }

    let batch_order_lines = state.orders().batch_lines();
    if batch_order_lines.num_rows() > 0 {
        let df_order_lines = ctx.ctx().read_batch(batch_order_lines.clone())?;
        let df_order_lines = append_cols(df_order_lines)?;
        tasks_defs.push((ORDER_LINES_REF.to_string(), df_order_lines))
    }

    let mut batch_sn = SnapshotMetaBuilder::new();
    batch_sn.add_snapshot(&snapshot_id, &ctx.simulation_id, state.current_time(), None);
    let batch_snapshot = batch_sn.build()?;
    let df_sn = ctx.ctx().read_batch(batch_snapshot)?;
    tasks_defs.push((SNAPSHOT_META_REF.to_string(), df_sn));

    let write_table = async |df: DataFrame, table_name: String| {
        let write_options = DataFrameWriteOptions::default()
            .with_insert_operation(InsertOp::Append)
            .with_partition_by(vec!["simulation_id".into()]);
        df.write_table(table_name.as_str(), write_options).await
    };

    let tasks = tasks_defs
        .into_iter()
        .map(|(table_name, df)| write_table(df, table_name))
        .collect::<Vec<_>>();

    let _results: Vec<_> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .try_collect()?;

    Ok(snapshot_id)
}
