use std::sync::LazyLock;

use arrow::array::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;

use crate::error::Result;

use super::SimulationContext;

pub struct SnapshotsSchema<'a> {
    pub(super) ctx: &'a SimulationContext,
}

pub(super) static SNAPSHOT_POPULATION_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "population"));
pub(super) static SNAPSHOT_OBJECTS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "objects"));
pub(super) static SNAPSHOT_ORDERS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "orders"));
pub(super) static SNAPSHOT_ORDER_LINES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "snapshots", "order_lines"));

impl SnapshotsSchema<'_> {
    fn ctx(&self) -> &SessionContext {
        &self.ctx.ctx
    }

    pub async fn objects(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT id, parent_id, label, name, properties
                FROM {}
                "#,
                *SNAPSHOT_OBJECTS_REF
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
                *SNAPSHOT_POPULATION_REF
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
                *SNAPSHOT_ORDERS_REF
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
                *SNAPSHOT_ORDER_LINES_REF
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
