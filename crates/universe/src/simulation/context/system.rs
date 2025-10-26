use std::sync::LazyLock;

use arrow::array::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;

use crate::error::Result;

use super::SimulationContext;

pub struct SystemSchema<'a> {
    pub(super) ctx: &'a SimulationContext,
}

pub(super) static ROUTING_NODES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "system", "routing_nodes"));
pub(super) static ROUTING_EDGES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", "system", "routing_edges"));

impl SystemSchema<'_> {
    fn ctx(&self) -> &SessionContext {
        &self.ctx.ctx
    }

    pub(crate) async fn routing_nodes(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT location, id, properties, geometry
                FROM {}
                "#,
                *ROUTING_NODES_REF
            )
        });
        Ok(self.ctx().sql(QUERY.as_str()).await?)
    }

    pub async fn write_routing_nodes(&self, data: RecordBatch) -> Result<()> {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        let df = self.ctx().read_batch(data)?;
        let _ = df
            .write_table(ROUTING_NODES_REF.to_string().as_str(), write_options)
            .await?;
        Ok(())
    }

    pub(crate) async fn routing_edges(&self) -> Result<DataFrame> {
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                r#"
                SELECT location, source, target, properties, geometry
                FROM {}
                "#,
                *ROUTING_EDGES_REF
            )
        });
        Ok(self.ctx().sql(QUERY.as_str()).await?)
    }

    pub async fn write_routing_edges(&self, data: RecordBatch) -> Result<()> {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        let df = self.ctx().read_batch(data)?;
        let _ = df
            .write_table(ROUTING_EDGES_REF.to_string().as_str(), write_options)
            .await?;
        Ok(())
    }
}
