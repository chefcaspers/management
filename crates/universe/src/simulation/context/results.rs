use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;

use crate::Result;

use super::SimulationContext;
pub(crate) use crate::simulation::execution::EVENTS_SCHEMA;

pub(super) static RESULTS_SCHEMA_NAME: &str = "results";
pub(super) static METRICS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", RESULTS_SCHEMA_NAME, "metrics"));
pub(super) static EVENTS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", RESULTS_SCHEMA_NAME, "events"));

pub(crate) static METRICS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("source", DataType::Utf8View, false),
        Field::new("label", DataType::Utf8View, false),
        Field::new("value", DataType::Int64, false),
    ]))
});

pub struct ResultsSchema<'a> {
    pub(super) ctx: &'a SimulationContext,
}

impl ResultsSchema<'_> {
    pub async fn metrics(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 4] = &["timestamp", "source", "label", "value"];
        Ok(self
            .ctx
            .scan_scoped(&METRICS_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn write_metrics(&self, data: DataFrame) -> Result<()> {
        self.ctx
            .extend_df(data)?
            .write_table(METRICS_REF.to_string().as_str(), Default::default())
            .await?;
        Ok(())
    }

    pub async fn events(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 7] = &[
            "id",
            "source",
            "specversion",
            "type",
            "datacontenttype",
            "time",
            "data",
        ];
        Ok(self
            .ctx
            .scan_scoped(&EVENTS_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn write_events(&self, data: DataFrame) -> Result<()> {
        self.ctx
            .extend_df(data)?
            .write_table(EVENTS_REF.to_string().as_str(), Default::default())
            .await?;
        Ok(())
    }
}
