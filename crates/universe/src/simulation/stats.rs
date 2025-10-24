use std::sync::{Arc, LazyLock};

use arrow::array::builder::{Int64Builder, TimestampMillisecondBuilder};
use arrow::array::{RecordBatch, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};

use crate::EventStats;
use crate::error::Result;

pub(crate) static STATS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("label", DataType::Utf8View, false),
        Field::new("value", DataType::Int64, false),
    ]))
});

pub(crate) struct EventStatsBuffer {
    timestamp: TimestampMillisecondBuilder,
    label: StringViewBuilder,
    value: Int64Builder,
}

impl EventStatsBuffer {
    pub(crate) fn new() -> Self {
        Self {
            timestamp: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            label: StringViewBuilder::new(),
            value: Int64Builder::new(),
        }
    }

    pub(crate) fn push_stats(
        &mut self,
        current_time: DateTime<Utc>,
        stats: &EventStats,
    ) -> Result<()> {
        let ts = current_time.timestamp_millis();

        self.timestamp.append_value(ts);
        self.label.append_value("orders_created");
        self.value.append_value(stats.num_orders_created as i64);

        self.timestamp.append_value(ts);
        self.label.append_value("orders_updated");
        self.value.append_value(stats.num_orders_updated as i64);

        self.timestamp.append_value(ts);
        self.label.append_value("order_lines_updated");
        self.value
            .append_value(stats.num_order_lines_updated as i64);

        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            STATS_SCHEMA.clone(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.label.finish()),
                Arc::new(self.value.finish()),
            ],
        )?)
    }
}
