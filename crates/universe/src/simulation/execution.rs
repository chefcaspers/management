use std::{
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit, UnionFields, UnionMode};
use datafusion::common::DataFusionError;
use datafusion::execution::RecordBatchStream;
use futures::Stream;

// TODO(): The way we represent the actual message data / payload
// is a great use case for variant data ... once that comes along

static EVENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("spec_version", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "attributes",
            DataType::Union(
                UnionFields::new(
                    vec![1, 2, 3, 4, 5, 6, 7],
                    vec![
                        Field::new("boolean", DataType::Boolean, false),
                        Field::new("integer", DataType::Int32, false),
                        Field::new("string", DataType::Utf8, false),
                        Field::new("bytes", DataType::Binary, false),
                        Field::new("uri", DataType::Utf8, false),
                        Field::new("uri_ref", DataType::Utf8, false),
                    ],
                ),
                UnionMode::Dense,
            ),
            true,
        ),
        Field::new("data", DataType::Utf8, false),
    ]))
});

struct EventStream {
    events: Vec<String>,
}

impl Stream for EventStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.events.is_empty() {
            Poll::Ready(None)
        } else {
            let batch = RecordBatch::try_new(EVENT_SCHEMA.clone(), vec![])?;
            Poll::Ready(Some(Ok(batch)))
        }
    }
}

impl RecordBatchStream for EventStream {
    fn schema(&self) -> SchemaRef {
        EVENT_SCHEMA.clone()
    }
}
