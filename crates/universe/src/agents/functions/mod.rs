use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::logical_expr::ScalarUDF;

mod create_order;

pub(crate) fn create_order(choices: RecordBatch) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(create_order::CreateOrder::new(
        choices,
    )))
}
