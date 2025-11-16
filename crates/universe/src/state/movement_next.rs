use arrow::{
    array::{AsArray, RecordBatch},
    datatypes::{Float64Type, Int64Type},
};
use arrow_schema::DataType;
use datafusion::{
    common::{HashMap, JoinType},
    functions::core::expr_ext::FieldAccessor as _,
    functions_window::expr_fn::row_number,
    prelude::{Expr, array_element, cast, col, lit},
};
use fast_paths::{FastGraph, InputGraph};
use futures::StreamExt as _;
use h3o::CellIndex;

use crate::{Result, SimulationContext, functions::h3_longlatash3, test_utils::print_frame};

pub struct MovementHandler {
    graphs: HashMap<CellIndex, FastGraph>,
}

impl MovementHandler {
    pub(crate) async fn try_new(
        ctx: &SimulationContext,
        site_cells: Vec<i64>,
        resolution: Expr,
    ) -> Result<Self> {
        let mut graphs = HashMap::new();

        for cell_id in site_cells {
            let mut graph = InputGraph::new();
            let cell_index = CellIndex::try_from(cell_id as u64)?;

            let node_indexes = ctx
                .system()
                .routing_nodes()
                .await?
                .select([
                    col("properties").field("osmid").alias("osmid"),
                    h3_longlatash3()
                        .call(vec![
                            col("geometry").field("x"),
                            col("geometry").field("y"),
                            resolution.clone(),
                        ])
                        .alias("routing_cell"),
                ])?
                .filter(col("routing_cell").eq(lit(cell_id)))?
                .sort(vec![col("osmid").sort(true, false)])?
                .select([
                    col("osmid"),
                    cast(row_number() - lit(1_i64), DataType::Int64).alias("node_id"),
                ])?
                .cache()
                .await?;

            let edges = ctx
                .system()
                .routing_edges()
                .await?
                .select([
                    col("properties").field("osmid_source").alias("source"),
                    col("properties").field("osmid_target").alias("target"),
                    col("properties").field("length").alias("length"),
                ])?
                .join_on(
                    node_indexes.clone(),
                    JoinType::Right,
                    [col("source").eq(col("osmid"))],
                )?
                .select([col("node_id").alias("source"), col("target"), col("length")])?
                .join_on(
                    node_indexes.clone(),
                    JoinType::Right,
                    [col("target").eq(col("osmid"))],
                )?
                .select([col("source"), col("node_id").alias("target"), col("length")])?;

            let mut edge_stream = edges.execute_stream().await?;
            while let Some(batch) = edge_stream.next().await {
                process_batch(&mut graph, batch?)?;
            }

            graph.freeze();
            graphs.insert(cell_index, fast_paths::prepare(&graph));
        }

        Ok(Self { graphs })
    }
}

fn process_batch(graph: &mut InputGraph, batch: RecordBatch) -> Result<()> {
    let edge_iter = batch
        .column(0)
        .as_primitive::<Int64Type>()
        .iter()
        .zip(batch.column(1).as_primitive::<Int64Type>().iter())
        .zip(batch.column(2).as_primitive::<Float64Type>().iter());
    for ((source, target), length) in edge_iter {
        match (source, target, length) {
            (Some(source), Some(target), Some(length)) => {
                graph.add_edge(
                    source as usize,
                    target as usize,
                    length.round().abs() as usize,
                );
            }
            _ => {
                println!(
                    "Skipping edge with missing data: source={:?}, target={:?}, length={:?}",
                    source, target, length
                );
            }
        }
    }
    Ok(())
}
