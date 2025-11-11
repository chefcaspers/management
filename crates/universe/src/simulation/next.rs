use std::sync::{Arc, LazyLock};

use arrow::{array::RecordBatch, compute::concat_batches};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit, extension::Uuid};
use datafusion::{
    functions_aggregate::count::count_all,
    logical_expr::ScalarUDF,
    prelude::{DataFrame, cast, col, lit},
    scalar::ScalarValue,
};

use crate::{Error, EventsHelper, ObjectLabel, OrderStatus, Result, SimulationContext};
use crate::{
    agents::{KitchenHandler, PopulationHandler, functions::create_order},
    test_utils::print_frame,
};

pub struct SimulationRunnerBuilder {
    ctx: SimulationContext,

    create_orders: Option<Box<dyn Fn(RecordBatch) -> Arc<ScalarUDF>>>,
}

impl From<SimulationContext> for SimulationRunnerBuilder {
    fn from(ctx: SimulationContext) -> Self {
        Self {
            ctx,
            create_orders: None,
        }
    }
}

impl SimulationRunnerBuilder {
    pub fn new(ctx: SimulationContext) -> Self {
        ctx.into()
    }

    pub fn with_create_orders(
        mut self,
        create_orders: Box<dyn Fn(RecordBatch) -> Arc<ScalarUDF>>,
    ) -> Self {
        self.create_orders = Some(create_orders);
        self
    }

    pub async fn build(self) -> Result<SimulationRunner> {
        let batches = self
            .ctx
            .snapshots()
            .objects()
            .await?
            .filter(col("label").eq(lit(ObjectLabel::MenuItem.as_ref())))?
            .select([
                col("parent_id").alias("brand_id"),
                col("id").alias("menu_item_id"),
            ])?
            .collect()
            .await?;
        let order_choices = concat_batches(batches[0].schema_ref(), &batches)?;

        let create_orders = if let Some(create) = self.create_orders {
            create(order_choices)
        } else {
            create_order(order_choices)
        };

        let population = PopulationHandler::try_new(&self.ctx, create_orders).await?;
        let kitchens = KitchenHandler::try_new(&self.ctx).await?;

        // TODO: load orders from snapshot
        let orders = self
            .ctx
            .ctx()
            .read_batch(RecordBatch::new_empty(ORDER_STATE.clone()))?
            .collect()
            .await?;

        Ok(SimulationRunner {
            ctx: self.ctx,
            population,
            kitchens,
            orders,
        })
    }
}

static ORDER_STATE: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("person_id", DataType::FixedSizeBinary(16), false).with_extension_type(Uuid),
        Field::new("order_id", DataType::FixedSizeBinary(16), true),
        Field::new(
            "submitted_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "destination",
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        ),
        Field::new("status", DataType::Utf8, false),
        Field::new_list(
            "items",
            Field::new_list_field(
                DataType::FixedSizeList(
                    Field::new_list_field(DataType::FixedSizeBinary(16), false).into(),
                    2,
                ),
                false,
            ),
            true,
        ),
        Field::new_list(
            "order_lines",
            Field::new_list_field(DataType::FixedSizeBinary(16), false),
            true,
        ),
    ]))
});

pub struct SimulationRunner {
    pub(crate) ctx: SimulationContext,

    pub(crate) population: PopulationHandler,
    pub(crate) kitchens: KitchenHandler,

    orders: Vec<RecordBatch>,
}

impl SimulationRunner {
    pub async fn run(&mut self, steps: usize) -> Result<()> {
        tracing::info!(
            target: "caspers::simulation",
            "statrting simulation run for {} steps ({} / {})",
            steps,
            self.ctx.simulation_id(),
            self.ctx.snapshot_id()
        );

        for _ in 0..steps {
            self.step().await?;
        }

        Ok(())
    }

    pub async fn step(&mut self) -> Result<()> {
        let orders = self
            .population
            .create_orders(&self.ctx)
            .await?
            .select([
                col("person_id"),
                col("order_id"),
                col("submitted_at"),
                col("destination"),
                lit(OrderStatus::Submitted.as_ref()).alias("status"),
                col("items"),
                cast(
                    lit(ScalarValue::Null),
                    DataType::List(
                        Field::new_list_field(DataType::FixedSizeBinary(16), false).into(),
                    ),
                )
                .alias("order_lines"),
            ])?
            .cache()
            .await?;

        let mut events = EventsHelper::empty(&self.ctx)?;

        let orders_count = orders.clone().count().await?;
        let kitchen_events = if orders_count > 0 {
            let curr_orders = self.ctx.ctx().read_batches(self.orders.iter().cloned())?;
            self.orders = orders.clone().union(curr_orders)?.collect().await?;
            events = events.union(EventsHelper::orders_created(orders.clone())?)?;

            self.kitchens.step(&self.ctx, Some(orders)).await?
        } else {
            self.kitchens.step(&self.ctx, None).await?
        };
        events = events.union(kitchen_events)?;

        self.ctx.step_time();
        self.send_events(events).await?;

        Ok(())
    }

    async fn send_events(&self, events: DataFrame) -> Result<()> {
        let aggreagte = events.aggregate(vec![col("type")], vec![count_all()])?;
        let type_count = aggreagte.clone().count().await?;
        if type_count > 0 {
            print_frame(&aggreagte).await?;
            // print_frame(&events).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn ctx(&self) -> &SimulationContext {
        &self.ctx
    }

    #[cfg(test)]
    pub(crate) fn population(&self) -> &PopulationHandler {
        &self.population
    }

    #[cfg(test)]
    pub(crate) fn advance_time(&mut self) {
        self.ctx.step_time();
    }
}

#[cfg(test)]
mod tests {
    use geo::Point;
    use rstest::*;

    use super::*;
    use crate::{
        Journey, PersonId,
        test_utils::{print_frame, runner},
    };

    #[rstest]
    #[tokio::test]
    async fn test_simulation_step(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let mut runner = runner.await?;

        runner.run(100).await?;

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_simulation_runner(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let mut runner = runner.await?;

        let orders = runner.population.create_orders(&runner.ctx).await?;

        print_frame(&orders).await?;

        let person_id = PersonId::new();
        let start_position = Point::new(0.0, 0.0);
        let journey: Journey = vec![
            (Point::new(1.0, 1.0), 10000_usize),
            (Point::new(2.2, 2.0), 200_usize),
        ]
        .into_iter()
        .collect();

        runner
            .population
            .start_journeys(&runner.ctx, vec![(person_id, start_position, journey)])
            .await?;

        print_frame(&runner.population.journeys(&runner.ctx)?).await?;

        runner.population.advance_journeys(&runner.ctx).await?;

        print_frame(&runner.population.journeys(&runner.ctx)?).await?;

        runner.population.advance_journeys(&runner.ctx).await?;

        print_frame(&runner.population.journeys(&runner.ctx)?).await?;

        Ok(())
    }
}
