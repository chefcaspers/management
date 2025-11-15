use std::sync::Arc;

use arrow::{array::RecordBatch, compute::concat_batches, util::pretty::print_batches};
use datafusion::{
    functions_aggregate::count::count_all,
    logical_expr::ScalarUDF,
    prelude::{DataFrame, col, lit},
};

use crate::agents::{KitchenHandler, PopulationHandler, functions::create_order};
use crate::{EventsHelper, ObjectLabel, Result, SimulationContext};

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

        Ok(SimulationRunner {
            ctx: self.ctx,
            population,
            kitchens,
        })
    }
}

pub struct SimulationRunner {
    pub(crate) ctx: SimulationContext,

    pub(crate) population: PopulationHandler,
    pub(crate) kitchens: KitchenHandler,
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
            .cache()
            .await?;

        let mut events = EventsHelper::empty(&self.ctx)?;

        let orders_count = orders.clone().count().await?;
        let kitchen_events = if orders_count > 0 {
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
            let agg_batches = aggreagte.collect().await?;
            print_batches(&agg_batches)?;
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
    use arrow::util::pretty::print_batches;
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

        print_batches(&runner.kitchens.orders)?;
        print_batches(&runner.kitchens.order_lines)?;
        // print_batches(&runner.orders)?;

        runner.run(100).await?;

        // print_batches(&runner.population.population)?;

        // print_frame(&runner.kitchens.stations(&runner.ctx)?).await?;
        // print_frame(&runner.kitchens.order_lines(&runner.ctx)?).await?;
        // print_batches(&runner.orders)?;

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
