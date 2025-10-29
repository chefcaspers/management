use std::sync::Arc;

use arrow::{array::AsArray as _, compute::concat_batches};
use datafusion::{
    logical_expr::ScalarUDF,
    prelude::{col, lit},
    scalar::ScalarValue,
};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::PointArray;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_schema::{Dimension, PointType};
use h3o::{LatLng, Resolution};
use tracing::{Level, instrument};
use uuid::Uuid;

use crate::{
    BrandId, EntityView as _, EventPayload, MenuItemId, ObjectLabel, OrderCreatedPayload,
    PersonRole, Result, SimulationContext, SiteId, State,
};

pub struct PopulationRunner {
    create_orders: Arc<ScalarUDF>,
}

impl PopulationRunner {
    pub async fn try_new(state: &State, ctx: &SimulationContext) -> Result<Self> {
        let batches = ctx
            .ctx()
            .read_batch(state.objects().objects().clone())?
            .filter(col("label").eq(lit(ObjectLabel::MenuItem.as_ref())))?
            .select_columns(&["parent_id", "id"])?
            .collect()
            .await?;
        let order_choices = concat_batches(batches[0].schema_ref(), &batches)?;
        let create_orders = super::functions::create_order(order_choices);
        Ok(PopulationRunner { create_orders })
    }

    #[instrument(
        name = "step_population",
        level = Level::TRACE,
        skip(self, ctx, state),
        fields(
            caspers.site_id = site_id.to_string()
        )
    )]
    pub(crate) async fn step(
        &self,
        ctx: &SimulationContext,
        site_id: &SiteId,
        state: &State,
    ) -> Result<impl Iterator<Item = EventPayload>> {
        let site = state.objects().site(site_id)?;
        let props = site.properties()?;
        let lat_lng = LatLng::new(props.latitude, props.longitude)?;
        let ts = state.current_time().timestamp_millis();

        let idle_people = state
            .population()
            .idle_people_in_cell(ctx, lat_lng.to_cell(Resolution::Six), &PersonRole::Customer)
            .await?
            .collect()
            .await?;

        let idle_people = ctx.ctx().read_batches(idle_people)?;

        let orders = idle_people
            .select(vec![
                col("id"),
                self.create_orders
                    .call(vec![
                        lit(ScalarValue::TimestampMillisecond(
                            Some(ts),
                            Some("UTC".into()),
                        )),
                        col("state"),
                    ])
                    .alias("order"),
                col("position"),
            ])?
            .filter(col("order").is_not_null())?
            .select_columns(&["id", "order", "position"])?
            .collect()
            .await?;

        let orders = orders.into_iter().flat_map(|o| {
            let positions: PointArray = (
                o.column(2).as_struct(),
                PointType::new(Dimension::XY, Default::default()),
            )
                .try_into()
                .unwrap();
            let orders_iter = o
                .column(0)
                .as_fixed_size_binary()
                .iter()
                .zip(o.column(1).as_list::<i32>().iter())
                .zip(positions.iter());
            let mut orders = Vec::new();

            for ((person_id, order), pos) in orders_iter {
                match (person_id, order, pos) {
                    (Some(person_id), Some(order), Some(Ok(pos))) => {
                        let items = order
                            .as_fixed_size_list()
                            .iter()
                            .flat_map(|it| {
                                it.map(|it2| {
                                    let arr = it2.as_fixed_size_binary();
                                    (
                                        BrandId::from(Uuid::from_slice(arr.value(0)).unwrap()),
                                        MenuItemId::from(Uuid::from_slice(arr.value(1)).unwrap()),
                                    )
                                })
                            })
                            .collect();
                        orders.push(EventPayload::OrderCreated(OrderCreatedPayload {
                            site_id: *site_id,
                            person_id: Uuid::from_slice(person_id).unwrap().into(),
                            items,
                            destination: pos.to_point(),
                        }));
                    }
                    _ => {}
                }
            }

            orders
        });

        Ok(orders)
    }
}
