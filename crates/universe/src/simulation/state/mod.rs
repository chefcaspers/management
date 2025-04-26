use std::collections::HashMap;
use std::time::Duration;

use arrow_array::{RecordBatch, cast::AsArray};
use chrono::{DateTime, Utc};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use geo::Point;
use geo_traits::PointTrait;
use geoarrow::trait_::NativeScalar;
use h3o::{LatLng, Resolution};
use rand::Rng;
use tokio::runtime::Runtime;
use uuid::Uuid;

use self::movement::TripPlanner;
use super::schemas::{OrderData, OrderDataBuilder};
use super::{EventPayload, SimulationConfig};
use crate::error::Result;
use crate::idents::*;
use crate::init::PopulationDataBuilder;
use crate::models::{Brand, Site};

mod movement;
mod objects;
mod population;

pub(crate) use movement::RoutingData;
pub(crate) use objects::{ObjectData, ObjectLabel};
pub(crate) use population::{PersonRole, PersonStatus, PersonView, PopulationData};

#[derive(Debug, thiserror::Error)]
enum StateError {
    // inconsistent data
    #[error("Inconsistent data")]
    InconsistentData,
}

// TODO:
//   - order data by labels and track slices for fast lookups.

pub struct State {
    config: SimulationConfig,

    /// Datafusion session context
    ctx: SessionContext,

    /// Async runtime to handle datafusion tasks
    rt: Runtime,

    /// Current simulation time
    time: DateTime<Utc>,

    /// Time increment per simulation step
    time_step: Duration,

    /// Population data
    population: PopulationData,

    /// Vendor data
    objects: ObjectData,

    /// Routing data
    routing: TripPlanner,
}

impl State {
    pub(crate) fn try_new(
        brands: impl IntoIterator<Item = (BrandId, Brand)>,
        sites: Vec<(SiteId, Site)>,
        routing: RoutingData,
        config: Option<SimulationConfig>,
    ) -> Result<Self> {
        let mut builder = PopulationDataBuilder::new();
        for (_site_id, site) in &sites {
            let n_people = rand::rng().random_range(100..1000);
            builder.add_site(site, n_people)?;
        }

        let brands: HashMap<_, _> = brands.into_iter().collect();
        let vendors = crate::init::generate_objects(&brands, sites)?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let config = config.unwrap_or_default();
        Ok(State {
            ctx: SessionContext::new(),
            rt,
            time_step: Duration::from_secs(config.time_increment.num_seconds() as u64),
            time: config.simulation_start,
            population: builder.finish()?,
            objects: ObjectData::try_new(vendors)?,
            routing: routing.into_trip_planner(),
            config,
        })
    }

    pub(crate) fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub(crate) fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub(crate) fn config(&self) -> &SimulationConfig {
        &self.config
    }

    pub fn people(&self) -> &RecordBatch {
        &self.population.people()
    }

    pub fn object_data(&self) -> &ObjectData {
        &self.objects
    }

    pub fn population(&self) -> &PopulationData {
        &self.population
    }

    pub fn trip_planner(&self) -> &TripPlanner {
        &self.routing
    }

    pub(crate) fn orders_for_site(&self, site_id: &SiteId) -> Result<OrderData> {
        let site = self.objects.site(site_id)?;
        let props = site.properties()?;
        let lat_lng = LatLng::new(props.latitude, props.longitude)?;

        self.population
            // NB: resolution 6 corresponds to a cell size of approximately 36 km2
            .idle_people_in_cell(lat_lng.to_cell(Resolution::Six), &PersonRole::Customer)
            .filter_map(|person| person.create_order(self).map(|items| (person, items)))
            .fold(OrderDataBuilder::new(), |builder, (person, items)| {
                builder.add_order(
                    &person,
                    person
                        .position()
                        .coord()
                        .unwrap()
                        .to_geo()
                        .try_into()
                        .unwrap(),
                    &items,
                )
            })
            .finish()
    }

    pub fn time_step(&self) -> Duration {
        self.time_step
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    pub(crate) fn next_time(&self) -> DateTime<Utc> {
        self.time + self.time_step
    }

    pub(crate) fn move_people(&mut self) -> Result<Vec<(PersonId, Vec<Point>)>> {
        // update person positions first, so that journeys stated in this step are not advanced.
        let (movements, status_updates) = self.population.update_journeys(self.time_step)?;
        // update person statuses after positions have been updated.
        for (person_id, status) in status_updates.into_iter() {
            self.population.update_person_status(&person_id, status)?;
        }
        Ok(movements)
    }

    pub(crate) fn step(&mut self, events: impl IntoIterator<Item = EventPayload>) -> Result<()> {
        for event in events.into_iter() {
            match event {
                EventPayload::PersonUpdated(payload) => {
                    self.population
                        .update_person_status(&payload.person_id, payload.status)?;
                }
                _ => {}
            }
        }

        self.time += self.time_step;

        Ok(())
    }

    pub(crate) fn snapshot(&self, base_path: &url::Url) -> Result<()> {
        let people_path = base_path
            .join(&format!(
                "people/{}.parquet",
                self.current_time().timestamp()
            ))
            .unwrap();

        let objects_path = base_path
            .join(&format!(
                "objects/{}.parquet",
                self.current_time().timestamp()
            ))
            .unwrap();

        self.rt().block_on(async {
            let df = self.ctx().read_batch(self.population.people().clone())?;
            df.write_parquet(people_path.as_str(), DataFrameWriteOptions::new(), None)
                .await?;

            let df = self.ctx().read_batch(self.objects.objects().clone())?;
            df.write_parquet(objects_path.as_str(), DataFrameWriteOptions::new(), None)
                .await?;

            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }
}

pub trait EntityView {
    type Id: TypedId;
    type Properties: serde::de::DeserializeOwned;

    fn data(&self) -> &ObjectData;

    fn valid_index(&self) -> usize;

    fn id(&self) -> Self::Id {
        Uuid::from_slice(
            self.data()
                .objects()
                .column_by_name("id")
                .expect("object data schema should be validated")
                .as_fixed_size_binary()
                .value(self.valid_index()),
        )
        .unwrap()
        .into()
    }

    fn properties(&self) -> Result<Self::Properties> {
        let raw = self
            .data()
            .objects()
            .column_by_name("properties")
            .ok_or(StateError::InconsistentData)?
            .as_string::<i32>()
            .value(self.valid_index());
        Ok(serde_json::from_str(raw)?)
    }
}
