use std::collections::HashMap;
use std::time::Duration;

use arrow_array::{RecordBatch, cast::AsArray};
use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use h3o::{LatLng, Resolution};
use rand::Rng;
use tokio::runtime::Runtime;
use uuid::Uuid;

use super::schemas::{OrderData, OrderDataBuilder};
use crate::error::Result;
use crate::idents::*;
use crate::init::PopulationDataBuilder;
use crate::models::{Brand, Site};

mod objects;
mod population;

pub(crate) use objects::{ObjectData, ObjectLabel};
pub(crate) use population::{Person, PopulationData};

#[derive(Debug, thiserror::Error)]
enum StateError {
    // object not found
    #[error("Object not found")]
    ObjectNotFound,

    // inconsistent data
    #[error("Inconsistent data")]
    InconsistentData,
}

// TODO:
//   - order data by labels and track slices for fast lookups.

pub struct State {
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
}

impl State {
    pub(crate) fn try_new(
        brands: impl IntoIterator<Item = (BrandId, Brand)>,
        sites: Vec<(SiteId, Site)>,
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

        Ok(State {
            ctx: SessionContext::new(),
            rt,
            time_step: Duration::from_secs(60),
            time: Utc::now(),
            population: builder.finish()?,
            objects: ObjectData::try_new(vendors)?,
        })
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub fn people(&self) -> &RecordBatch {
        &self.population.people()
    }

    pub fn objects(&self) -> &RecordBatch {
        &self.objects.objects()
    }

    pub fn object_data(&self) -> &ObjectData {
        &self.objects
    }

    pub(crate) fn orders_for_site_batch(&self, site_id: &SiteId) -> Result<OrderData> {
        let site = self.objects.site(site_id)?;
        let props = site.properties()?;
        let lat_lng = LatLng::new(props.latitude, props.longitude)?;

        self.population
            // NB: resolution 6 corresponds to a cell size of approximately 36 km2
            .people_in_cell(lat_lng.to_cell(Resolution::Six))
            .filter_map(|person| person.create_order(self).map(|items| (person, items)))
            .fold(OrderDataBuilder::new(), |builder, (person, items)| {
                builder.add_order(&person, &items)
            })
            .finish()
    }

    pub fn time_step(&self) -> Duration {
        self.time_step
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn next_time(&self) -> DateTime<Utc> {
        self.time + self.time_step
    }

    pub fn step(&mut self) {
        self.time += self.time_step;
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
