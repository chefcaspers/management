use std::convert::AsRef;
use std::sync::Arc;

use arrow::array::{Array, LargeStringArray};
use arrow::array::{RecordBatch, cast::AsArray as _};
use arrow::datatypes::{Field, Schema};
use arrow_schema::DataType;
use chrono::{DateTime, Utc};
use geo_traits::PointTrait as _;
use geo_traits::to_geo::ToGeoCoord;
use geoarrow::array::{PointArray, PointBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor as _, IntoArrow};
use geoarrow_schema::{Dimension, PointType};
use h3o::{CellIndex, LatLng, Resolution};
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::idents::{OrderId, PersonId};
use crate::{EventPayload, OrderData, OrderStatus};

use super::movement::{Journey, Transport};

pub use builder::PopulationDataBuilder;

mod builder;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum PersonStatus {
    #[default]
    Idle,
    AwaitingOrder(OrderId),
    Eating(DateTime<Utc>),
    Moving(Journey),
    Delivering(OrderId, Journey),
    WaitingForCustomer(OrderId, Journey),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PersonState {
    status: PersonStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PersonRole {
    Customer,
    Courier,
}

/// Population data.
///
/// Holds information for all people in the simulation as well as
/// tracking their current locations and roles.
pub struct PopulationData {
    /// Metadata for individuals tracked in the simulation
    people: RecordBatch,

    /// Current geo locations of people
    positions: PointArray,

    /// Lookup index for people.
    ///
    /// An [`IndexMap`] tracks the insertion order of [`PersonId`]s.
    /// as such we can use the "position" of a person in the [`IndexMap`] to
    /// efficiently lookup their [`Person`] data as it corresponds to
    /// the index value within the [`people`] array.
    lookup_index: IndexMap<PersonId, PersonState>,
}

impl PopulationData {
    pub(crate) fn try_new(people: RecordBatch, positions: PointArray) -> Result<Self> {
        if people.num_rows() != positions.len() {
            return Err(Error::internal(
                "people and positions data must have the same length",
            ));
        }
        let lookup_index = lookup_index(&people)?;
        Ok(PopulationData {
            people,
            positions,
            lookup_index,
        })
    }

    pub fn people(&self) -> &RecordBatch {
        &self.people
    }

    pub fn snapshot(&self) -> RecordBatch {
        let people = self.people.clone();
        let positions = self.positions.clone().into_arrow();
        let states: Arc<dyn Array> = Arc::new(LargeStringArray::from(
            self.lookup_index
                .values()
                .map(|v| serde_json::to_string(v).unwrap())
                .collect_vec(),
        ));
        let full_schema = people
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(vec![
                Arc::new(Field::new("position", positions.data_type().clone(), false)),
                Arc::new(Field::new("state", DataType::LargeUtf8, false)),
            ])
            .collect_vec();
        let mut columns = people.columns().iter().cloned().collect_vec();
        columns.push(positions);
        columns.push(states);
        RecordBatch::try_new(Arc::new(Schema::new(full_schema)), columns).unwrap()
    }

    pub fn person(&self, id: &PersonId) -> Option<PersonView<'_>> {
        self.lookup_index
            .get_full(id)
            .map(|(idx, person_id, _)| PersonView::new(person_id, self, idx))
    }

    pub(crate) fn idle_people_in_cell(
        &self,
        cell_index: CellIndex,
        role: &PersonRole,
    ) -> impl Iterator<Item = PersonView<'_>> {
        self.iter().filter_map(move |person| {
            (person.is_idle()
                && person.has_role(role)
                && person.cell(cell_index.resolution()).ok()? == cell_index)
                .then_some(person)
        })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = PersonView<'_>> {
        self.lookup_index
            .iter()
            .enumerate()
            .map(|(valid_index, (id, _))| PersonView::new(id, self, valid_index))
    }

    pub fn update_person_status(&mut self, id: &PersonId, status: &PersonStatus) -> Result<()> {
        self.lookup_index.get_mut(id).ok_or(Error::NotFound)?.status = status.clone();
        Ok(())
    }

    pub(super) fn update_journeys(
        &mut self,
        current_time: &DateTime<Utc>,
        time_step: std::time::Duration,
        order_data: &OrderData,
    ) -> Result<Vec<EventPayload>> {
        let mut new_positions =
            PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        let mut events = Vec::new();

        for (idx, (person_id, state)) in self.lookup_index.iter_mut().enumerate() {
            let (progress, next_status) = match &mut state.status {
                PersonStatus::Moving(journey) => {
                    let progress = journey.advance(&Transport::Bicycle, time_step);
                    let next_status = journey.is_done().then_some(PersonStatus::Idle);
                    (Some(progress), next_status)
                }
                PersonStatus::Delivering(order_id, journey) => {
                    let progress = journey.advance(&Transport::Bicycle, time_step);
                    let next_status = journey.is_done().then_some({
                        // couriers need to reverse their journey when they're done delivering
                        let mut journey = journey.clone();
                        journey.reset_reverse();
                        PersonStatus::WaitingForCustomer(*order_id, journey)
                    });
                    (Some(progress), next_status)
                }
                PersonStatus::WaitingForCustomer(order_id, journey) => {
                    if let Some(order) = order_data.order(order_id) {
                        events.push(EventPayload::order_updated(
                            *order_id,
                            OrderStatus::Delivered,
                            None,
                        ));
                        events.push(EventPayload::person_updated(
                            order.customer_person_id().try_into()?,
                            PersonStatus::Eating(
                                *current_time + chrono::Duration::seconds(30 * 60),
                            ),
                        ));
                    };
                    (None, Some(PersonStatus::Moving(journey.clone())))
                }
                _ => (None, None),
            };

            if let Some(next_status) = next_status {
                events.push(EventPayload::person_updated(*person_id, next_status))
            }

            match progress {
                Some(slice) => {
                    if let Some(last_pos) = slice.last() {
                        new_positions.push_point(Some(last_pos));
                    } else {
                        new_positions.push_point(Some(&self.positions.value(idx)?));
                    }
                }
                None => new_positions.push_point(Some(&self.positions.value(idx)?)),
            }
        }

        self.positions = new_positions.finish();

        Ok(events)
    }
}

pub struct PersonView<'a> {
    id: &'a PersonId,
    data: &'a PopulationData,
    valid_index: usize,
}

impl<'a> PersonView<'a> {
    fn new(id: &'a PersonId, data: &'a PopulationData, valid_index: usize) -> Self {
        PersonView {
            id,
            data,
            valid_index,
        }
    }

    pub fn id(&self) -> &PersonId {
        self.id
    }

    pub fn position(&self) -> Result<geoarrow_array::scalar::Point<'a>> {
        Ok(self.data.positions.value(self.valid_index)?)
    }

    pub fn has_role(&self, role: &PersonRole) -> bool {
        self.data
            .people
            .column(5)
            .as_string_view()
            .value(self.valid_index)
            == role.as_ref()
    }

    pub fn cell(&self, resolution: Resolution) -> Result<CellIndex> {
        let coords = self.position()?.coord().unwrap();
        let lat_lng: LatLng = coords.to_coord().try_into()?;
        Ok(lat_lng.to_cell(resolution))
    }

    pub fn first_name(&self) -> &str {
        self.data
            .people
            .column(1)
            .as_string::<i32>()
            .value(self.valid_index)
    }

    pub fn last_name(&self) -> &str {
        self.data
            .people
            .column(2)
            .as_string::<i32>()
            .value(self.valid_index)
    }

    pub fn full_name(&self) -> String {
        format!("{} {}", self.first_name(), self.last_name())
    }

    pub fn email(&self) -> &str {
        self.data
            .people
            .column(3)
            .as_string::<i32>()
            .value(self.valid_index)
    }

    pub fn cc_number(&self) -> &str {
        self.data
            .people
            .column(4)
            .as_string::<i32>()
            .value(self.valid_index)
    }

    pub fn state(&self) -> &PersonState {
        self.data
            .lookup_index
            .get(self.id())
            .expect("Person state should be initialized for all people")
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.state().status, PersonStatus::Idle)
    }
}

impl std::fmt::Debug for PersonView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Person")
            .field("position", &self.position())
            .field("first_name", &self.first_name())
            .field("last_name", &self.last_name())
            .field("email", &self.email())
            .field("cc_number", &self.cc_number())
            .finish()
    }
}

fn lookup_index(batch: &RecordBatch) -> Result<IndexMap<PersonId, PersonState>> {
    Ok(batch
        .column(0)
        .as_fixed_size_binary()
        .iter()
        .filter_map(|data| {
            data.and_then(|data| Some((PersonId(Uuid::from_slice(data).ok()?), Default::default())))
        })
        .collect())
}
