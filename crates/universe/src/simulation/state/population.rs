use std::convert::AsRef;

use arrow_array::{RecordBatch, cast::AsArray};
use chrono::{DateTime, Utc};
use geo::Point;
use geo_traits::PointTrait;
use geoarrow::array::PointArray;
use geoarrow::trait_::{ArrayAccessor, NativeScalar};
use geoarrow::{ArrayBase, array::PointBuilder, scalar::Point as ArrowPoint};
use geoarrow_schema::Dimension;
use h3o::{CellIndex, LatLng, Resolution};
use indexmap::IndexMap;
use itertools::Itertools;
use rand::Rng;
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use uuid::Uuid;

use crate::error::Result;
use crate::idents::{BrandId, MenuItemId, OrderId, PersonId};
use crate::simulation::state::EntityView;

use super::State;
use super::movement::Journey;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PersonStatus {
    Idle,
    AwaitingOrder(OrderId),
    Eating(DateTime<Utc>),
    Moving(Journey),
}

impl Default for PersonStatus {
    fn default() -> Self {
        PersonStatus::Idle
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PersonState {
    status: PersonStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AsRefStr)]
pub enum PersonRole {
    Customer,
    Courier,
}

/// Population data.
///
/// Holds information for all people in the simulation.
pub struct PopulationData {
    people: RecordBatch,

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
            return Err("people and positions data must have the same length".into());
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

    fn apply_offsets(
        &mut self,
        offsets: impl IntoIterator<Item = Option<(f64, f64)>>,
    ) -> Result<()> {
        let offsets = offsets.into_iter().collect_vec();
        if offsets.len() != self.positions.len() {
            return Err("Population data must have the same length".into());
        }
        let mut builder = PointBuilder::with_capacity(Dimension::XY, self.positions.len());
        for (curr, maybe_offset) in self.positions.iter().zip(offsets.iter()) {
            match (maybe_offset, curr) {
                (Some(offset), Some(point)) => {
                    let curr_pos = point.to_geo().x_y();
                    builder.push_point(Some(&Point::new(
                        curr_pos.0 + offset.0,
                        curr_pos.1 + offset.1,
                    )));
                }
                (None, curr) => {
                    builder.push_point(curr.as_ref());
                }
                (Some(_), None) => return Err("Offset provided for a missing position".into()),
            }
        }

        self.positions = builder.finish();
        Ok(())
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
                .then(|| person)
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = PersonView<'_>> {
        self.lookup_index
            .iter()
            .enumerate()
            .map(|(valid_index, (id, _))| PersonView::new(id, self, valid_index))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = PersonView<'_>> {
        self.lookup_index
            .iter()
            .enumerate()
            .map(|(valid_index, (id, _))| PersonView::new(id, self, valid_index))
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

    pub fn position(&self) -> ArrowPoint {
        self.data.positions.value(self.valid_index)
    }

    pub fn has_role(&self, role: &PersonRole) -> bool {
        self.data
            .people
            .column(5)
            .as_string::<i32>()
            .value(self.valid_index)
            == role.as_ref()
    }

    pub fn cell(&self, resolution: Resolution) -> Result<CellIndex> {
        let coords = self.position().coord().unwrap();
        let lat_lng: LatLng = coords.to_geo().try_into()?;
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

    pub(crate) fn create_order(&self, state: &State) -> Option<Vec<(BrandId, MenuItemId)>> {
        let mut rng = rand::rng();

        // Do not create an order if the person is not idle
        if !self.is_idle() {
            return None;
        }

        // TODO: compute probability from person state
        rng.random_bool(1.0 / 50.0).then(|| {
            state
                .object_data()
                .sample_menu_items(None, &mut rng)
                .into_iter()
                .map(|menu_item| (menu_item.brand_id().try_into().unwrap(), menu_item.id()))
                .collect()
        })
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
            data.map(|data| {
                (
                    PersonId(Uuid::from_slice(data).unwrap()),
                    Default::default(),
                )
            })
        })
        .collect())
}
