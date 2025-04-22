use arrow_array::{RecordBatch, cast::AsArray};
use geo::Centroid;
use geo::{Geometry, Point};
use geoarrow::array::{PointArray, PolygonArray};
use geoarrow::trait_::{ArrayAccessor, NativeScalar};
use geoarrow::{ArrayBase, array::PointBuilder, scalar::Point as ArrowPoint};
use geoarrow_schema::Dimension;
use indexmap::IndexSet;
use itertools::Itertools;
use uuid::Uuid;

use crate::SiteRunner;
use crate::error::Result;
use crate::idents::PersonId;
use crate::simulation::Entity;

// A specific place or areas
pub trait Location: Entity {
    fn location(&self) -> &Geometry;

    fn centroid(&self) -> Point {
        self.location().centroid().unwrap()
    }
}

impl Location for SiteRunner {
    fn location(&self) -> &Geometry {
        todo!()
    }
}

pub trait Movable: Entity {
    fn position(&self) -> ArrowPoint;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Transport {
    Foot,
    Bicycle,
    Car,
    Bus,
    Train,
    Plane,
    Ship,
}

impl Transport {
    /// Returns the default velocity of the transport in km/h.
    fn default_velocity_km_h(&self) -> f64 {
        match self {
            Transport::Foot => 5.0,
            Transport::Bicycle => 15.0,
            Transport::Car => 60.0,
            Transport::Bus => 30.0,
            Transport::Train => 100.0,
            Transport::Plane => 800.0,
            Transport::Ship => 20.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JourneyLeg {
    destination: Point,
    transport: Transport,
}

impl<T: Into<Point>> From<(Transport, T)> for JourneyLeg {
    fn from(value: (Transport, T)) -> Self {
        JourneyLeg {
            destination: value.1.into(),
            transport: value.0,
        }
    }
}

impl<T: Into<Point>> From<(T, Transport)> for JourneyLeg {
    fn from(value: (T, Transport)) -> Self {
        JourneyLeg {
            destination: value.0.into(),
            transport: value.1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Journey {
    legs: Vec<JourneyLeg>,
}

impl Journey {
    fn add_leg(&mut self, leg: JourneyLeg) {
        self.legs.push(leg);
    }
}

impl<T: Into<JourneyLeg>> FromIterator<T> for Journey {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Journey {
            legs: iter.into_iter().map(Into::into).collect(),
        }
    }
}

pub struct PopulationData {
    people: RecordBatch,
    homes: Option<PolygonArray>,
    positions: PointArray,
    lookup_index: IndexSet<PersonId>,
}

impl PopulationData {
    pub fn person(&self, id: &PersonId) -> Option<Person<'_>> {
        let idx = self.lookup_index.get_index_of(id)?;
        self.lookup_index
            .get(id)
            .map(|person_id| Person::new(person_id, self, idx))
    }

    pub fn iter(&self) -> impl Iterator<Item = Person<'_>> {
        self.lookup_index
            .iter()
            .enumerate()
            .map(|(valid_index, id)| Person::new(id, self, valid_index))
    }
}

pub struct Person<'a> {
    id: &'a PersonId,
    data: &'a PopulationData,
    valid_index: usize,
}

impl<'a> Person<'a> {
    fn new(id: &'a PersonId, data: &'a PopulationData, valid_index: usize) -> Self {
        Person {
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
}

impl std::fmt::Debug for Person<'_> {
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

// for small local distances, an euclidianian distance of 0.0009 corresponds to ~1km

impl PopulationData {
    fn try_new(people: RecordBatch, positions: PointArray) -> Result<Self> {
        if people.num_rows() != positions.len() {
            return Err("people and positions data must have the same length".into());
        }
        let lookup_index = lookup_index(&people)?;
        Ok(PopulationData {
            people,
            homes: None,
            positions,
            lookup_index,
        })
    }

    pub fn from_site(
        (minx, miny): (f64, f64),
        (maxx, maxy): (f64, f64),
        n_people: usize,
    ) -> Result<Self> {
        let (people, positions) =
            crate::init::generate_population((minx, miny), (maxx, maxy), n_people)?;
        Self::try_new(people, positions)
    }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> Self {
        let people = self.people.slice(offset, length);
        // let homes = self.homes.slice(offset, length);
        let positions = self.positions.slice(offset, length);
        // safety: the data is already validated in the constructor
        Self::try_new(people, positions).unwrap()
    }

    // fn home_at(&self, index: usize) -> Option<ArrowPolygon> {
    //     if index >= self.homes.len() {
    //         None
    //     } else {
    //         self.homes.get(index)
    //     }
    // }

    fn position_at(&self, index: usize) -> Option<ArrowPoint> {
        if index >= self.positions.len() {
            None
        } else {
            self.positions.get(index)
        }
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
}

fn lookup_index(batch: &RecordBatch) -> Result<IndexSet<PersonId>> {
    Ok(batch
        .column(0)
        .as_fixed_size_binary()
        .iter()
        .filter_map(|data| data.map(|data| PersonId(Uuid::from_slice(data).unwrap())))
        .collect())
}
