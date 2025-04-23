use geo::{Centroid, Geometry, Point};
use geoarrow::scalar::Point as ArrowPoint;
use serde::{Deserialize, Serialize};

use crate::simulation::Entity;

// A specific place or areas
pub trait Location: Entity {
    fn location(&self) -> &Geometry;

    fn centroid(&self) -> Point {
        self.location().centroid().unwrap()
    }
}

pub trait Movable: Entity {
    fn position(&self) -> ArrowPoint;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Journey {
    legs: Vec<JourneyLeg>,
}

impl<T: Into<JourneyLeg>> FromIterator<T> for Journey {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Journey {
            legs: iter.into_iter().map(Into::into).collect(),
        }
    }
}
