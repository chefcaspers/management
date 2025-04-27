use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fake::Fake;
use geo::Centroid;
use geo::{BoundingRect, LineString, Point, Polygon};
use geoarrow::array::PointBuilder;
use geoarrow_schema::Dimension;
use h3o::{LatLng, Resolution};
use rand::distr::{Distribution, Uniform};
use rand::rngs::ThreadRng;

use super::{PersonRole, PopulationData};
use crate::error::Result;
use crate::idents::PersonId;
use crate::models::Site;

static POPULATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("cc_number", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, false),
    ]))
});

pub struct PopulationDataBuilder {
    ids: FixedSizeBinaryBuilder,
    first_names: StringBuilder,
    last_names: StringBuilder,
    emails: StringBuilder,
    cc_numbers: StringBuilder,
    roles: StringBuilder,
    positions: PointBuilder,

    rng: ThreadRng,
}

impl PopulationDataBuilder {
    pub fn new() -> Self {
        Self {
            ids: FixedSizeBinaryBuilder::new(16),
            first_names: StringBuilder::new(),
            last_names: StringBuilder::new(),
            emails: StringBuilder::new(),
            cc_numbers: StringBuilder::new(),
            roles: StringBuilder::new(),
            positions: PointBuilder::new(Dimension::XY),
            rng: rand::rng(),
        }
    }

    pub fn add_site(&mut self, site: &Site, n_people: usize) -> Result<()> {
        let gen_first_name = fake::faker::name::en::FirstName();
        let gen_last_name = fake::faker::name::en::LastName();
        let gen_email = fake::faker::internet::en::SafeEmail();
        let gen_cc = fake::faker::creditcard::en::CreditCardNumber();

        tracing::info!("Adding {} people", n_people);
        for _ in 0..n_people {
            let id = PersonId::new();
            self.ids.append_value(id)?;
            self.first_names
                .append_value(gen_first_name.fake_with_rng::<String, _>(&mut self.rng));
            self.last_names
                .append_value(gen_last_name.fake_with_rng::<String, _>(&mut self.rng));
            self.emails
                .append_value(gen_email.fake_with_rng::<String, _>(&mut self.rng));
            self.cc_numbers
                .append_value(gen_cc.fake_with_rng::<String, _>(&mut self.rng));
            self.roles.append_value(PersonRole::Customer.as_ref());
        }

        let latlng = LatLng::new(site.latitude, site.longitude)?;
        let cell_index = latlng.to_cell(Resolution::Six);
        let boundary: LineString = cell_index.boundary().iter().cloned().collect();
        let polygon = Polygon::new(boundary, Vec::new());

        let bounding_rect = polygon.bounding_rect().unwrap();
        let (maxx, maxy) = bounding_rect.max().x_y();
        let (minx, miny) = bounding_rect.min().x_y();

        let x_range = Uniform::new(minx, maxx)?;
        let y_range = Uniform::new(miny, maxy)?;
        x_range
            .sample_iter(rand::rng())
            .take(n_people)
            .zip(y_range.sample_iter(rand::rng()).take(n_people))
            .for_each(|(x, y)| {
                self.positions.push_point(Some(&Point::new(x, y)));
            });

        let n_couriers = n_people / 10;
        tracing::info!("Adding {} couriers", n_couriers);
        let loc = polygon.centroid().unwrap();
        for _ in 0..n_couriers {
            let id = PersonId::new();
            self.ids.append_value(id)?;
            self.first_names
                .append_value(gen_first_name.fake_with_rng::<String, _>(&mut self.rng));
            self.last_names
                .append_value(gen_last_name.fake_with_rng::<String, _>(&mut self.rng));
            self.emails
                .append_value(gen_email.fake_with_rng::<String, _>(&mut self.rng));
            self.cc_numbers
                .append_value(gen_cc.fake_with_rng::<String, _>(&mut self.rng));
            self.roles.append_value(PersonRole::Courier.as_ref());
            self.positions.push_point(Some(&loc));
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<PopulationData> {
        let ids = Arc::new(self.ids.finish());
        let first_names = Arc::new(self.first_names.finish());
        let last_names = Arc::new(self.last_names.finish());
        let emails = Arc::new(self.emails.finish());
        let cc_numbers = Arc::new(self.cc_numbers.finish());
        let roles = Arc::new(self.roles.finish());

        let people = RecordBatch::try_new(
            POPULATION_SCHEMA.clone(),
            vec![ids, first_names, last_names, emails, cc_numbers, roles],
        )?;

        PopulationData::try_new(people, self.positions.finish())
    }
}
