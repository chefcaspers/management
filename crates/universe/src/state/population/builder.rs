use std::sync::{Arc, LazyLock};

use arrow::array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow::array::{RecordBatch, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use fake::Fake;
use geo::{BoundingRect, Centroid, Contains, Point};
use geoarrow::array::PointBuilder;
use geoarrow_array::IntoArrow;
use geoarrow_schema::{Dimension, PointType};
use h3o::{LatLng, Resolution, geom::SolventBuilder};
use itertools::Itertools as _;
use rand::distr::{Distribution, Uniform};
use rand::rngs::ThreadRng;

use super::PersonRole;
use crate::Error;
use crate::error::Result;
use crate::idents::PersonId;
use crate::state::population::PersonState;

static DEFAULT_STATE: LazyLock<String> =
    LazyLock::new(|| serde_json::to_string(&PersonState::default()).unwrap());

pub(super) static POPULATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("cc_number", DataType::Utf8, true),
        Field::new("role", DataType::Utf8View, false),
    ]))
});

pub(crate) struct PopulationDataBuilder {
    ids: FixedSizeBinaryBuilder,
    first_names: StringBuilder,
    last_names: StringBuilder,
    emails: StringBuilder,
    cc_numbers: StringBuilder,
    roles: StringViewBuilder,
    positions: PointBuilder,
    states: StringViewBuilder,

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
            roles: StringViewBuilder::new(),
            positions: PointBuilder::new(PointType::new(Dimension::XY, Default::default())),
            states: StringViewBuilder::new(),
            rng: rand::rng(),
        }
    }

    pub fn add_site(&mut self, n_people: usize, latitude: f64, longitude: f64) -> Result<()> {
        let gen_first_name = fake::faker::name::en::FirstName();
        let gen_last_name = fake::faker::name::en::LastName();
        let gen_email = fake::faker::internet::en::SafeEmail();
        let gen_cc = fake::faker::creditcard::en::CreditCardNumber();

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
            self.states.append_value(DEFAULT_STATE.as_str());
        }

        let latlng = LatLng::new(latitude, longitude)?;
        let cell_index = latlng.to_cell(Resolution::Nine);
        let cells = cell_index.grid_disk::<Vec<_>>(10);
        let solvent = SolventBuilder::new().build();
        let geom = solvent.dissolve(cells)?;

        let bounding_rect = geom
            .bounding_rect()
            .ok_or(Error::internal("failed to get bounding rect"))?;
        let (maxx, maxy) = bounding_rect.max().x_y();
        let (minx, miny) = bounding_rect.min().x_y();

        let x_range = Uniform::new(minx, maxx)?;
        let y_range = Uniform::new(miny, maxy)?;
        x_range
            .sample_iter(rand::rng())
            .zip(y_range.sample_iter(rand::rng()))
            .filter_map(|(x, y)| {
                let p = Point::new(x, y);
                geom.contains(&p).then_some(p)
            })
            .take(n_people)
            .for_each(|p| {
                self.positions.push_point(Some(&p));
            });

        let n_couriers = n_people / 10;

        let loc = geom
            .centroid()
            .ok_or(Error::internal("failed to get centroid"))?;
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
            self.states.append_value(DEFAULT_STATE.as_str());
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let ids = Arc::new(self.ids.finish());
        let first_names = Arc::new(self.first_names.finish());
        let last_names = Arc::new(self.last_names.finish());
        let emails = Arc::new(self.emails.finish());
        let cc_numbers = Arc::new(self.cc_numbers.finish());
        let roles = Arc::new(self.roles.finish());
        let positions = self.positions.finish().into_arrow();
        let states = Arc::new(self.states.finish());

        let all_fields = POPULATION_SCHEMA
            .as_ref()
            .fields()
            .iter()
            .cloned()
            .chain(vec![
                Arc::new(Field::new("position", positions.data_type().clone(), false)),
                Arc::new(Field::new("state", DataType::Utf8View, false)),
            ])
            .collect_vec();

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(all_fields)),
            vec![
                ids,
                first_names,
                last_names,
                emails,
                cc_numbers,
                roles,
                positions,
                states,
            ],
        )?)
    }
}
