mod agents;
mod error;
mod idents;
pub mod init;
mod models;
mod simulation;
pub mod state;

use chrono::Duration;
use url::Url;

pub use self::agents::*;
pub use self::error::*;
pub use self::idents::*;
pub use self::models::*;
pub use self::simulation::*;
pub use self::state::*;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymethods]
impl Site {
    #[new]
    #[pyo3(signature = (id, name, latitude, longitude))]
    fn new(id: String, name: String, latitude: f64, longitude: f64) -> Self {
        Site {
            id,
            name,
            latitude,
            longitude,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Site(id={}, name={}, latitude={}, longitude={})",
            self.id, self.name, self.latitude, self.longitude
        )
    }
}

pub fn run_simulation(
    sites: Vec<Site>,
    brands: Vec<Brand>,
    duration: usize,
    output_location: Url,
) -> Result<(), Error> {
    let simulation = SimulationBuilder::new()
        .with_result_storage_location(output_location)
        .with_snapshot_interval(Duration::minutes(10))
        .with_time_increment(Duration::minutes(1));

    // add barnds to simulation
    let simulation = brands
        .into_iter()
        .fold(simulation, |sim, brand| sim.with_brand(brand));

    // add sites to simulation
    let simulation = sites
        .into_iter()
        .fold(simulation, |sim, site| sim.with_site(site));

    let mut simulation = simulation.build()?;
    simulation.run(duration)?;

    Ok(())
}
