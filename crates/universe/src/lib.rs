use chrono::Duration;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use url::Url;

pub use self::agents::*;
pub use self::error::*;
pub use self::idents::*;
pub use self::models::*;
pub use self::simulation::*;
pub use self::state::*;

#[cfg(feature = "python")]
use pyo3::prelude::*;

mod agents;
mod error;
mod idents;
pub mod init;
mod models;
#[cfg(feature = "python")]
mod python;
mod simulation;
pub mod state;

#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
#[derive(Debug, Clone)]
pub struct SimulationSetup {
    pub sites: Vec<SiteSetup>,
    pub brands: Vec<Brand>,
}

impl SimulationSetup {
    pub async fn load(store: &dyn ObjectStore, base_path: &Path) -> Result<Self> {
        let sites_path = base_path.child("sites");
        let brands_path = base_path.child("brands");

        let sites = SimulationSetup::load_sites(store, &sites_path).await?;
        let brands = SimulationSetup::load_brands(store, &brands_path).await?;

        Ok(SimulationSetup { sites, brands })
    }

    async fn load_sites(store: &dyn ObjectStore, sites_path: &Path) -> Result<Vec<SiteSetup>> {
        let site_files: Vec<_> = store.list(Some(sites_path)).try_collect().await?;

        let mut sites = Vec::new();
        for file in site_files
            .into_iter()
            .filter(|file| file.location.extension() == Some("json"))
        {
            let site_bytes = store.get(&file.location).await?.bytes().await?;
            let mut site_setup: SiteSetup = serde_json::from_slice(&site_bytes)?;
            if let Some(ref mut site) = site_setup.info {
                site.id = SiteId::from_uri_ref(format!("sites/{}", site.name)).to_string();
                site_setup.kitchens = site_setup
                    .kitchens
                    .into_iter()
                    .map(|mut kitchen_setup| {
                        if let Some(ref mut kitchen) = kitchen_setup.info {
                            kitchen.id = KitchenId::from_uri_ref(format!(
                                "sites/{}/kitchens/{}",
                                site.name, kitchen.name
                            ))
                            .to_string();

                            for station in &mut kitchen_setup.stations {
                                station.id = StationId::from_uri_ref(format!(
                                    "sites/{}/kitchens/{}/stations/{}",
                                    site.name, kitchen.name, station.name
                                ))
                                .to_string();
                            }
                        }

                        kitchen_setup
                    })
                    .collect();

                sites.push(site_setup);
            } else {
                return Err(Error::invalid_data("missing site information"));
            };
        }

        Ok(sites)
    }

    async fn load_brands(store: &dyn ObjectStore, brands_path: &Path) -> Result<Vec<Brand>> {
        Ok(crate::init::generate_brands())
    }
}

pub async fn load_simulation_setup<I, K, V>(url: &Url, options: I) -> Result<SimulationSetup>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (store, path) = object_store::parse_url_opts(url, options)?;
    SimulationSetup::load(&store, &path).await
}

pub async fn run_simulation(
    setup: SimulationSetup,
    duration: usize,
    output_location: Url,
) -> Result<(), Error> {
    let simulation = SimulationBuilder::new()
        .with_result_storage_location(output_location)
        .with_snapshot_interval(Duration::minutes(10))
        .with_time_increment(Duration::minutes(1));

    // add barnds to simulation
    let simulation = setup
        .brands
        .into_iter()
        .fold(simulation, |sim, brand| sim.with_brand(brand));

    // add sites to simulation
    let simulation = setup
        .sites
        .into_iter()
        .fold(simulation, |sim, site| sim.with_site(site));

    let mut simulation = simulation.build()?;

    simulation.run(duration).await?;

    Ok(())
}
