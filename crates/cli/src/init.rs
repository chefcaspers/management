use caspers_universe::{
    Brand, BrandId, Error, KitchenId, MenuItemId, SimulationSetup, SiteId, SiteSetup, StationId,
};
use dialoguer::MultiSelect;
use itertools::Itertools as _;

use crate::error::Result;

#[derive(Debug, Clone, clap::Parser)]
pub(super) struct InitArgs {
    #[arg(short, long, default_value_t = true)]
    template: bool,
}

pub(super) async fn handle(args: InitArgs) -> Result<()> {
    if args.template {
        let sites = vec![
            SiteTemplate::Amsterdam,
            SiteTemplate::Berlin,
            SiteTemplate::London,
        ];
        let brands = vec![
            BrandTemplate::Asian,
            BrandTemplate::FastFood,
            BrandTemplate::Mexican,
        ];

        let Some(site_selection) = MultiSelect::new()
            .with_prompt("Which sites should be included?")
            .items(&sites)
            .defaults(&[true, false, true])
            .interact_opt()?
        else {
            return Ok(());
        };

        let Some(brand_selection) = MultiSelect::new()
            .with_prompt("Which brands should be included?")
            .items(&brands)
            .defaults(&[true, true, true])
            .interact_opt()?
        else {
            return Ok(());
        };

        let selected_sites = site_selection
            .into_iter()
            .map(|idx| sites[idx])
            .collect::<Vec<_>>();

        let selected_brands = brand_selection
            .into_iter()
            .map(|idx| brands[idx])
            .collect::<Vec<_>>();

        let template = Template::new(selected_sites, selected_brands);

        let _setup = template.load()?;

        println!("Template loaded successfully");
    } else {
        println!("Initializing without template");
    }
    Ok(())
}

pub struct Template {
    sites: Vec<SiteTemplate>,
    brands: Vec<BrandTemplate>,
}

impl Template {
    pub fn new(sites: Vec<SiteTemplate>, brands: Vec<BrandTemplate>) -> Self {
        Self { sites, brands }
    }

    pub fn load(&self) -> Result<SimulationSetup> {
        load_template(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SiteTemplate {
    Amsterdam,
    Berlin,
    London,
}

impl std::fmt::Display for SiteTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SiteTemplate::Amsterdam => write!(f, "Amsterdam"),
            SiteTemplate::Berlin => write!(f, "Berlin"),
            SiteTemplate::London => write!(f, "London"),
        }
    }
}

impl SiteTemplate {
    fn data(&self) -> &[u8] {
        match self {
            SiteTemplate::Amsterdam => include_bytes!("../templates/base/sites/amsterdam.json"),
            SiteTemplate::Berlin => todo!(),
            SiteTemplate::London => include_bytes!("../templates/base/sites/london.json"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BrandTemplate {
    Asian,
    FastFood,
    Mexican,
}

impl std::fmt::Display for BrandTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrandTemplate::Asian => write!(f, "Asian"),
            BrandTemplate::FastFood => write!(f, "FastFood"),
            BrandTemplate::Mexican => write!(f, "Mexican"),
        }
    }
}

impl BrandTemplate {
    pub fn data(&self) -> &[u8] {
        match self {
            BrandTemplate::Asian => include_bytes!("../templates/base/brands/asian.json"),
            BrandTemplate::FastFood => include_bytes!("../templates/base/brands/fast_food.json"),
            BrandTemplate::Mexican => include_bytes!("../templates/base/brands/mexican.json"),
        }
    }
}

fn load_template(template: &Template) -> Result<SimulationSetup> {
    let sites = template.sites.iter().map(load_site).try_collect()?;
    let brands = template.brands.iter().map(load_brand).try_collect()?;
    Ok(SimulationSetup { sites, brands })
}

fn load_brand(brand: &BrandTemplate) -> Result<Brand> {
    let mut brand: Brand = serde_json::from_slice(brand.data()).map_err(Error::from)?;
    brand.id = BrandId::from_uri_ref(format!("brands/{}", brand.name)).to_string();

    for menu_item in brand.items.iter_mut() {
        menu_item.id =
            MenuItemId::from_uri_ref(format!("brands/{}/menu_items/{}", brand.id, menu_item.name))
                .to_string();
    }

    Ok(brand)
}

fn load_site(site: &SiteTemplate) -> Result<SiteSetup> {
    let mut site_setup: SiteSetup = serde_json::from_slice(site.data()).map_err(Error::from)?;
    let Some(ref mut site) = site_setup.info else {
        return Err(Error::invalid_data("missing site information").into());
    };
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

    Ok(site_setup)
}
