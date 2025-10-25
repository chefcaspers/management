use std::f64;

use chrono::Timelike;
use geo_traits::to_geo::ToGeoPoint;
use h3o::{LatLng, Resolution};
use rand::Rng as _;
use tracing::{Level, instrument};

use crate::{
    BrandId, EntityView as _, EventPayload, MenuItemId, OrderCreatedPayload, PersonRole, Result,
    SiteId, State,
};

pub struct PopulationRunner {}

impl Default for PopulationRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl PopulationRunner {
    pub fn new() -> Self {
        PopulationRunner {}
    }

    #[instrument(
        name = "step_population",
        level = Level::TRACE,
        skip(self, ctx),
        fields(
            caspers.site_id = site_id.to_string()
        )
    )]
    pub(crate) fn step(
        &self,
        site_id: &SiteId,
        ctx: &State,
    ) -> Result<impl Iterator<Item = EventPayload>> {
        let site = ctx.objects().site(site_id)?;
        let props = site.properties()?;
        let lat_lng = LatLng::new(props.latitude, props.longitude)?;

        Ok(ctx
            .population()
            // NB: resolution 6 corresponds to a cell size of approximately 36 km2
            .idle_people_in_cell(lat_lng.to_cell(Resolution::Six), &PersonRole::Customer)
            .filter_map(|person| create_order(ctx).map(|items| (person, items)))
            .flat_map(|(person, items)| {
                Some(EventPayload::OrderCreated(OrderCreatedPayload {
                    site_id: *site_id,
                    person_id: *person.id(),
                    items,
                    destination: person.position().ok()?.to_point(),
                }))
            }))
    }
}

fn create_order(state: &State) -> Option<Vec<(BrandId, MenuItemId)>> {
    use std::f64::consts::{E, PI};

    let mut rng = rand::rng();

    let current_time = state.current_time();
    let current_minutes = (current_time.hour() * 60 + current_time.minute()) as f64 / 60.0;

    let sigma_sq = 0.4_f64;

    let bell = |x: f64, mu: f64| {
        let exponent = -(x - mu).powi(2) / (2.0 * sigma_sq);
        1.0 / (2.0 * PI * sigma_sq).powf(2.0) * E.powf(exponent)
    };

    let prob = 0.01 * (bell(current_minutes, 12.0) + bell(current_minutes, 18.0));

    // TODO: compute probability from person state
    rng.random_bool(prob).then(|| {
        state
            .objects()
            .sample_menu_items(None, &mut rng)
            .into_iter()
            .map(|menu_item| (menu_item.brand_id().try_into().unwrap(), menu_item.id()))
            .collect()
    })
}
