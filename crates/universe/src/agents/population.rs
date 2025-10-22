use geo_traits::to_geo::ToGeoPoint;
use h3o::{LatLng, Resolution};
use rand::Rng as _;

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
    let mut rng = rand::rng();

    // TODO: compute probability from person state
    rng.random_bool(1.0 / 1000.0).then(|| {
        state
            .objects()
            .sample_menu_items(None, &mut rng)
            .into_iter()
            .map(|menu_item| (menu_item.brand_id().try_into().unwrap(), menu_item.id()))
            .collect()
    })
}
