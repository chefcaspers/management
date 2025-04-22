use std::collections::{HashMap, VecDeque};

use counter::Counter;
use itertools::Itertools;
use tabled::Tabled;

use super::kitchen::{KitchenRunner, KitchenStats};
use crate::idents::*;
use crate::{Entity, Simulatable, State, error::Result};

#[derive(Clone)]
pub struct Order {
    pub(crate) id: OrderId,
    pub(crate) lines: Vec<OrderLineId>,
}

#[derive(Clone)]
pub struct OrderLine {
    pub(crate) id: OrderLineId,
    pub(crate) order_id: OrderId,
    pub(crate) item: (BrandId, MenuItemId),
}

struct OrderRouter<'a> {
    kitchens: &'a mut HashMap<KitchenId, KitchenRunner>,
    brand_to_kitchens: HashMap<BrandId, Vec<KitchenId>>,
    submit_counter: Counter<BrandId>,
}

impl<'a> OrderRouter<'a> {
    fn new(kitchens: &'a mut HashMap<KitchenId, KitchenRunner>) -> Self {
        let brand_to_kitchens = kitchens
            .iter()
            .flat_map(|(id, kitchen)| kitchen.accepted_brands().iter().map(|brand| (*brand, *id)))
            .into_group_map();
        OrderRouter {
            kitchens,
            brand_to_kitchens,
            submit_counter: Counter::new(),
        }
    }

    pub fn route_order_line(&mut self, order_line: OrderLine) {
        let brand = order_line.item.0;
        self.submit_counter[&brand] += 1;
        let kitchen_ids = &self.brand_to_kitchens[&brand];
        let index = self.submit_counter[&brand] % kitchen_ids.len();
        if let Some(kitchen) = self.kitchens.get_mut(&kitchen_ids[index]) {
            kitchen.queue_order_line(order_line);
        } else {
            tracing::error!("No kitchen available for brand {:?}", brand);
        }
    }
}

#[derive(Clone, Debug, Tabled, Default, PartialEq, Eq)]
pub struct SiteStats {
    pub queue_length: usize,
}

pub struct SiteRunner {
    id: SiteId,
    name: String,

    /// Kitchens available at this location.
    kitchens: HashMap<KitchenId, KitchenRunner>,

    /// Orders currently being processed at this location.
    orders: HashMap<OrderId, Order>,

    /// Orders waiting to be processed at this location.
    order_queue: VecDeque<OrderId>,

    /// Order lines currently being processed at this location.
    order_lines: HashMap<OrderLineId, OrderLine>,

    /// Completed orders at this location.
    completed_order_lines: HashMap<OrderId, Vec<OrderLineId>>,
}

impl Entity for SiteRunner {
    type Id = SiteId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Simulatable for SiteRunner {
    fn step(&mut self, ctx: &State) -> Result<()> {
        // Process order queue
        let mut router = OrderRouter::new(&mut self.kitchens);
        while let Some(order_id) = self.order_queue.pop_front() {
            if let Some(order) = self.orders.get(&order_id) {
                for line_id in &order.lines {
                    if let Some(line) = self.order_lines.get(line_id) {
                        router.route_order_line(line.clone());
                    }
                }
            }
        }

        for kitchen in self.kitchens.values_mut() {
            kitchen.step(ctx)?;
            for (order_id, line_id) in kitchen.take_completed() {
                self.completed_order_lines
                    .entry(order_id)
                    .or_default()
                    .push(line_id);
            }
        }

        Ok(())
    }
}

impl SiteRunner {
    pub fn try_new(id: SiteId, state: &State) -> Result<Self> {
        let kitchens = state
            .vendors
            .kitchens(&id)?
            .map_ok(|(id, brands)| {
                Ok::<_, Box<dyn std::error::Error>>((
                    id,
                    KitchenRunner::try_new(id, brands, state)?,
                ))
            })
            .flatten()
            .try_collect()?;
        Ok(SiteRunner {
            id,
            name: "DUMMY".to_string(),
            kitchens,
            orders: HashMap::new(),
            order_queue: VecDeque::new(),
            order_lines: HashMap::new(),
            completed_order_lines: HashMap::new(),
        })
    }

    pub fn snapshot(&self) {
        for kitchen in self.kitchens.values() {
            println!("{:?}", kitchen.stats());
        }
    }

    pub(crate) fn queue_order(&mut self, items: impl IntoIterator<Item = (BrandId, MenuItemId)>) {
        let mut order = Order {
            id: OrderId::new(),
            lines: Vec::new(),
        };

        for item in items {
            let line = OrderLine {
                id: OrderLineId::new(),
                order_id: order.id,
                item,
            };
            order.lines.push(line.id);
            self.order_lines.insert(line.id, line);
        }

        self.order_queue.push_back(order.id);
        self.orders.insert(order.id, order);
    }

    pub fn stats(&self) -> SiteStats {
        SiteStats {
            queue_length: self.order_queue.len(),
        }
    }

    pub fn kitchen_stats(&self) -> impl Iterator<Item = KitchenStats> {
        self.kitchens.values().map(|kitchen| kitchen.stats())
    }

    pub fn total_kitchen_stats(&self) -> KitchenStats {
        self.kitchen_stats()
            .fold(KitchenStats::default(), |acc, stats| acc + stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use geo::{BoundingRect, Contains, Point};
    use geo_types::{LineString, Polygon};
    use h3o::{LatLng, Resolution};

    #[test]
    fn test_queue_order() {
        let latlng = LatLng::new(0.0, 0.0).unwrap();
        let cell_index = latlng.to_cell(Resolution::Six);

        let boundary: LineString = cell_index.boundary().into_iter().cloned().collect();

        let polygon: Polygon = Polygon::new(boundary, Vec::new());
        polygon.contains(&Point::new(0., 0.));

        println!("{:#?}", polygon);
    }
}
