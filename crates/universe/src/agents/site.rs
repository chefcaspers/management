use std::collections::{HashMap, VecDeque};

use counter::Counter;
use itertools::Itertools;
use tabled::Tabled;

use super::kitchen::{Kitchen, KitchenStats};
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
    kitchens: &'a mut HashMap<KitchenId, Kitchen>,
    brand_to_kitchens: HashMap<BrandId, Vec<KitchenId>>,
    submit_counter: Counter<BrandId>,
}

impl<'a> OrderRouter<'a> {
    fn new(kitchens: &'a mut HashMap<KitchenId, Kitchen>) -> Self {
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

pub struct Site {
    id: SiteId,
    name: String,
    /// Kitchens available at this location.
    kitchens: HashMap<KitchenId, Kitchen>,
    /// Orders currently being processed at this location.
    orders: HashMap<OrderId, Order>,
    /// Orders waiting to be processed at this location.
    order_queue: VecDeque<OrderId>,
    /// Order lines currently being processed at this location.
    order_lines: HashMap<OrderLineId, OrderLine>,
}

impl Entity for Site {
    type Id = SiteId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Simulatable for Site {
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
        }

        Ok(())
    }
}

impl Site {
    pub fn new(name: impl ToString) -> Self {
        let name = name.to_string();
        Site {
            id: SiteId::from_uri_ref(&name),
            name,
            kitchens: HashMap::new(),
            orders: HashMap::new(),
            order_queue: VecDeque::new(),
            order_lines: HashMap::new(),
        }
    }

    pub fn snapshot(&self) {
        for kitchen in self.kitchens.values() {
            println!("{:?}", kitchen.stats());
        }
    }

    pub fn add_kitchen(&mut self, kitchen: Kitchen) {
        self.kitchens.insert(*kitchen.id(), kitchen);
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
