use std::collections::{HashMap, VecDeque};

use counter::Counter;
use itertools::Itertools;
use tabled::Tabled;

use super::kitchen::{KitchenRunner, KitchenStats};
use crate::simulation::schemas::{OrderData, OrderLineStatus};
use crate::{Entity, Simulatable, State, error::Result};
use crate::{Event, idents::*};

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

impl std::ops::Add for SiteStats {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            queue_length: self.queue_length + other.queue_length,
        }
    }
}

pub struct SiteRunner {
    id: SiteId,

    /// Kitchens available at this location.
    kitchens: HashMap<KitchenId, KitchenRunner>,

    order_data: OrderData,

    /// Orders waiting to be processed at this location.
    order_queue: VecDeque<OrderId>,

    /// Order lines currently being processed at this location.
    order_lines: HashMap<OrderLineId, OrderLine>,
}

impl Entity for SiteRunner {
    type Id = SiteId;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl Simulatable for SiteRunner {
    fn step(&mut self, ctx: &State) -> Result<Vec<Event>> {
        let order_data = ctx.orders_for_site(&self.id)?;
        self.queue_order_data(&order_data)?;
        self.order_data = self.order_data.merge(order_data)?;

        // Process order queue
        let mut router = OrderRouter::new(&mut self.kitchens);
        while let Some(order_id) = self.order_queue.pop_front() {
            if let Some(order) = self.order_data.order(&order_id) {
                for line in order.lines() {
                    if let Some(line) = self.order_lines.get(line.id()) {
                        router.route_order_line(line.clone());
                    }
                }
            }
        }

        let mut completed_orders = Vec::new();
        for kitchen in self.kitchens.values_mut() {
            kitchen.step(ctx)?;
            completed_orders.extend(kitchen.take_completed());
        }

        let updates = completed_orders
            .iter()
            .map(|(_, line_id)| (line_id.clone(), OrderLineStatus::Ready));
        self.order_data.update_order_line_status(updates)?;

        Ok(vec![])
    }
}

impl SiteRunner {
    pub fn try_new(id: SiteId, state: &State) -> Result<Self> {
        let kitchens = state
            .object_data()
            .kitchens(&id)?
            .map_ok(|(id, brands)| {
                Ok::<_, Box<dyn std::error::Error>>((
                    id,
                    KitchenRunner::try_new(id, brands, state)?,
                ))
            })
            .flatten()
            .try_collect()?;

        let order_data = state.orders_for_site(&id)?;
        // let site = state.object_data().site(&id)?;

        Ok(SiteRunner {
            id,
            order_data,
            kitchens,
            order_queue: VecDeque::new(),
            order_lines: HashMap::new(),
        })
    }

    pub fn order_data(&self) -> &OrderData {
        &self.order_data
    }

    pub fn snapshot(&self) {
        println!("{:?}", self.stats());
        for kitchen in self.kitchens.values() {
            println!("{:?}", kitchen.stats());
        }
    }

    fn queue_order_data(&mut self, data: &OrderData) -> Result<()> {
        for order in data.orders() {
            for line in order.lines() {
                self.order_lines.insert(
                    *line.id(),
                    OrderLine {
                        id: *line.id(),
                        order_id: *line.order_id(),
                        item: (line.brand_id().try_into()?, line.menu_item_id().try_into()?),
                    },
                );
            }
            self.order_queue.push_back(*order.id());
        }

        Ok(())
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
