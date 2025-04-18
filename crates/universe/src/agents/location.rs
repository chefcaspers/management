use std::collections::{HashMap, VecDeque};

use itertools::Itertools;
use uuid::Uuid;

use super::Kitchen;
use crate::models::MenuItemRef;
use crate::simulation::{Entity, Simulatable, State};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LocationId(pub Uuid);

impl From<Uuid> for LocationId {
    fn from(id: Uuid) -> Self {
        LocationId(id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OrderId(pub Uuid);

impl From<Uuid> for OrderId {
    fn from(id: Uuid) -> Self {
        OrderId(id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OrderLineId(pub Uuid);

impl From<Uuid> for OrderLineId {
    fn from(id: Uuid) -> Self {
        OrderLineId(id)
    }
}

#[derive(Clone)]
pub struct Order {
    pub(crate) id: OrderId,
    pub(crate) lines: Vec<OrderLineId>,
}

#[derive(Clone)]
pub struct OrderLine {
    pub(crate) id: OrderLineId,
    pub(crate) order_id: OrderId,
    pub(crate) item: MenuItemRef,
}

pub struct Location {
    id: LocationId,
    name: String,
    kitchens: HashMap<Uuid, Kitchen>,
    orders: HashMap<OrderId, Order>,
    order_queue: VecDeque<OrderId>,
    order_lines: HashMap<OrderLineId, OrderLine>,
}

impl Entity for Location {
    fn id(&self) -> Uuid {
        self.id.0
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Simulatable for Location {
    fn step(&mut self, ctx: &State) -> Option<()> {
        // Queue incoming orders
        let orders = ctx.orders_for_location(&self.id).collect_vec();
        for items in orders {
            self.queue_order(items);
        }

        // Process order queue
        while let Some(order_id) = self.order_queue.pop_front() {
            if let Some(order) = self.orders.get(&order_id) {
                // Process order lines
                for line_id in &order.lines {
                    if let Some(line) = self.order_lines.get(line_id) {
                        // Process order line
                        // ...
                    }
                }
            }
        }

        Some(())
    }
}

impl Location {
    pub fn new(name: impl ToString) -> Self {
        Location {
            id: LocationId(Uuid::new_v4()),
            name: name.to_string(),
            kitchens: HashMap::new(),
            orders: HashMap::new(),
            order_queue: VecDeque::new(),
            order_lines: HashMap::new(),
        }
    }

    pub fn add_kitchen(&mut self, kitchen: Kitchen) {
        self.kitchens.insert(kitchen.id(), kitchen);
    }

    fn queue_order(&mut self, items: impl IntoIterator<Item = MenuItemRef>) {
        let mut order = Order {
            id: OrderId(Uuid::new_v4()),
            lines: Vec::new(),
        };

        for item in items {
            let line = OrderLine {
                id: OrderLineId(Uuid::new_v4()),
                order_id: order.id,
                item,
            };
            order.lines.push(line.id);
            self.order_lines.insert(line.id, line);
        }

        self.order_queue.push_back(order.id);
        self.orders.insert(order.id, order);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::KitchenStation;

    fn setup() -> Location {
        let mut location = Location::new("locations/location-1");

        let kitchens = [
            vec![
                ("workstation-1", KitchenStation::Workstation),
                ("workstation-2", KitchenStation::Workstation),
                ("stove", KitchenStation::Stove),
                ("oven", KitchenStation::Oven),
            ],
            vec![
                ("workstation-1", KitchenStation::Workstation),
                ("workstation-2", KitchenStation::Workstation),
                ("stove", KitchenStation::Stove),
                ("oven", KitchenStation::Oven),
            ],
        ];

        for (index, stations) in kitchens.iter().enumerate() {
            let mut kitchen = Kitchen::new(format!(
                "{}/kitchens/kitchen-{}",
                location.name(),
                index + 1
            ));
            for (name, station) in stations {
                kitchen.add_station(
                    format!("{}/stations/{}", kitchen.name(), name),
                    station.clone(),
                );
            }
            location.add_kitchen(kitchen);
        }

        location
    }

    #[test_log::test]
    fn test_new_location() {
        let location = setup();
        println!("{}", location.name());
    }
}
