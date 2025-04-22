use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use tabled::Tabled;

use super::OrderLine;
use crate::idents::*;
use crate::models::KitchenStation;
use crate::{Entity, Simulatable, State, error::Result};

#[derive(Clone)]
enum StationStatus {
    // Station is available for use
    Available,

    // Stores the recipe ID using this station
    InUse(OrderLineId),

    // Out of order
    OutOfOrder,
}

/// A kitchen station
///
/// Represents a station in the kitchen where certain instructions can be executed.
/// This can be a workstation - i.e. a place where a chef can perform a task and
/// has cutting board, knives, and other necessary tools - or some more complex station
/// such as a freezer, stove, or oven.
#[derive(Clone)]
struct Station {
    id: StationId,
    name: String,
    station_type: KitchenStation,
    status: StationStatus,
}

impl Entity for Station {
    type Id = StationId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Station {
    pub fn new(name: impl ToString, station_type: KitchenStation) -> Self {
        let name = name.to_string();
        Station {
            id: StationId::from_uri_ref(&name),
            name,
            station_type,
            status: StationStatus::Available,
        }
    }
}

#[derive(Clone)]
enum OrderLineStatus {
    Queued,
    // Current instruction index
    InProgress(usize, DateTime<Utc>),
    Completed,
    // Blocked at instruction index
    Blocked(usize),
}

#[derive(Clone)]
struct OrderProgress {
    // The order line item being processed
    order_line: OrderLine,

    // The processing status of the recipe
    status: OrderLineStatus,
}

#[derive(Clone, Debug, Tabled, Default, PartialEq, Eq)]
pub struct KitchenStats {
    pub queued: usize,
    pub in_progress: usize,
    pub completed: usize,
    pub idle_stations: usize,
    pub total_stations: usize,
}

impl std::ops::Add for KitchenStats {
    type Output = KitchenStats;

    fn add(self, other: KitchenStats) -> KitchenStats {
        KitchenStats {
            queued: self.queued + other.queued,
            in_progress: self.in_progress + other.in_progress,
            completed: self.completed + other.completed,
            idle_stations: self.idle_stations + other.idle_stations,
            total_stations: self.total_stations + other.total_stations,
        }
    }
}

pub struct Kitchen {
    id: KitchenId,
    name: String,
    stations: Vec<Station>,
    queue: VecDeque<OrderLine>,
    in_progress: HashMap<OrderLineId, OrderProgress>,
    completed: Vec<(OrderId, OrderLineId)>,
    accepted_brands: HashSet<BrandId>,
}

impl Entity for Kitchen {
    type Id = KitchenId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Simulatable for Kitchen {
    fn step(&mut self, ctx: &State) -> Result<()> {
        // Try to start new recipes if possible
        while self.start_order_line(ctx)? {}

        // Process in-progress recipes
        let mut completed_recipe_ids = Vec::new();
        let mut to_update = Vec::new();

        for (order_line_id, progress) in self.in_progress.iter() {
            let menu_item = ctx.menu_item(&progress.order_line.item)?;
            match &progress.status {
                OrderLineStatus::InProgress(instruction_idx, stated_time) => {
                    let expected_duration = menu_item.instructions[*instruction_idx]
                        .expected_duration
                        .map(|duration| duration.seconds)
                        .unwrap_or(0);

                    // Check if the recipe will be completed within the current time step
                    if (ctx.next_time() - stated_time).num_seconds() < expected_duration {
                        continue;
                    }

                    // We finished to current step, so release the current asset
                    let curr = &menu_item.instructions[*instruction_idx];
                    release_station(&mut self.stations, &curr.required_station, order_line_id);

                    // Move to next instruction
                    let next_idx = instruction_idx + 1;
                    if next_idx >= menu_item.instructions.len() {
                        // Recipe is complete
                        completed_recipe_ids.push(*order_line_id);
                        continue;
                    }

                    // Move the order to the next station, or block if not available
                    let next_step = &menu_item.instructions[next_idx];
                    if let Some(idx) = take_station(&self.stations, &next_step.required_station) {
                        self.stations[idx].status = StationStatus::InUse(*order_line_id);
                        to_update.push((
                            *order_line_id,
                            OrderLineStatus::InProgress(next_idx, ctx.next_time()),
                        ));
                    } else {
                        to_update.push((*order_line_id, OrderLineStatus::Blocked(next_idx)));
                    }
                }
                OrderLineStatus::Blocked(instruction_idx) => {
                    // Check if we can now acquire the needed asset
                    let step = &menu_item.instructions[*instruction_idx];
                    if let Some(asset_idx) = take_station(&self.stations, &step.required_station) {
                        // Mark asset as in use
                        self.stations[asset_idx].status = StationStatus::InUse(*order_line_id);
                        to_update.push((
                            *order_line_id,
                            OrderLineStatus::InProgress(*instruction_idx, ctx.next_time()),
                        ));
                    }
                }
                _ => (),
            }
        }

        // Apply updates
        for (recipe_id, status) in to_update {
            if let Some(progress) = self.in_progress.get_mut(&recipe_id) {
                progress.status = status;
            }
        }

        // Move completed recipes
        for recipe_id in completed_recipe_ids {
            if let Some(progress) = self.in_progress.remove(&recipe_id) {
                self.completed
                    .push((progress.order_line.order_id, progress.order_line.id));
            }
        }

        Ok(())
    }
}

impl Kitchen {
    pub fn new(name: impl ToString) -> Self {
        let name = name.to_string();
        Kitchen {
            id: KitchenId::from_uri_ref(&name),
            name,
            stations: Vec::new(),
            queue: VecDeque::new(),
            in_progress: HashMap::new(),
            completed: Vec::new(),
            accepted_brands: HashSet::new(),
        }
    }

    pub fn accepted_brands(&self) -> &HashSet<BrandId> {
        &self.accepted_brands
    }

    pub fn add_station(&mut self, name: impl ToString, station_type: KitchenStation) {
        self.stations.push(Station::new(name, station_type));
    }

    pub fn add_accepted_brand(&mut self, brand_id: BrandId) {
        self.accepted_brands.insert(brand_id);
    }

    pub fn queue_order_line(&mut self, item: OrderLine) {
        self.queue.push_back(item);
    }

    fn start_order_line(&mut self, ctx: &State) -> Result<bool> {
        if let Some(order_line) = self.queue.pop_front() {
            let menu_item = ctx.menu_item(&order_line.item)?;
            // Check if we can start the first step
            let step = &menu_item.instructions[0];
            if let Some(asset_idx) = take_station(&self.stations, &step.required_station) {
                // Mark asset as in use
                self.stations[asset_idx].status = StationStatus::InUse(order_line.id);

                // Add recipe to in-progress with first instruction
                self.in_progress.insert(
                    order_line.id,
                    OrderProgress {
                        order_line,
                        status: OrderLineStatus::InProgress(0, ctx.current_time()),
                    },
                );

                Ok(true)
            } else {
                // Can't start the recipe yet, put it back in the queue
                self.queue.push_front(order_line);
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Get statistics about the kitchen's current state.
    pub fn stats(&self) -> KitchenStats {
        KitchenStats {
            queued: self.queue.len(),
            in_progress: self.in_progress.len(),
            completed: self.completed.len(),
            idle_stations: self
                .stations
                .iter()
                .filter(|a| matches!(a.status, StationStatus::Available))
                .count(),
            total_stations: self.stations.len(),
        }
    }
}

fn take_station(assets: &[Station], asset_type: &i32) -> Option<usize> {
    assets.iter().position(|asset| {
        matches!(asset.status, StationStatus::Available)
            && &(asset.station_type as i32) == asset_type
    })
}

fn release_station(assets: &mut Vec<Station>, asset_type: &i32, recipe_id: &OrderLineId) {
    for asset in assets {
        if &(asset.station_type as i32) == asset_type {
            if let StationStatus::InUse(id) = &asset.status {
                if id == recipe_id {
                    asset.status = StationStatus::Available;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agents::Site,
        models::{Brand, Instruction, MenuItem},
    };

    use super::*;

    fn dummy_brand() -> (BrandId, Brand) {
        let id = BrandId::from_uri_ref("brands/dummy");
        let brand = Brand {
            id: Some(id.to_string()),
            name: "dummy".to_string(),
            description: "Dummy Brand Description".to_string(),
            category: "dummy".to_string(),
            items: vec![MenuItem {
                id: MenuItemId::from_uri_ref("brands/dummy/items/dummy").to_string(),
                name: "dummy".to_string(),
                description: "Dummy Item Description".to_string(),
                price: 10.0,
                image_url: None,
                ingredients: vec![],
                instructions: vec![Instruction {
                    step: "step".to_string(),
                    description: "description".to_string(),
                    required_station: KitchenStation::Workstation as i32,
                    expected_duration: Some(pbjson_types::Duration {
                        seconds: 110,
                        nanos: 0,
                    }),
                }],
            }],
        };
        (id, brand)
    }

    #[test]
    fn test_kitchen_stats() {
        let brand = dummy_brand();
        let mut state = State::try_new(vec![brand.clone()]).unwrap();

        let mut kitchen = Kitchen::new("some-kitchen");
        kitchen.add_station("station-1".to_string(), KitchenStation::Workstation);
        kitchen.add_accepted_brand(brand.0);

        let mut site = Site::new("some-site");
        site.add_kitchen(kitchen);

        site.queue_order(Some((
            brand.0,
            uuid::Uuid::parse_str(&brand.1.items[0].id).unwrap().into(),
        )));

        let expected_stats = [
            KitchenStats {
                queued: 0,
                in_progress: 0,
                completed: 0,
                idle_stations: 1,
                total_stations: 1,
            },
            KitchenStats {
                queued: 0,
                in_progress: 1,
                completed: 0,
                idle_stations: 0,
                total_stations: 1,
            },
            KitchenStats {
                queued: 0,
                in_progress: 0,
                completed: 1,
                idle_stations: 1,
                total_stations: 1,
            },
        ];

        for stats in expected_stats {
            assert_eq!(site.kitchen_stats().next().unwrap(), stats);
            site.step(&state).unwrap();
            state.step();
        }
    }
}
