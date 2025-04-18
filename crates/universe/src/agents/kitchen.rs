use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::OrderLine;
use crate::idents::*;
use crate::models::{KitchenStation, MenuItemRef};
use crate::simulation::{Entity, Simulatable, State};

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
    pub(crate) id: StationId,
    name: String,
    station_type: KitchenStation,
    status: StationStatus,
}

impl Entity for Station {
    fn id(&self) -> uuid::Uuid {
        self.id.as_ref().clone()
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

#[derive(Clone, Debug)]
pub struct KitchenStats {
    queued: usize,
    in_progress: usize,
    completed: usize,
    idle_assets: usize,
    total_assets: usize,
    simulation_time: Duration,
}

pub struct Kitchen {
    pub(crate) id: KitchenId,
    name: String,
    stations: Vec<Station>,
    queue: VecDeque<OrderLine>,
    in_progress: HashMap<OrderLineId, OrderProgress>,
    completed: Vec<MenuItemRef>,
    simulation_time: Duration,
    accepted_brands: HashSet<BrandId>,
}

impl Entity for Kitchen {
    fn id(&self) -> uuid::Uuid {
        self.id.as_ref().clone()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Simulatable for Kitchen {
    fn step(&mut self, ctx: &State) -> Option<()> {
        let time_step = ctx.time_step();
        self.simulation_time += time_step;

        // Try to start new recipes if possible
        while self.start_order_line(ctx) {}

        // Process in-progress recipes
        let mut completed_recipe_ids = Vec::new();
        let mut to_update = Vec::new();

        for (recipe_id, progress) in self.in_progress.iter() {
            match &progress.status {
                OrderLineStatus::InProgress(instruction_idx, stated_time) => {
                    let expected_duration = progress.order_line.item.1.instructions
                        [*instruction_idx]
                        .expected_duration
                        .map(|duration| duration.seconds)
                        .unwrap_or(0);

                    // Check if the recipe will be completed within the current time step
                    if (ctx.next_time() - stated_time).num_seconds() < expected_duration {
                        continue;
                    }

                    // We finished to current step, so release the current asset
                    let curr = &progress.order_line.item.1.instructions[*instruction_idx];
                    release_station(&mut self.stations, &curr.required_station, recipe_id);

                    // Move to next instruction
                    let next_idx = instruction_idx + 1;
                    if next_idx >= progress.order_line.item.1.instructions.len() {
                        // Recipe is complete
                        completed_recipe_ids.push(*recipe_id);
                        continue;
                    }

                    // Move the order to the next station, or block if not available
                    let next_step = &progress.order_line.item.1.instructions[next_idx];
                    if let Some(idx) = take_station(&self.stations, &next_step.required_station) {
                        self.stations[idx].status = StationStatus::InUse(*recipe_id);
                        to_update.push((
                            *recipe_id,
                            OrderLineStatus::InProgress(next_idx, ctx.next_time()),
                        ));
                    } else {
                        to_update.push((*recipe_id, OrderLineStatus::Blocked(next_idx)));
                    }
                }
                OrderLineStatus::Blocked(instruction_idx) => {
                    // Check if we can now acquire the needed asset
                    let step = &progress.order_line.item.1.instructions[*instruction_idx];
                    if let Some(asset_idx) = take_station(&self.stations, &step.required_station) {
                        // Mark asset as in use
                        self.stations[asset_idx].status = StationStatus::InUse(*recipe_id);
                        to_update.push((
                            *recipe_id,
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
                self.completed.push(progress.order_line.item.1);
            }
        }

        Some(())
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
            simulation_time: Duration::from_secs(0),
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

    fn start_order_line(&mut self, ctx: &State) -> bool {
        if let Some(order_line) = self.queue.pop_front() {
            // Check if we can start the first step
            let step = &order_line.item.1.instructions[0];
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

                true
            } else {
                // Can't start the recipe yet, put it back in the queue
                self.queue.push_front(order_line);
                false
            }
        } else {
            false
        }
    }

    /// Get statistics about the kitchen's current state.
    pub fn stats(&self) -> KitchenStats {
        KitchenStats {
            queued: self.queue.len(),
            in_progress: self.in_progress.len(),
            completed: self.completed.len(),
            idle_assets: self
                .stations
                .iter()
                .filter(|a| matches!(a.status, StationStatus::Available))
                .count(),
            total_assets: self.stations.len(),
            simulation_time: self.simulation_time,
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
