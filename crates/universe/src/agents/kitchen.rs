use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::debug;
use uuid::Uuid;

use crate::models::{KitchenStation, MenuItemRef};
use crate::simulation::{Entity, State};

#[derive(Clone)]
enum StationStatus {
    // Station is available for use
    Available,

    // Stores the recipe ID using this station
    InUse(Uuid),

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
    id: String,
    name: String,
    station_type: KitchenStation,
    status: StationStatus,
}

impl Station {
    pub fn new(name: impl ToString, station_type: KitchenStation) -> Self {
        Station {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            station_type,
            status: StationStatus::Available,
        }
    }
}

#[derive(Clone)]
struct OrderLine {
    id: Uuid,
    order_id: Uuid,
    item: MenuItemRef,
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
    // The recipe being processed
    recipe: OrderLine,

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
    id: Uuid,
    name: String,
    stations: Vec<Station>,
    queue: VecDeque<OrderLine>,
    in_progress: HashMap<Uuid, OrderProgress>,
    completed: Vec<MenuItemRef>,
    simulation_time: Duration,
}

impl Entity for Kitchen {
    fn id(&self) -> Uuid {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Kitchen {
    pub fn new(name: impl ToString) -> Self {
        Kitchen {
            id: Uuid::new_v4(),
            name: name.to_string(),
            stations: Vec::new(),
            queue: VecDeque::new(),
            in_progress: HashMap::new(),
            completed: Vec::new(),
            simulation_time: Duration::from_secs(0),
        }
    }

    pub fn add_asset(&mut self, name: impl ToString, station_type: KitchenStation) {
        self.stations.push(Station {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            station_type,
            status: StationStatus::Available,
        });
    }

    pub fn queue_order_line(&mut self, item: MenuItemRef) {
        self.queue.push_back(OrderLine {
            id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            item,
        });
    }

    fn start_order_line(&mut self, ctx: &State) -> bool {
        if let Some(recipe) = self.queue.pop_front() {
            let recipe_id = recipe.id.clone();
            debug!("starting recipe {}", recipe_id);

            // Check if we can start the first instruction
            let first_instruction = &recipe.item.instructions[0];

            if let Some(asset_idx) =
                find_available_station(&self.stations, &first_instruction.required_station)
            {
                // Mark asset as in use
                self.stations[asset_idx].status = StationStatus::InUse(recipe_id.clone());

                // Add recipe to in-progress with first instruction
                self.in_progress.insert(
                    recipe_id,
                    OrderProgress {
                        recipe,
                        status: OrderLineStatus::InProgress(0, ctx.current_time()),
                    },
                );

                true
            } else {
                // Can't start the recipe yet, put it back in the queue
                self.queue.push_front(recipe);
                false
            }
        } else {
            false
        }
    }

    pub fn simulation_step(&mut self, ctx: &State) {
        let time_step = ctx.time_step();
        self.simulation_time += time_step;

        // Try to start new recipes if possible
        while self.start_order_line(ctx) {}

        // Process in-progress recipes
        let mut completed_recipe_ids = Vec::new();
        let mut to_update = Vec::new();

        for (recipe_id, progress) in self.in_progress.iter_mut() {
            match &progress.status {
                OrderLineStatus::InProgress(instruction_idx, stated_time) => {
                    let expected_duration = progress.recipe.item.instructions[*instruction_idx]
                        .expected_duration
                        .map(|duration| duration.seconds)
                        .unwrap_or(0);

                    // Check if the recipe will be completed within the current time step
                    if (ctx.next_time() - stated_time).num_seconds() < expected_duration {
                        continue;
                    }

                    // We finished to current step, so release the current asset
                    let current_instruction = &progress.recipe.item.instructions[*instruction_idx];
                    release_station(
                        &mut self.stations,
                        &current_instruction.required_station,
                        recipe_id,
                    );

                    // Move to next instruction
                    let next_idx = instruction_idx + 1;
                    if next_idx >= progress.recipe.item.instructions.len() {
                        // Recipe is complete
                        completed_recipe_ids.push(recipe_id.clone());
                        continue;
                    }

                    // Move the order to the next station, or block if not available
                    let next_instruction = &progress.recipe.item.instructions[next_idx];
                    if let Some(asset_idx) =
                        find_available_station(&self.stations, &next_instruction.required_station)
                    {
                        self.stations[asset_idx].status = StationStatus::InUse(recipe_id.clone());
                        to_update.push((
                            recipe_id.clone(),
                            OrderLineStatus::InProgress(next_idx, ctx.next_time()),
                        ));
                    } else {
                        to_update.push((recipe_id.clone(), OrderLineStatus::Blocked(next_idx)));
                    }
                }
                OrderLineStatus::Blocked(instruction_idx) => {
                    // Check if we can now acquire the needed asset
                    let instruction = &progress.recipe.item.instructions[*instruction_idx];

                    if let Some(asset_idx) =
                        find_available_station(&self.stations, &instruction.required_station)
                    {
                        // Mark asset as in use
                        self.stations[asset_idx].status = StationStatus::InUse(recipe_id.clone());

                        // Update status
                        to_update.push((
                            recipe_id.clone(),
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
                self.completed.push(progress.recipe.item);
            }
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

fn find_available_station(assets: &[Station], asset_type: &i32) -> Option<usize> {
    assets.iter().position(|asset| {
        matches!(asset.status, StationStatus::Available)
            && &(asset.station_type as i32) == asset_type
    })
}

fn release_station(assets: &mut Vec<Station>, asset_type: &i32, recipe_id: &Uuid) {
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
    use super::*;

    #[test_log::test]
    fn test_kitchen_stats() {
        let mut kitchen = Kitchen::new("test-kitchen");

        kitchen.add_asset("ws1", KitchenStation::Workstation);
        kitchen.add_asset("ws2", KitchenStation::Workstation);
        kitchen.add_asset("oven", KitchenStation::Oven);
        kitchen.add_asset("stove", KitchenStation::Stove);

        let mut state = State::try_new().unwrap();

        for item in state.menu_items().values() {
            kitchen.queue_order_line(item.clone());
        }

        for _ in 0..100 {
            kitchen.simulation_step(&state);
            state.step();

            let stats = kitchen.stats();
            println!("{:?}", stats);
        }
    }
}
