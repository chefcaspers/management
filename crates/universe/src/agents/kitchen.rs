use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tracing::debug;
use uuid::Uuid;

use crate::models::{IngredientQuantity, KitchenStation, MenuItemRef};
use crate::simulation::{Entity, State};

#[derive(Clone)]
enum StationStatus {
    // Station is available for use
    Available,

    // Stores the recipe ID using this station
    InUse(String),

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

struct Order {
    id: Uuid,
    items: Vec<MenuItemRef>,
}

#[derive(Clone)]
struct Instruction {
    id: String,
    name: String,
    required_station: KitchenStation,
    duration: Duration, // How long this instruction takes
}

#[derive(Clone)]
struct Recipe {
    id: String,
    ingredients: Vec<IngredientQuantity>,
    instructions: Vec<Instruction>,
}

#[derive(Clone)]
enum RecipeStatus {
    Queued,
    // Current instruction index
    InProgress(usize),
    Completed,
    // Blocked at instruction index
    Blocked(usize),
}

#[derive(Clone)]
struct RecipeProgress {
    // The recipe being processed
    recipe: Recipe,

    // The processing status of the recipe
    status: RecipeStatus,

    // Remaining processing time for the current instruction
    remaining_time: Option<Duration>,
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
    assets: Vec<Station>,
    queue: VecDeque<Recipe>,
    in_progress: HashMap<String, RecipeProgress>,
    completed: Vec<Recipe>,
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
            assets: Vec::new(),
            queue: VecDeque::new(),
            in_progress: HashMap::new(),
            completed: Vec::new(),
            simulation_time: Duration::from_secs(0),
        }
    }

    pub fn add_asset(&mut self, name: impl ToString, station_type: KitchenStation) {
        self.assets.push(Station {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            station_type,
            status: StationStatus::Available,
        });
    }

    pub fn queue_recipe(&mut self, recipe: Recipe) {
        self.queue.push_back(recipe);
    }

    fn start_recipe(&mut self) -> bool {
        if let Some(recipe) = self.queue.pop_front() {
            let recipe_id = recipe.id.clone();
            debug!("starting recipe {}", recipe_id);

            // Check if we can start the first instruction
            let first_instruction = &recipe.instructions[0];

            if let Some(asset_idx) =
                find_available_station(&self.assets, &first_instruction.required_station)
            {
                // Mark asset as in use
                self.assets[asset_idx].status = StationStatus::InUse(recipe_id.clone());

                // Add recipe to in-progress with first instruction
                self.in_progress.insert(
                    recipe_id,
                    RecipeProgress {
                        recipe: recipe.clone(),
                        status: RecipeStatus::InProgress(0),
                        remaining_time: Some(first_instruction.duration),
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
        while self.start_recipe() {}

        // Process in-progress recipes
        let mut completed_recipe_ids = Vec::new();
        let mut to_update = Vec::new();

        for (recipe_id, progress) in &mut self.in_progress {
            if let RecipeStatus::InProgress(instruction_idx) = progress.status {
                if let Some(remaining) = progress.remaining_time.as_mut() {
                    // Reduce remaining time for current instruction
                    if time_step < *remaining {
                        *remaining -= time_step;
                        continue;
                    }
                    // This instruction is complete

                    // Release the current asset
                    let current_instruction = &progress.recipe.instructions[instruction_idx];
                    release_station(
                        &mut self.assets,
                        &current_instruction.required_station,
                        recipe_id,
                    );

                    // Move to next instruction
                    let next_idx = instruction_idx + 1;
                    if next_idx >= progress.recipe.instructions.len() {
                        // Recipe is complete
                        completed_recipe_ids.push(recipe_id.clone());
                        continue;
                    }

                    // Try to acquire asset for next instruction
                    let next_instruction = &progress.recipe.instructions[next_idx];
                    if let Some(asset_idx) =
                        find_available_station(&self.assets, &next_instruction.required_station)
                    {
                        // Mark asset as in use
                        self.assets[asset_idx].status = StationStatus::InUse(recipe_id.clone());

                        // Update status
                        to_update.push((
                            recipe_id.clone(),
                            RecipeStatus::InProgress(next_idx),
                            Some(next_instruction.duration),
                        ));
                    } else {
                        // Block recipe until asset becomes available
                        to_update.push((recipe_id.clone(), RecipeStatus::Blocked(next_idx), None));
                    }
                }
            } else if let RecipeStatus::Blocked(instruction_idx) = progress.status {
                // Check if we can now acquire the needed asset
                let instruction = &progress.recipe.instructions[instruction_idx];

                if let Some(asset_idx) =
                    find_available_station(&self.assets, &instruction.required_station)
                {
                    // Mark asset as in use
                    self.assets[asset_idx].status = StationStatus::InUse(recipe_id.clone());

                    // Update status
                    to_update.push((
                        recipe_id.clone(),
                        RecipeStatus::InProgress(instruction_idx),
                        Some(instruction.duration),
                    ));
                }
            }
        }

        // Apply updates
        for (recipe_id, status, remaining_time) in to_update {
            if let Some(progress) = self.in_progress.get_mut(&recipe_id) {
                progress.status = status;
                progress.remaining_time = remaining_time;
            }
        }

        // Move completed recipes
        for recipe_id in completed_recipe_ids {
            if let Some(progress) = self.in_progress.remove(&recipe_id) {
                self.completed.push(progress.recipe);
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
                .assets
                .iter()
                .filter(|a| matches!(a.status, StationStatus::Available))
                .count(),
            total_assets: self.assets.len(),
            simulation_time: self.simulation_time,
        }
    }
}

fn find_available_station(assets: &[Station], asset_type: &KitchenStation) -> Option<usize> {
    assets.iter().position(|asset| {
        matches!(asset.status, StationStatus::Available)
            && std::mem::discriminant(&asset.station_type) == std::mem::discriminant(asset_type)
    })
}

fn release_station(assets: &mut Vec<Station>, asset_type: &KitchenStation, recipe_id: &str) {
    for asset in assets {
        if std::mem::discriminant(&asset.station_type) == std::mem::discriminant(asset_type) {
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

    fn get_recipe() -> Recipe {
        // get a random recipe id
        let recipe_id = format!("recipe-{}", rand::random::<u32>());
        Recipe {
            id: recipe_id,
            ingredients: vec![
                IngredientQuantity {
                    ingredient_ref: "ingredients/beef".to_string(),
                    quantity: "130g".to_string(),
                },
                IngredientQuantity {
                    ingredient_ref: "ingredients/bun".to_string(),
                    quantity: "150g".to_string(),
                },
            ],
            instructions: vec![
                Instruction {
                    id: "prep".to_string(),
                    name: "Prepare ingredients".to_string(),
                    required_station: KitchenStation::Workstation,
                    duration: Duration::from_secs(120), // 2 minutes
                },
                Instruction {
                    id: "cook".to_string(),
                    name: "Cook patty".to_string(),
                    required_station: KitchenStation::Oven,
                    duration: Duration::from_secs(300), // 5 minutes
                },
                Instruction {
                    id: "assemble".to_string(),
                    name: "Assemble burger".to_string(),
                    required_station: KitchenStation::Workstation,
                    duration: Duration::from_secs(60), // 1 minute
                },
            ],
        }
    }

    #[test_log::test]
    fn test_kitchen_stats() {
        let mut kitchen = Kitchen::new("test-kitchen");

        kitchen.add_asset("ws1", KitchenStation::Workstation);
        kitchen.add_asset("ws2", KitchenStation::Workstation);
        kitchen.add_asset("oven", KitchenStation::Oven);

        let stats = kitchen.stats();
        assert_eq!(stats.idle_assets, 3);
        assert_eq!(stats.total_assets, 3);

        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());

        let state = State::try_new().unwrap();

        for _ in 0..100 {
            kitchen.simulation_step(&state);

            // Print status
            let stats = kitchen.stats();
            println!("{:?}", stats);
        }
    }
}
