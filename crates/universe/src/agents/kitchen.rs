use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tracing::{info, warn};

#[derive(Clone)]
enum AssetType {
    Workstation,
    Oven,
    Refrigerator,
}

#[derive(Clone)]
enum AssetStatus {
    Available,
    InUse(String), // Stores the recipe ID using this asset
}

#[derive(Clone)]
struct Asset {
    id: String,
    name: String,
    asset_type: AssetType,
    status: AssetStatus,
}

impl Asset {
    pub fn new(name: impl ToString, asset_type: AssetType) -> Self {
        Asset {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            asset_type,
            status: AssetStatus::Available,
        }
    }
}

#[derive(Clone)]
struct Ingredient {
    id: String,
    name: String,
}

#[derive(Clone)]
struct Instruction {
    id: String,
    name: String,
    required_asset: AssetType,
    duration: Duration, // How long this instruction takes
}

#[derive(Clone)]
struct Recipe {
    id: String,
    name: String,
    ingredients: Vec<Ingredient>,
    instructions: Vec<Instruction>,
    current_instruction: usize, // Index of the current instruction being processed
}

#[derive(Clone)]
enum RecipeStatus {
    Queued,
    InProgress(usize), // Current instruction index
    Completed,
    Blocked(usize), // Blocked at instruction index
}

#[derive(Clone)]
struct RecipeProgress {
    recipe: Recipe,
    status: RecipeStatus,
    remaining_time: Option<Duration>, // For the current instruction
}

#[derive(Clone, Debug)]
pub struct KitchenStats {
    queued_recipes: usize,
    in_progress_recipes: usize,
    completed_recipes: usize,
    available_assets: usize,
    total_assets: usize,
    simulation_time: Duration,
}

pub struct Kitchen {
    name: String,
    assets: Vec<Asset>,
    recipe_queue: VecDeque<Recipe>,
    in_progress: HashMap<String, RecipeProgress>,
    completed_recipes: Vec<Recipe>,
    simulation_time: Duration,
}

impl Kitchen {
    pub fn new(name: &str) -> Self {
        Kitchen {
            name: name.to_string(),
            assets: Vec::new(),
            recipe_queue: VecDeque::new(),
            in_progress: HashMap::new(),
            completed_recipes: Vec::new(),
            simulation_time: Duration::from_secs(0),
        }
    }

    pub fn add_asset(&mut self, name: impl ToString, asset_type: AssetType) {
        self.assets.push(Asset {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            asset_type,
            status: AssetStatus::Available,
        });
    }

    pub fn queue_recipe(&mut self, recipe: Recipe) {
        self.recipe_queue.push_back(recipe);
    }

    fn start_recipe(&mut self) -> bool {
        if let Some(recipe) = self.recipe_queue.pop_front() {
            let recipe_id = recipe.id.clone();
            warn!("starting recipe {}", recipe_id);

            // Check if we can start the first instruction
            let first_instruction = &recipe.instructions[0];

            if let Some(asset_idx) =
                find_available_asset(&self.assets, &first_instruction.required_asset)
            {
                // Mark asset as in use
                self.assets[asset_idx].status = AssetStatus::InUse(recipe_id.clone());

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
                self.recipe_queue.push_front(recipe);
                false
            }
        } else {
            false
        }
    }

    pub fn simulation_step(&mut self, time_step: Duration) {
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
                    } else {
                        // This instruction is complete
                        let recipe = &mut progress.recipe;

                        // Release the current asset
                        let current_instruction = &recipe.instructions[instruction_idx];
                        release_asset(
                            &mut self.assets,
                            &current_instruction.required_asset,
                            recipe_id,
                        );

                        // Move to next instruction
                        let next_idx = instruction_idx + 1;

                        if next_idx >= recipe.instructions.len() {
                            // Recipe is complete
                            completed_recipe_ids.push(recipe_id.clone());
                        } else {
                            // Try to acquire asset for next instruction
                            let next_instruction = &recipe.instructions[next_idx];

                            if let Some(asset_idx) =
                                find_available_asset(&self.assets, &next_instruction.required_asset)
                            {
                                // Mark asset as in use
                                self.assets[asset_idx].status =
                                    AssetStatus::InUse(recipe_id.clone());

                                // Update status
                                to_update.push((
                                    recipe_id.clone(),
                                    RecipeStatus::InProgress(next_idx),
                                    Some(next_instruction.duration),
                                ));
                            } else {
                                // Block recipe until asset becomes available
                                to_update.push((
                                    recipe_id.clone(),
                                    RecipeStatus::Blocked(next_idx),
                                    None,
                                ));
                            }
                        }
                    }
                }
            } else if let RecipeStatus::Blocked(instruction_idx) = progress.status {
                // Check if we can now acquire the needed asset
                let instruction = &progress.recipe.instructions[instruction_idx];

                if let Some(asset_idx) =
                    find_available_asset(&self.assets, &instruction.required_asset)
                {
                    // Mark asset as in use
                    self.assets[asset_idx].status = AssetStatus::InUse(recipe_id.clone());

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
                self.completed_recipes.push(progress.recipe);
            }
        }
    }

    pub fn get_stats(&self) -> KitchenStats {
        KitchenStats {
            queued_recipes: self.recipe_queue.len(),
            in_progress_recipes: self.in_progress.len(),
            completed_recipes: self.completed_recipes.len(),
            available_assets: self
                .assets
                .iter()
                .filter(|a| matches!(a.status, AssetStatus::Available))
                .count(),
            total_assets: self.assets.len(),
            simulation_time: self.simulation_time,
        }
    }
}

fn find_available_asset(assets: &[Asset], asset_type: &AssetType) -> Option<usize> {
    assets.iter().position(|asset| {
        matches!(asset.status, AssetStatus::Available)
            && std::mem::discriminant(&asset.asset_type) == std::mem::discriminant(asset_type)
    })
}

fn release_asset(assets: &mut Vec<Asset>, asset_type: &AssetType, recipe_id: &str) {
    for asset in assets {
        if std::mem::discriminant(&asset.asset_type) == std::mem::discriminant(asset_type) {
            if let AssetStatus::InUse(id) = &asset.status {
                if id == recipe_id {
                    asset.status = AssetStatus::Available;
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
            name: "Classic Burger".to_string(),
            ingredients: vec![
                Ingredient {
                    id: "beef".to_string(),
                    name: "Ground Beef".to_string(),
                },
                Ingredient {
                    id: "bun".to_string(),
                    name: "Burger Bun".to_string(),
                },
            ],
            instructions: vec![
                Instruction {
                    id: "prep".to_string(),
                    name: "Prepare ingredients".to_string(),
                    required_asset: AssetType::Workstation,
                    duration: Duration::from_secs(120), // 2 minutes
                },
                Instruction {
                    id: "cook".to_string(),
                    name: "Cook patty".to_string(),
                    required_asset: AssetType::Oven,
                    duration: Duration::from_secs(300), // 5 minutes
                },
                Instruction {
                    id: "assemble".to_string(),
                    name: "Assemble burger".to_string(),
                    required_asset: AssetType::Workstation,
                    duration: Duration::from_secs(60), // 1 minute
                },
            ],
            current_instruction: 0,
        }
    }

    #[test_log::test]
    fn test_kitchen_stats() {
        let mut kitchen = Kitchen::new("test-kitchen");

        kitchen.add_asset("ws1", AssetType::Workstation);
        kitchen.add_asset("ws2", AssetType::Workstation);
        kitchen.add_asset("oven", AssetType::Oven);

        let stats = kitchen.get_stats();
        assert_eq!(stats.available_assets, 3);
        assert_eq!(stats.total_assets, 3);

        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());
        kitchen.queue_recipe(get_recipe());

        for _ in 0..100 {
            kitchen.simulation_step(Duration::from_secs(60)); // 1 minute per step

            // Print status
            let stats = kitchen.get_stats();
            println!("stats: {:?}", stats);
        }
    }
}
