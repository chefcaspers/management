use std::collections::HashMap;

use uuid::Uuid;

pub use state::State;

pub mod execution;
mod state;

pub struct Movement(f64, f64);
pub struct GeoLocation(f64, f64);

impl GeoLocation {
    pub fn new(long: f64, lat: f64) -> Self {
        GeoLocation(long, lat)
    }

    pub fn distance(&self, other: &GeoLocation) -> f64 {
        ((self.0 - other.0).powi(2) + (self.1 - other.1).powi(2)).sqrt()
    }
}

/// Core trait that any simulatable entity must implement
pub trait Entity: Send + Sync + 'static {
    /// Unique identifier for the entity
    fn id(&self) -> Uuid;

    /// Human-readable name of the entity
    fn name(&self) -> &str;
}

/// Trait for entities that need to be updated each simulation step
pub trait Simulatable: Entity {
    /// Update the entity state based on the current simulation context
    fn step(&mut self, context: &state::State) -> Option<()>;
}

/// Events that can occur during simulation
#[derive(Debug, Clone)]
pub enum Event {
    /// New order has been placed
    OrderPlaced {
        order_id: Uuid,
        items: Vec<(Uuid, i32)>, // (menu_item_id, quantity)
    },

    /// The status of an order has changed
    OrderStatusChanged { order_id: Uuid, status: OrderStatus },
    // Additional events can be added as needed
}

/// Possible states of an order
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    Received,
    Accepted,
    Processing,
    Ready,
    PickedUp,
    Delivered,
    Cancelled,
}

/// The main simulation engine
pub struct Simulation {
    /// Global simulation state
    pub context: state::State,

    /// All entities in the simulation, organized by their type and ID
    entities: HashMap<String, HashMap<Uuid, Box<dyn Simulatable>>>,

    /// Event handlers registered for specific event types
    event_handlers: HashMap<
        String,
        Vec<Box<dyn Fn(&Event, &mut HashMap<String, HashMap<Uuid, Box<dyn Simulatable>>>)>>,
    >,
}

impl Simulation {
    /// Create a new simulation with default parameters
    pub fn new() -> Self {
        Self {
            context: state::State::try_new().unwrap(),
            entities: HashMap::new(),
            event_handlers: HashMap::new(),
        }
    }

    /// Add an entity to the simulation
    pub fn add_entity<T: Simulatable>(&mut self, entity_type: &str, entity: T) {
        let type_map = self.entities.entry(entity_type.to_string()).or_default();

        type_map.insert(entity.id(), Box::new(entity));
    }

    /// Register an event handler for a specific event type
    pub fn register_event_handler<F>(&mut self, event_type: &str, handler: F)
    where
        F: Fn(&Event, &mut HashMap<String, HashMap<Uuid, Box<dyn Simulatable>>>) + 'static,
    {
        let handlers = self
            .event_handlers
            .entry(event_type.to_string())
            .or_default();

        handlers.push(Box::new(handler));
    }

    /// Advance the simulation by one time step
    pub fn tick(&mut self) {
        // Advance simulation time
        self.context.step();
    }

    /// Run the simulation for a specified number of steps
    pub fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.tick();
        }
    }

    /// Process a single event by calling all registered handlers
    fn process_event(&mut self, event: &Event) {
        let event_type = match event {
            Event::OrderPlaced { .. } => "OrderPlaced",
            Event::OrderStatusChanged { .. } => "OrderStatusChanged",
            // Add cases for other event types
        };

        if let Some(handlers) = self.event_handlers.get(event_type) {
            for handler in handlers {
                handler(event, &mut self.entities);
            }
        }
    }

    /// Get a reference to an entity by its type and ID
    pub fn get_entity(&self, entity_type: &str, id: &Uuid) -> Option<&Box<dyn Simulatable>> {
        self.entities.get(entity_type)?.get(id)
    }

    /// Get a mutable reference to an entity by its type and ID
    pub fn get_entity_mut(
        &mut self,
        entity_type: &str,
        id: &Uuid,
    ) -> Option<&mut Box<dyn Simulatable>> {
        self.entities.get_mut(entity_type)?.get_mut(id)
    }
}
