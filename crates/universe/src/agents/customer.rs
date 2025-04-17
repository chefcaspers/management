use chrono::{DateTime, Timelike, Utc};
use uuid::Uuid;

use crate::simulation::{Entity, Location, Simulatable, SimulationContext};

pub enum CustomerActions {
    Register,
    PlaceOrder,
    CancelOrder,
    ProvideFeedback,
}

enum CustomerState {
    Idle,
    Waiting(DateTime<Utc>),
    Moving(Location),
}

pub struct Customer {
    id: Uuid,
    name: String,
    location: Location,
    hunger: f64,
    state: CustomerState,
}

impl Customer {
    pub fn new(name: impl Into<String>) -> Self {
        Customer {
            id: Uuid::new_v4(),
            name: name.into(),
            location: Location::new(0.0, 0.0),
            hunger: 0.0,
            state: CustomerState::Idle,
        }
    }

    fn action(&self, ctx: &SimulationContext) -> Option<CustomerActions> {
        match self.state {
            CustomerState::Idle => {
                if ctx.time.time().hour() == 11 {
                    Some(CustomerActions::PlaceOrder)
                } else {
                    None
                }
            }
            CustomerState::Waiting(_) => None,
            CustomerState::Moving(_) => None,
        }
    }
}

impl Entity for Customer {
    fn id(&self) -> Uuid {
        self.id
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl Simulatable for Customer {
    fn tick(&mut self, ctx: &SimulationContext) -> Option<()> {
        match self.action(ctx)? {
            CustomerActions::PlaceOrder => {
                self.hunger += 1.0;
                self.state = CustomerState::Waiting(ctx.local_time());
                Some(())
            }
            _ => None,
        }
    }
}
