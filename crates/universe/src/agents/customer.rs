use chrono::{DateTime, Timelike, Utc};

use crate::Event;
use crate::idents::PersonId;
use crate::{Entity, Simulatable, State, error::Result};

pub enum CustomerActions {
    Register,
    PlaceOrder,
    CancelOrder,
    ProvideFeedback,
}

enum CustomerState {
    Idle,
    Waiting(DateTime<Utc>),
}

pub struct Customer {
    id: PersonId,
    hunger: f64,
    state: CustomerState,
}

impl Customer {
    pub fn new() -> Self {
        Customer {
            id: PersonId::new(),
            hunger: 0.0,
            state: CustomerState::Idle,
        }
    }

    fn action(&self, ctx: &State) -> Option<CustomerActions> {
        match self.state {
            CustomerState::Idle => {
                if ctx.current_time().hour() == 11 {
                    Some(CustomerActions::PlaceOrder)
                } else {
                    None
                }
            }
            CustomerState::Waiting(_) => None,
        }
    }
}

impl Entity for Customer {
    type Id = PersonId;

    fn id(&self) -> &PersonId {
        &self.id
    }
}

impl Simulatable for Customer {
    fn step(&mut self, ctx: &State) -> Result<Vec<Event>> {
        if let Some(CustomerActions::PlaceOrder) = self.action(ctx) {
            self.hunger += 1.0;
            self.state = CustomerState::Waiting(ctx.current_time());
        };
        Ok(vec![])
    }
}
