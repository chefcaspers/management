use chrono::{DateTime, Timelike, Utc};

use crate::idents::PersonId;
use crate::{Entity, Simulatable, State};

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
    name: String,
    hunger: f64,
    state: CustomerState,
}

impl Customer {
    pub fn new(name: impl Into<String>) -> Self {
        Customer {
            id: PersonId::new(),
            name: name.into(),
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

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl Simulatable for Customer {
    fn step(&mut self, ctx: &State) -> Option<()> {
        match self.action(ctx)? {
            CustomerActions::PlaceOrder => {
                self.hunger += 1.0;
                self.state = CustomerState::Waiting(ctx.current_time());
                Some(())
            }
            _ => None,
        }
    }
}
