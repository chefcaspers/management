use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LocationId(Uuid);

impl LocationId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        LocationId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl From<Uuid> for LocationId {
    fn from(id: Uuid) -> Self {
        LocationId(id)
    }
}

impl AsRef<Uuid> for LocationId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KitchenId(Uuid);

impl From<Uuid> for KitchenId {
    fn from(uuid: Uuid) -> Self {
        KitchenId(uuid)
    }
}

impl KitchenId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        KitchenId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl AsRef<Uuid> for KitchenId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StationId(Uuid);

impl StationId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        StationId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl From<Uuid> for StationId {
    fn from(uuid: Uuid) -> Self {
        StationId(uuid)
    }
}

impl AsRef<Uuid> for StationId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OrderId(Uuid);

impl OrderId {
    pub fn new() -> Self {
        OrderId(Uuid::now_v7())
    }
}

impl From<Uuid> for OrderId {
    fn from(id: Uuid) -> Self {
        OrderId(id)
    }
}

impl AsRef<Uuid> for OrderId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OrderLineId(Uuid);

impl OrderLineId {
    pub fn new() -> Self {
        OrderLineId(Uuid::now_v7())
    }
}

impl From<Uuid> for OrderLineId {
    fn from(id: Uuid) -> Self {
        OrderLineId(id)
    }
}

impl AsRef<Uuid> for OrderLineId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BrandId(Uuid);

impl BrandId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        BrandId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl From<Uuid> for BrandId {
    fn from(uuid: Uuid) -> Self {
        BrandId(uuid)
    }
}

impl AsRef<Uuid> for BrandId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MenuItemId(Uuid);

impl MenuItemId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        MenuItemId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl From<Uuid> for MenuItemId {
    fn from(uuid: Uuid) -> Self {
        MenuItemId(uuid)
    }
}

impl AsRef<Uuid> for MenuItemId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}
