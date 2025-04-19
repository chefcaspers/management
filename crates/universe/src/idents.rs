use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SiteId(Uuid);

impl SiteId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        SiteId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl From<Uuid> for SiteId {
    fn from(id: Uuid) -> Self {
        SiteId(id)
    }
}

impl AsRef<Uuid> for SiteId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl AsRef<[u8]> for SiteId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for SiteId {
    fn to_string(&self) -> String {
        self.0.to_string()
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

impl AsRef<[u8]> for KitchenId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for KitchenId {
    fn to_string(&self) -> String {
        self.0.to_string()
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

impl AsRef<[u8]> for StationId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for StationId {
    fn to_string(&self) -> String {
        self.0.to_string()
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

impl AsRef<[u8]> for OrderId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for OrderId {
    fn to_string(&self) -> String {
        self.0.to_string()
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

impl AsRef<[u8]> for OrderLineId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for OrderLineId {
    fn to_string(&self) -> String {
        self.0.to_string()
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

impl AsRef<[u8]> for BrandId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for BrandId {
    fn to_string(&self) -> String {
        self.0.to_string()
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

impl AsRef<[u8]> for MenuItemId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for MenuItemId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PersonId(pub(crate) Uuid);

impl PersonId {
    pub fn new() -> Self {
        PersonId(Uuid::new_v4())
    }
}

impl From<Uuid> for PersonId {
    fn from(uuid: Uuid) -> Self {
        PersonId(uuid)
    }
}

impl AsRef<Uuid> for PersonId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl AsRef<[u8]> for PersonId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl ToString for PersonId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}
