use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use itertools::Itertools;
use uuid::Uuid;

use crate::error::Result;
use crate::idents::{BrandId, KitchenId, MenuItemId, SiteId, StationId};
use crate::init::ObjectLabel;
use crate::models::{MenuItem, Station};

#[derive(Debug, thiserror::Error)]
enum VendorDataError {
    #[error("Brand not found")]
    BrandNotFound,

    #[error("Menu item not found")]
    MenuItemNotFound,

    #[error("Inconsistent data")]
    InconsistentData,

    #[error("Column not found")]
    ColumnNotFound(&'static str),
}

pub struct ObjectData {
    objects: RecordBatch,

    // Map of brand_id to (offset, length) slice in the objects record batch
    brand_slices: HashMap<BrandId, (usize, usize)>,

    // kitchen_slices: HashMap<KitchenId, (usize, usize)>,
    menu_items: Arc<DashMap<MenuItemId, MenuItem>>,
}

impl ObjectData {
    /// Record batch MUST be sorted by parent_id.
    pub fn try_new(objects: RecordBatch) -> Result<Self> {
        let mut brand_slices = HashMap::new();

        for (idx, brand_id) in objects
            .column_by_name("parent_id")
            .unwrap()
            .as_fixed_size_binary()
            .iter()
            .enumerate()
        {
            if let Some(id) = brand_id {
                let typed: BrandId = Uuid::from_slice(id)?.into();
                brand_slices.entry(typed).or_insert_with(Vec::new).push(idx);
            }
        }

        let brand_slices = brand_slices
            .into_iter()
            .map(|(id, mut indices)| {
                indices.sort();
                // safety: we always push at least one item
                (id, (*indices.iter().min().unwrap(), indices.len()))
            })
            .collect();

        Ok(Self {
            objects,
            brand_slices,
            menu_items: Arc::new(DashMap::new()),
        })
    }

    pub fn objects(&self) -> &RecordBatch {
        &self.objects
    }

    pub(crate) fn brand(&self, brand_id: &BrandId) -> Result<BrandData> {
        let (offset, length) = *self
            .brand_slices
            .get(brand_id)
            .ok_or(VendorDataError::BrandNotFound)?;
        Ok(BrandData {
            data: self.objects.slice(offset, length),
            menu_items: HashMap::new(),
        })
    }

    fn iter_ids(
        &self,
    ) -> Result<impl Iterator<Item = (Option<&[u8]>, Option<&[u8]>, Option<&str>)>> {
        let ids = self
            .objects
            .column_by_name("id")
            .ok_or(VendorDataError::ColumnNotFound("id"))?
            .as_fixed_size_binary();

        let parent_ids = self
            .objects
            .column_by_name("parent_id")
            .ok_or(VendorDataError::ColumnNotFound("parent_id"))?
            .as_fixed_size_binary();

        let labels = self
            .objects
            .column_by_name("label")
            .ok_or(VendorDataError::ColumnNotFound("label"))?
            .as_string::<i32>();

        Ok(ids
            .iter()
            .zip(parent_ids.iter())
            .zip(labels.iter())
            .map(|((id, parent_id), label)| (id, parent_id, label)))
    }

    pub(crate) fn menu_item(
        &self,
        item_id: &(BrandId, MenuItemId),
    ) -> Result<Ref<'_, MenuItemId, MenuItem>> {
        if let Some(item) = self.menu_items.get(&item_id.1) {
            return Ok(item);
        }

        for (index, item_ids) in self.iter_ids()?.enumerate() {
            if let (Some(id), Some(parent_id), _) = item_ids {
                if AsRef::<[u8]>::as_ref(&item_id.0) == parent_id
                    && AsRef::<[u8]>::as_ref(&item_id.1) == id
                {
                    let raw = self
                        .objects
                        .column_by_name("properties")
                        .ok_or(VendorDataError::ColumnNotFound("properties"))?
                        .as_string::<i32>();
                    let value = raw.value(index);
                    let properties: MenuItem = serde_json::from_str(value)?;
                    self.menu_items.insert(item_id.1, properties.clone());
                    return Ok(self.menu_items.get(&item_id.1).unwrap());
                }
            }
        }

        Err(VendorDataError::MenuItemNotFound.into())
    }

    pub(crate) fn sites(&self) -> Result<impl Iterator<Item = SiteId>> {
        Ok(self.iter_ids()?.filter_map(|(id, _parent_id, label)| {
            id.and_then(|id| {
                (label == Some(ObjectLabel::Site.as_ref()))
                    .then(|| uuid::Uuid::from_slice(id).unwrap().into())
            })
        }))
    }

    pub(crate) fn kitchens(
        &self,
        site_id: &SiteId,
    ) -> Result<impl Iterator<Item = Result<(KitchenId, Vec<BrandId>)>>> {
        let brands: Vec<_> = self
            .iter_ids()?
            .filter_map(|(id, _, label)| {
                (label == Some(ObjectLabel::Brand.as_ref()) && id.is_some())
                    .then(|| uuid::Uuid::from_slice(id.unwrap()).map(|id| id.into()))
            })
            .try_collect()?;
        Ok(self.iter_ids()?.filter_map(move |(id, parent_id, label)| {
            id.and_then(|id| {
                (parent_id == Some(site_id.as_ref())
                    && label == Some(ObjectLabel::Kitchen.as_ref()))
                .then(|| Ok((uuid::Uuid::from_slice(id)?.into(), brands.clone())))
            })
        }))
    }

    pub(crate) fn kitchen_stations(
        &self,
        kitchen_id: &KitchenId,
    ) -> Result<impl Iterator<Item = Result<(StationId, Station)>>> {
        let properties = self
            .objects
            .column_by_name("properties")
            .ok_or(VendorDataError::ColumnNotFound("properties"))?
            .as_string::<i32>();
        Ok(self.iter_ids()?.zip(properties.iter()).filter_map(
            |((id, parent_id, label), properties)| {
                id.and_then(|id| {
                    (parent_id == Some(kitchen_id.as_ref())
                        && label == Some(ObjectLabel::Station.as_ref()))
                    .then(|| {
                        Ok((
                            uuid::Uuid::from_slice(id)?.into(),
                            serde_json::from_str(
                                properties.ok_or(VendorDataError::InconsistentData)?,
                            )?,
                        ))
                    })
                })
            },
        ))
    }
}

pub struct BrandData {
    data: RecordBatch,
    menu_items: HashMap<MenuItemId, MenuItem>,
}
