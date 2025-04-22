use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use uuid::Uuid;

use crate::error::Result;
use crate::idents::{BrandId, MenuItemId};
use crate::models::MenuItem;

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

pub(crate) struct VendorData {
    pub brands: RecordBatch,
    // recipes: RecordBatch,
    brand_slices: HashMap<BrandId, (usize, usize)>,
    menu_items: Arc<DashMap<MenuItemId, MenuItem>>,
}

impl VendorData {
    /// Record batch MUST be sorted by parent_id.
    pub fn try_new(brands: RecordBatch) -> Result<Self> {
        let mut brand_slices = HashMap::new();

        for (idx, brand_id) in brands
            .column_by_name("parent_id")
            .unwrap()
            .as_fixed_size_binary()
            .iter()
            .enumerate()
        {
            if let Some(id) = brand_id {
                let typed: BrandId = Uuid::from_slice(id)?.into();
                brand_slices.entry(typed).or_insert_with(Vec::new);
                brand_slices.get_mut(&typed).unwrap().push(idx);
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
            brands,
            // recipes: RecordBatch::new_empty(crate::simulation::schemas::BRAND_SCHEMA.clone()),
            brand_slices,
            menu_items: Arc::new(DashMap::new()),
        })
    }

    pub(crate) fn brand(&self, brand_id: &BrandId) -> Result<BrandData> {
        let (offset, length) = *self
            .brand_slices
            .get(brand_id)
            .ok_or(VendorDataError::BrandNotFound)?;
        Ok(BrandData {
            data: self.brands.slice(offset, length),
            menu_items: HashMap::new(),
        })
    }

    pub(crate) fn menu_item(
        &self,
        item_id: &(BrandId, MenuItemId),
    ) -> Result<Ref<'_, MenuItemId, MenuItem>> {
        if let Some(item) = self.menu_items.get(&item_id.1) {
            return Ok(item);
        }

        let ids = self
            .brands
            .column_by_name("id")
            .ok_or(VendorDataError::ColumnNotFound("id"))?
            .as_fixed_size_binary();

        let parent_ids = self
            .brands
            .column_by_name("parent_id")
            .ok_or(VendorDataError::ColumnNotFound("parent_id"))?
            .as_fixed_size_binary();

        for (index, item_ids) in ids.iter().zip(parent_ids.iter()).enumerate() {
            if let (Some(id), Some(parent_id)) = item_ids {
                if AsRef::<[u8]>::as_ref(&item_id.0) == parent_id
                    && AsRef::<[u8]>::as_ref(&item_id.1) == id
                {
                    let raw = self
                        .brands
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
}

pub struct BrandData {
    data: RecordBatch,
    menu_items: HashMap<MenuItemId, MenuItem>,
}
