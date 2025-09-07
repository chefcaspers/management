use std::collections::HashMap;

use arrow_array::RecordBatch;

use crate::SiteSetup;
use crate::error::Result;
use crate::idents::BrandId;
use crate::models::Brand;
use crate::state::ObjectDataBuilder;

pub(crate) fn generate_objects(
    brands: &HashMap<BrandId, Brand>,
    sites: impl IntoIterator<Item = SiteSetup>,
) -> Result<RecordBatch> {
    let mut builder = ObjectDataBuilder::new();

    for (brand_id, brand) in brands.iter() {
        builder.append_brand(brand_id, brand);
    }

    for site in sites {
        builder.append_site_info(&site)?;
    }

    builder.finish()
}
