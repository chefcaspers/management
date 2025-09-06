use caspers_universe::Site;
use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Site>()?;

    Ok(())
}
