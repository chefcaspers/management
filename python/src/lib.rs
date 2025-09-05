use pyo3::prelude::*;

#[pyclass(get_all, set_all)]
struct Site {
    name: String,
    latitude: f64,
    longitude: f64,
}

#[pymethods]
impl Site {
    #[new]
    #[pyo3(signature = (name, latitude, longitude))]
    fn new(name: String, latitude: f64, longitude: f64) -> Self {
        Site {
            name,
            latitude,
            longitude,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Site(name={}, latitude={}, longitude={})",
            self.name, self.latitude, self.longitude
        )
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Site>()?;

    Ok(())
}
