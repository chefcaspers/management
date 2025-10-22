use std::{collections::HashMap, sync::OnceLock};

use caspers_universe::{
    SimulationSetup, Site, load_simulation_setup as load_simulation,
    run_simulation as run_simulation_inner,
};
use pyo3::{exceptions::PyValueError, prelude::*};
use tokio::runtime::Runtime;
use url::Url;

use crate::error::Error;

mod error;

#[inline]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    static PID: OnceLock<u32> = OnceLock::new();

    let pid = std::process::id();
    let runtime_pid = *PID.get_or_init(|| pid);
    if pid != runtime_pid {
        panic!(
            "Forked process detected - current PID is {pid} but the tokio runtime was created by {runtime_pid}. The tokio \
            runtime does not support forked processes https://github.com/tokio-rs/tokio/issues/4301. If you are \
            seeing this message while using Python multithreading make sure to use the `spawn` or `forkserver` \
            mode.",
        );
    }

    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}

#[pyfunction]
#[pyo3(signature = (path, options = None))]
fn load_simulation_setup(
    path: String,
    options: Option<HashMap<String, String>>,
) -> PyResult<SimulationSetup> {
    let url = url::Url::parse(&path)
        .map_err(|_| PyValueError::new_err(format!("failed to parse url: {path}")))?;
    let setup = rt()
        .block_on(load_simulation(&url, options.unwrap_or_default()))
        .map_err(Error::from)?;
    Ok(setup)
}

#[pyfunction]
#[pyo3(signature = (setup, duration, output_location, routing_location, dry_run = false))]
fn run_simulation(
    setup: SimulationSetup,
    duration: usize,
    output_location: String,
    routing_location: String,
    dry_run: bool,
) -> PyResult<()> {
    let output_location = resolve_url(&output_location)?;
    let routing_location = resolve_url(&routing_location)?;
    let setup = rt()
        .block_on(run_simulation_inner(
            setup,
            duration,
            output_location,
            routing_location,
            dry_run,
        ))
        .map_err(Error::from)?;
    Ok(setup)
}

fn resolve_url(url: &str) -> PyResult<Url> {
    match url::Url::parse(url) {
        Ok(url) => Ok(url),
        Err(_) => {
            let path = std::fs::canonicalize(url)?;
            Ok(Url::from_directory_path(path).unwrap())
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Site>()?;

    m.add_function(wrap_pyfunction!(load_simulation_setup, m)?)?;
    m.add_function(wrap_pyfunction!(run_simulation, m)?)?;

    Ok(())
}
