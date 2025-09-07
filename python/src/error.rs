use arrow::error::ArrowError;
use caspers_universe::Error as InnerError;
use datafusion::error::DataFusionError;
use object_store::Error as ObjectStoreError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::{
    PyException, PyFileNotFoundError, PyIOError, PyNotImplementedError, PyValueError,
};
use pyo3::{PyErr, create_exception};
use std::fmt::Display;

create_exception!(_internal, CaspersError, PyException);
create_exception!(_internal, TableNotFoundError, CaspersError);
create_exception!(_internal, DeltaProtocolError, CaspersError);
create_exception!(_internal, CommitFailedError, CaspersError);
create_exception!(_internal, SchemaMismatchError, CaspersError);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error in delta table")]
    Caspers(#[from] InnerError),
    #[error("Error in object store")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("Error in arrow")]
    Arrow(#[from] ArrowError),
    #[error("Error in data fusion")]
    DataFusion(#[from] DataFusionError),
}

impl From<Error> for pyo3::PyErr {
    fn from(value: Error) -> Self {
        match value {
            Error::Caspers(err) => inner_to_py_err(err),
            Error::ObjectStore(err) => object_store_to_py(err),
            Error::Arrow(err) => arrow_to_py(&err),
            Error::DataFusion(err) => datafusion_to_py(err),
        }
    }
}

fn inner_to_py_err(err: InnerError) -> PyErr {
    match err {
        InnerError::ObjectStore { source } => object_store_to_py(source),
        InnerError::Arrow { source } => arrow_to_py(&source),
        InnerError::Datafusion { source } => datafusion_to_py(source),
        _ => CaspersError::new_err(err.to_string()),
    }
}

fn datafusion_to_py(err: DataFusionError) -> PyErr {
    match err {
        DataFusionError::ArrowError(err, _) => arrow_to_py(&err),
        DataFusionError::External(err) => PyException::new_err(err.to_string()),
        DataFusionError::Internal(msg) => PyRuntimeError::new_err(msg),
        DataFusionError::NotImplemented(msg) => PyNotImplementedError::new_err(msg),
        DataFusionError::Plan(msg) => PyValueError::new_err(msg),
        DataFusionError::IoError(msg) => PyIOError::new_err(msg),
        DataFusionError::SchemaError(msg, _) => SchemaMismatchError::new_err(msg.to_string()),
        _ => CaspersError::new_err(err.to_string()),
    }
}

fn arrow_to_py(err: &ArrowError) -> PyErr {
    match err {
        ArrowError::IoError(msg, _) => PyIOError::new_err(msg.clone()),
        ArrowError::DivideByZero => PyValueError::new_err("division by zero"),
        ArrowError::InvalidArgumentError(msg) => PyValueError::new_err(msg.clone()),
        ArrowError::NotYetImplemented(msg) => PyNotImplementedError::new_err(msg.clone()),
        ArrowError::SchemaError(msg) => SchemaMismatchError::new_err(msg.clone()),
        other => PyException::new_err(other.to_string()),
    }
}

fn object_store_to_py(err: ObjectStoreError) -> PyErr {
    match err {
        ObjectStoreError::NotFound { .. } => PyFileNotFoundError::new_err(
            DisplaySourceChain {
                err,
                error_name: "FileNotFoundError".to_string(),
            }
            .to_string(),
        ),
        ObjectStoreError::Generic { source, .. }
            if source.to_string().contains("AWS_S3_ALLOW_UNSAFE_RENAME") =>
        {
            DeltaProtocolError::new_err(source.to_string())
        }
        _ => PyIOError::new_err(
            DisplaySourceChain {
                err,
                error_name: "IOError".to_string(),
            }
            .to_string(),
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DisplaySourceChain<T> {
    err: T,
    error_name: String,
}

impl<T: std::error::Error + 'static> Display for DisplaySourceChain<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // walk the source chain and collect error messages
        let mut err_msgs = Vec::new();
        let mut current_err = Some(&self.err as &(dyn std::error::Error + 'static));
        while let Some(err) = current_err {
            let err_msg = err.to_string();
            err_msgs.push(err_msg);
            current_err = err.source();
        }
        // produce output message parts from source error messages
        // message parts are delimited by the substring ": "
        let mut out_parts = Vec::with_capacity(err_msgs.capacity());
        for err_msg in &err_msgs {
            // not very clean but std lib doesn't easily support splitting on two substrings
            for err_part in err_msg.split(": ").flat_map(|s| s.split("\ncaused by\n")) {
                if !err_part.is_empty()
                    && !out_parts.contains(&err_part)
                    && !out_parts.iter().any(|p| p.contains(err_part))
                {
                    out_parts.push(err_part);
                }
            }
        }
        for (i, part) in out_parts.iter().enumerate() {
            if i == 0 {
                writeln!(f, "{part}")?;
            } else {
                writeln!(
                    f,
                    "{}\x1b[31m↳\x1b[0m {}",
                    " ".repeat(self.error_name.len() + ": ".len() + i),
                    part
                )?;
            }
        }
        Ok(())
    }
}
