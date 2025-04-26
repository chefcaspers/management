pub type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Not found")]
    NotFound,

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Missing geometry")]
    MissingGeometry,

    #[error("Invalid geometry: {0}")]
    InvalidGeometry(String),

    #[error("Generic error: {0}")]
    Generic(Box<dyn std::error::Error>),
}

impl Error {
    pub fn invalid_data(message: impl ToString) -> Self {
        Error::InvalidData(message.to_string())
    }

    pub fn invalid_geometry(message: impl ToString) -> Self {
        Error::InvalidGeometry(message.to_string())
    }

    pub fn generic(error: impl std::error::Error + 'static) -> Self {
        Error::Generic(Box::new(error))
    }
}
