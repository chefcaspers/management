pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Universe {
        #[from]
        source: caspers_universe::Error,
    },
    #[error(transparent)]
    Dialogue {
        #[from]
        source: dialoguer::Error,
    },
}
