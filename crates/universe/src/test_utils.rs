use std::{path::PathBuf, process::Command};

use crate::{Error, Result, Simulation, Template};

pub async fn setup_test_simulation(template: impl Into<Option<Template>>) -> Result<Simulation> {
    let caspers_root = find_git_root()?.join(".caspers/system/");
    let system_path = url::Url::from_directory_path(caspers_root)
        .map_err(|_| Error::internal("invalid directory"))?;
    Simulation::try_new_with_template(template.into().unwrap_or_default(), &system_path).await
}

pub fn find_git_root() -> Result<PathBuf> {
    let command = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()
        .map_err(Error::from)?;

    if !command.status.success() {
        return Err(Error::invalid_data("no git root found"));
    }

    let output = String::from_utf8(command.stdout).unwrap();
    Ok(std::fs::canonicalize(output.trim())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simulation() {
        let mut simulation = setup_test_simulation(None).await.unwrap();

        let event_stats = simulation.event_stats();
        assert_eq!(event_stats.num_orders_created, 0);

        simulation.run(100).await.unwrap();

        let event_stats = simulation.event_stats();
        assert!(event_stats.num_orders_created > 0);
    }
}
