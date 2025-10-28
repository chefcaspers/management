use arrow::array::AsArray;
use arrow::datatypes::TimestampMillisecondType;
use caspers_universe::Error as UniverseError;
use caspers_universe::{Simulation, SimulationContext, SimulationMode, resolve_url};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use dialoguer::Select;

use crate::error::Result;

/// Execution mode for the simulation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum SimulationModeCli {
    /// Run the simulation for the specified time horizon.
    Backfill,
    /// Align time passed in simulation with time passed in real time.
    Realtime,
    /// Continue simulation from last snapshot up to current time, then switch to real time.
    Catchup,
}

impl From<SimulationModeCli> for SimulationMode {
    fn from(value: SimulationModeCli) -> Self {
        match value {
            SimulationModeCli::Backfill => SimulationMode::Backfill,
            SimulationModeCli::Realtime => SimulationMode::Realtime,
            SimulationModeCli::Catchup => SimulationMode::Catchup,
        }
    }
}

#[derive(Debug, Clone, clap::Parser)]
pub(crate) struct RunArgs {
    #[arg(short, long, default_value_t = 100)]
    duration: usize,

    #[arg(short, long)]
    /// Path where basic simulation setup is stored.
    working_directory: Option<String>,

    #[arg(short, long, value_enum, default_value_t = SimulationModeCli::Backfill)]
    mode: SimulationModeCli,

    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

pub(super) async fn handle(args: RunArgs) -> Result<()> {
    let caspers_directory = resolve_url(args.working_directory)?;
    let mut builder =
        SimulationContext::builder().with_working_directory(caspers_directory.clone());

    let simulations = builder
        .load_simulations()
        .await?
        .select_columns(&["id"])
        .map_err(UniverseError::from)?
        .collect()
        .await
        .map_err(UniverseError::from)?;

    let selections = (0..simulations.len())
        .flat_map(|idx| simulations[idx].column(0).as_string_view().iter())
        .flatten()
        .collect::<Vec<_>>();

    let Some(sim_selection) = Select::new()
        .with_prompt("Which simulation to run?")
        .items(&selections)
        .interact_opt()?
    else {
        return Ok(());
    };

    let simulation_id =
        uuid::Uuid::try_parse(selections[sim_selection]).map_err(UniverseError::from)?;
    builder = builder.with_simulation_id(simulation_id);

    let snapshots = builder
        .load_snapshots()
        .await?
        .select_columns(&["id", "simulation_time"])
        .map_err(UniverseError::from)?
        .collect()
        .await
        .map_err(UniverseError::from)?;

    let selections = (0..snapshots.len())
        .flat_map(|idx| snapshots[idx].column(0).as_string_view().iter())
        .flatten()
        .collect::<Vec<_>>();

    let Some(sn_selection) = Select::new()
        .with_prompt("Which snapshot to start from?")
        .items(&selections)
        .interact_opt()?
    else {
        return Ok(());
    };

    let snapshot_id =
        uuid::Uuid::try_parse(selections[sn_selection]).map_err(UniverseError::from)?;
    let ctx = builder.with_snapshot_id(snapshot_id).build().await?;

    let start_time = snapshots[sn_selection]
        .column(1)
        .as_primitive::<TimestampMillisecondType>()
        .value(sn_selection);
    let start_time = DateTime::<Utc>::from_timestamp_millis(start_time).expect("Invalid timestamp");

    let mut simulation = Simulation::builder()
        .with_context(ctx)
        .with_dry_run(args.dry_run)
        .with_start_time(start_time)
        .build()
        .await?;

    simulation.run(args.duration).await?;

    Ok(())
}
