use clap::{Args, Parser, Subcommand, ValueEnum};

use caspers_universe::{Result, SimulationMode};

use crate::{init::InitArgs, run::RunArgs};

mod error;
mod init;
mod run;
mod telemetry;

#[derive(clap::Parser)]
#[command(name = "caspers-universe", version, about = "Running Caspers Universe", long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[clap(flatten)]
    global_opts: GlobalOpts,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Args)]
struct GlobalOpts {
    /// Server URL
    #[clap(
        long,
        global = true,
        env = "UC_SERVER_URL",
        default_value = "http://localhost:8080"
    )]
    server: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a simulation
    Run(RunArgs),
    /// Initialize a simulation setup
    Init(InitArgs),
}

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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    telemetry::init_tracer_provider();
    let _guard = telemetry::init_tracing_subscriber();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run(args) => run::handle(args).await?,
        Commands::Init(args) => init::handle(args).await?,
    }

    Ok(())
}
