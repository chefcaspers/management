use clap::{Args, Parser, Subcommand, ValueEnum};

use caspers_universe::{Result, SimulationMode, run_simulation};

use crate::init::{InitArgs, resolve_url};

mod error;
mod init;
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

#[derive(Debug, Clone, clap::Parser)]
struct RunArgs {
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    telemetry::init_tracer_provider();
    let _guard = telemetry::init_tracing_subscriber();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run(args) => {
            let working_directory = resolve_url(args.working_directory)?;
            let snapshots_location = working_directory.join("snapshots/")?;
            let routing_location = working_directory.join("routing/")?;
            run_simulation(
                args.duration,
                snapshots_location,
                routing_location,
                args.dry_run,
            )
            .await?;
        }
        Commands::Init(args) => init::handle(args).await?,
    }

    Ok(())
}
