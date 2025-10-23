use clap::{Args, Parser, Subcommand, ValueEnum};
use url::Url;

use caspers_universe::{Result, load_simulation_setup, run_simulation, run_simulation_from};

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

#[derive(Debug, Clone, clap::Parser)]
struct RunArgs {
    #[arg(short, long, default_value_t = 100)]
    duration: usize,

    #[arg(long)]
    /// Path where basic simulation setup is stored.
    setup_path: String,

    #[arg(short, long, value_enum, default_value_t = SimulationMode::Backfill)]
    mode: SimulationMode,

    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Debug, Clone, clap::Parser)]
struct InitArgs {
    #[arg(short, long, default_value_t = true)]
    template: bool,
}

/// Execution mode for the simulation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum SimulationMode {
    /// Run the simulation for the specified time horizon.
    Backfill,
    /// Align time passed in simulation with time passed in real time.
    Realtime,
    /// Continue simulation from last snapshot up to current time, then switch to real time.
    Catchup,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run(args) => {
            let data_path =
                Url::parse("file:///Users/robert.pack/code/management/notebooks/data/")?;
            let routing_path =
                Url::parse("file:///Users/robert.pack/code/management/data/routing")?;

            let setup_path = std::fs::canonicalize(args.setup_path)?;
            let path = Url::from_directory_path(setup_path).expect("Path to be valid directory");
            let setup = load_simulation_setup(&path, None::<(&str, &str)>).await?;

            run_simulation(setup, args.duration, data_path, routing_path, args.dry_run).await?;
            // run_simulation_from(setup, args.duration, data_path, routing_path, args.dry_run).await?;
        }
        Commands::Init(args) => {
            todo!()
        }
    }

    Ok(())
}
