use clap::{Args, Parser, Subcommand, ValueEnum};
use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::{
    Resource,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use tracing::level_filters::LevelFilter;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
use url::Url;

use caspers_universe::{Result, load_simulation_setup, run_simulation};

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

fn resource() -> Resource {
    Resource::builder()
        .with_service_name("caspers_universe")
        .build()
}

fn init_meter_provider() -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
        .unwrap();

    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(30))
        .build();

    // For debugging in development
    // let stdout_reader =
    //     PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource())
        .with_reader(reader)
        // with_reader(stdout_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

fn init_tracer_provider() {
    let exporter = SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create span exporter");
    let provider = SdkTracerProvider::builder()
        .with_resource(resource())
        .with_batch_exporter(exporter)
        .build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider);
}

// Initialize tracing-subscriber and return OtelGuard for opentelemetry-related termination processing
fn init_tracing_subscriber() -> OtelGuard {
    // let tracer_provider = global::tracer_provider();
    let meter_provider = init_meter_provider();
    //
    let exporter = SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name("caspers_universe")
                .build(),
        )
        .with_batch_exporter(exporter)
        .build();

    let tracer = tracer_provider.tracer("caspers_universe");

    tracing_subscriber::registry()
        // The global level filter prevents the exporter network stack
        // from reentering the globally installed OpenTelemetryLayer with
        // its own spans while exporting, as the libraries should not use
        // tracing levels below DEBUG. If the OpenTelemetry layer needs to
        // trace spans and events with higher verbosity levels, consider using
        // per-layer filtering to target the telemetry layer specifically,
        // e.g. by target matching.
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::ERROR.into())
                .parse_lossy("caspers_universe=debug"),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(MetricsLayer::new(meter_provider.clone()))
        .with(
            OpenTelemetryLayer::new(tracer)
                .with_location(false)
                .with_threads(false),
        )
        .init();

    OtelGuard {
        tracer_provider,
        meter_provider,
    }
}

struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("{err:?}");
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracer_provider();
    let _guard = init_tracing_subscriber();

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
        Commands::Init(_args) => {
            todo!()
        }
    }

    Ok(())
}
