#![feature(iter_array_chunks)]
#![feature(once_cell_try)]

use anyhow::Result;
use clap::Parser;
use config::{Config, File, FileFormat};
use settings::Settings;
use std::{
    fs::exists,
    path::Path,
    sync::{Arc, LazyLock, OnceLock},
    thread,
    time::Duration,
};
use tokio::{
    select,
    signal::{self, ctrl_c},
    spawn,
    sync::{broadcast, watch},
    time::{Sleep, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const SLEEP: Duration = Duration::from_secs(10);
const CHANNEL_LENGTH: usize = 1;
static DEFAULT_CONFIG: &str = include_str!("../default_config.toml");
static CONFIG: &str = "./config.toml";
static SETTINGS: LazyLock<Settings> = LazyLock::new(|| {
    (|| -> Result<_> {
        let path = Path::new(CONFIG);
        let mut builder = Config::builder();
        builder = match exists(path) {
            Ok(true) => builder.add_source(File::from(path)),
            _ => builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml)),
        };
        let settings = builder.build()?.try_deserialize()?;
        Ok(settings)
    })()
    .unwrap()
});

/// BLCS server
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,
}

// #[tokio::main]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    console_subscriber::init();
    // tracing_subscriber::fmt::init();
    // let reload_handle = log::with_reload_handle();
    // log::init();
    info!("Log init");

    // let args = Args::parse();
    info!("settings: {SETTINGS:?}");

    // let token = CancellationToken::new();
    // shutdown::serve(token.clone());
    loop {
        let cancellation = CancellationToken::new();
        if let Err(error) = run(&cancellation).await {
            error!(%error);
            cancellation.cancel();
            sleep(SLEEP).await;
        }
    }
}

async fn run(cancellation: &CancellationToken) -> Result<()> {
    let (temperature_sender, temperature_receiver) = broadcast::channel(CHANNEL_LENGTH);
    let (turbidity_sender, turbidity_receiver) = broadcast::channel(CHANNEL_LENGTH);
    logger::spawn(
        temperature_receiver.resubscribe(),
        turbidity_receiver.resubscribe(),
        cancellation.clone(),
    )?;
    mqtt::spawn(
        temperature_receiver.resubscribe(),
        turbidity_receiver.resubscribe(),
        cancellation.clone(),
    )?;
    let temperature = temperature::spawn(temperature_sender)?;
    let turbidity = turbidity::spawn(turbidity_sender)?;
    select! {
        result = temperature => result??,
        result = turbidity => result??,
    }
    Ok(())
}

mod log;
mod logger;
mod mqtt;
mod settings;
mod shutdown;
mod temperature;
mod turbidity;
