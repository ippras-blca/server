#![feature(iter_array_chunks)]
#![feature(once_cell_try)]

use anyhow::Result;
use clap::Parser;
use settings::Settings;
use std::{sync::LazyLock, time::Duration};
use tokio::{select, sync::broadcast, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const SLEEP: Duration = Duration::from_secs(10);
const CHANNEL_LENGTH: usize = 1;

static SETTINGS: LazyLock<Settings> = LazyLock::new(|| Settings::new(None).unwrap());

/// BLCS server
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,
}

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
