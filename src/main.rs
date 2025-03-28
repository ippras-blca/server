#![feature(iter_array_chunks)]

use anyhow::Result;
use clap::Parser;
use config::Config;
use tokio::{
    select,
    signal::{self, ctrl_c},
    spawn,
    sync::{broadcast, watch},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const CHANNEL_LENGTH: usize = 1;

/// BLCA server
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    console_subscriber::init();
    // tracing_subscriber::fmt::init();
    // let reload_handle = log::with_reload_handle();
    // log::init();
    info!("Log init");

    let args = Args::parse();
    let config = Config::new(&args.config)?;
    info!("config: {config:?}");

    // let token = CancellationToken::new();
    // shutdown::serve(token.clone());
    let (temperature_sender, temperature_receiver) = broadcast::channel(CHANNEL_LENGTH);
    let (turbidity_sender, turbidity_receiver) = broadcast::channel(CHANNEL_LENGTH);
    mqtt::serve(temperature_receiver, turbidity_receiver);
    logger::serve(temperature_sender.subscribe(), turbidity_sender.subscribe());
    select! {
        _ = temperature::start(temperature_sender) => error!("temperature reader stopped"),
        _ = turbidity::start(turbidity_sender) => error!("turbidity reader stopped"),
    };
    Ok(())
}

mod config;
mod log;
mod logger;
mod mqtt;
mod shutdown;
mod temperature;
mod turbidity;
