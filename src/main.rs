#![feature(duration_constructors)]
#![feature(iter_array_chunks)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]

use anyhow::Result;
use clap::Parser;
use tokio::{join, spawn, sync::broadcast, task};
use tracing::{debug, trace};

/// MQTT broker
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    // console_subscriber::init();
    // let reload_handle = log::with_reload_handle();
    // log::init();
    trace!("Log init");

    let args = Args::parse();
    // let config = config(&args.config, None)?;
    // debug!(?config);
    let (temperature_sender, temperature_receiver) = broadcast::channel(1);
    let (turbidity_sender, turbidity_receiver) = broadcast::channel(1);
    spawn(mqtt::serve(temperature_receiver));
    let receiver = temperature_sender.subscribe();
    spawn(logger::serve(receiver));

    spawn(turbidity::serve(turbidity_sender));
    let _ = spawn(temperature::serve(temperature_sender)).await;
    Ok(())
}

mod log;
mod logger;
mod mqtt;
mod temperature;
mod turbidity;
// mod commander;
// mod config;
// mod logger;
