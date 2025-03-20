#![feature(iter_array_chunks)]

use anyhow::Result;
use clap::Parser;
use tokio::{spawn, task};
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
    // let mut broker = Broker::new(config);
    // let (tx, rx) = broker.link("repeater")?;
    let mut temperature_receivers = temperature::serve();
    mqtt::serve(&mut temperature_receivers[0]).await;
    logger::serve(&mut temperature_receivers[1]).await;
    Ok(())
}

mod log;
mod logger;
mod mqtt;
mod temperature;
// mod commander;
// mod config;
// mod logger;
