use self::config::config;
use anyhow::Result;
use clap::Parser;
use rumqttd::Broker;
use tokio::task;
use tracing::{debug, trace};

/// MQTT broker
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,
}

// let mut mqttoptions = MqttOptions::new("rumqtt-async", "test.mosquitto.org", 1883);
// mqttoptions.set_keep_alive(Duration::from_secs(5));

// let (mut client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
// client.subscribe("hello/rumqtt", QoS::AtMostOnce).await.unwrap();

// task::spawn(async move {
//     for i in 0..10 {
//         client.publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![i; i as usize]).await.unwrap();
//         time::sleep(Duration::from_millis(100)).await;
//     }
// });

#[tokio::main]
async fn main() -> Result<()> {
    // console_subscriber::init();
    // let reload_handle = log::with_reload_handle();
    log::init();
    trace!("init");

    let args = Args::parse();
    let config = config(&args.config, None)?;
    debug!(?config);
    let mut broker = Broker::new(config);
    let (tx, rx) = broker.link("repeater")?;
    task::spawn(repeater::run(tx, rx));
    let (tx, rx) = broker.link("logger")?;
    task::spawn(logger::run(tx, rx));
    let (tx, rx) = broker.link("commander")?;
    task::spawn(commander::run(tx, rx));
    // let alerts = broker.alerts()?;
    // let _handle = spawn(move || loop {
    //     if let Ok(alert) = alerts.recv() {
    //         println!("Alert: {alert:?}");
    //     }
    //     sleep(Duration::from_secs(1));
    // });
    broker.start()?;
    Ok(())
}

mod commander;
mod config;
mod log;
mod logger;
mod repeater;

// let mut config_builder = config::Config::builder();
// config_builder = match &args.config {
//     Some(config) => config_builder.add_source(File::with_name(config)),
//     None => config_builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml)),
// };
// let mut config: rumqttd::Config = config_builder.build()?.try_deserialize()?;
// if let Some(console_config) = &mut config.console {
//     console_config.set_filter_reload_handle(reload_handle)
// }
// config.validate();
