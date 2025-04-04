use crate::{temperature::Message as TemperatureMessage, turbidity::Message as TurbidityMessage};
use anyhow::Result;
use rumqttc::{AsyncClient, EventLoop, MqttOptions};
use scopeguard::defer;
use std::io;
use tokio::{
    select,
    sync::broadcast,
    task::{Builder, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, trace, warn};

const MQTT_HOST: &str = "broker.emqx.io";
const MQTT_PORT: u16 = 1883;
const MQTT_ID: &str = "ippras.ru/blcs/server";
const MQTT_TOPIC_DTEC: &str = "ippras.ru/blcs/dtec";
const MQTT_TOPIC_ATUC: &str = "ippras.ru/blcs/atuc";
const CAPACITY: usize = 9;

pub(crate) fn spawn(
    temperature_receiver: broadcast::Receiver<TemperatureMessage>,
    turbidity_receiver: broadcast::Receiver<TurbidityMessage>,
    cancellation: CancellationToken,
) -> io::Result<JoinHandle<()>> {
    Builder::new().name("mqtt").spawn(Box::pin(async move {
        loop {
            select! {
                biased;
                _ = cancellation.cancelled() => {
                    warn!("mqtt cancelled");
                    break;
                }
                _ = run(temperature_receiver.resubscribe(), turbidity_receiver.resubscribe(), cancellation.clone()) => {},
            };
            warn!("loop mqtt");
        }
    }))
}

#[instrument(err)]
async fn run(
    temperature_receiver: broadcast::Receiver<TemperatureMessage>,
    turbidity_receiver: broadcast::Receiver<TurbidityMessage>,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut options = MqttOptions::new(MQTT_ID, MQTT_HOST, MQTT_PORT);
    options.set_clean_session(true);
    let (client, event_loop) = AsyncClient::new(options, CAPACITY);
    // Event loop
    let event_loop = Builder::new()
        .name("event loop")
        .spawn(Box::pin(read(event_loop)))?;
    let abort_handle = event_loop.abort_handle();
    defer! {
        abort_handle.abort();
    }
    // Publish
    let temperature = temperature::run(
        temperature_receiver.resubscribe(),
        client.clone(),
        cancellation.clone(),
    );
    let turbidity = turbidity::run(
        turbidity_receiver.resubscribe(),
        client.clone(),
        cancellation.clone(),
    );
    select! {
        result = temperature => result?,
        result = turbidity => result?,
        result = event_loop => result?,
    }
    Ok(())
}

#[instrument(skip(event_loop))]
async fn read(mut event_loop: EventLoop) {
    loop {
        match event_loop.poll().await {
            Ok(event) => trace!(?event),
            Err(error) => error!(?error),
        }
    }
}

mod temperature;
mod turbidity;
