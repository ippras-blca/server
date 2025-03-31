use crate::{temperature::Message as TemperatureMessage, turbidity::Message as TurbidityMessage};
use anyhow::Result;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tokio::{select, spawn, sync::broadcast::Receiver, time::sleep};
use tracing::{error, trace};

const MQTT_HOST: &str = "broker.emqx.io";
const MQTT_PORT: u16 = 1883;
const MQTT_ID: &str = "ippras.ru/blcs/server";
const MQTT_TOPIC_TEMPERATURE: &str = "ippras.ru/blcs/temperature";
const MQTT_TOPIC_TURBIDITY: &str = "ippras.ru/blcs/turbidity";
const CAPACITY: usize = 9;
const SLEEP: u64 = 1;

pub(crate) fn serve(
    mut temperature_receiver: Receiver<TemperatureMessage>,
    mut turbidity_receiver: Receiver<TurbidityMessage>,
) {
    spawn(async move {
        loop {
            if let Err(error) = run(&mut temperature_receiver, &mut turbidity_receiver).await {
                error!(%error);
            }
            sleep(Duration::from_secs(SLEEP)).await;
        }
    });
}

async fn run(
    temperature_receiver: &mut Receiver<TemperatureMessage>,
    turbidity_receiver: &mut Receiver<TurbidityMessage>,
) -> Result<()> {
    let options = MqttOptions::new(MQTT_ID, MQTT_HOST, MQTT_PORT);
    let (client, mut event_loop) = AsyncClient::new(options, CAPACITY);
    // Event loop
    spawn(async move {
        loop {
            match event_loop.poll().await {
                Ok(event) => trace!(?event),
                Err(error) => error!(?error),
            }
        }
    });
    // Publish
    loop {
        select! {
            message = temperature_receiver.recv() => {
                client
                .publish(
                    MQTT_TOPIC_TEMPERATURE,
                    QoS::ExactlyOnce,
                    false,
                    ron::to_string(&message?)?.into_bytes(),
                )
                .await?;
            }
            message = turbidity_receiver.recv() => {
                client
                .publish(
                    MQTT_TOPIC_TURBIDITY,
                    QoS::ExactlyOnce,
                    false,
                    ron::to_string(&message?)?.into_bytes(),
                )
                .await?;
            }
        }
    }
}
