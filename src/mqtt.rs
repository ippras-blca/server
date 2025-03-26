use crate::temperature::Value;
use anyhow::Result;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tokio::{spawn, sync::broadcast::Receiver, time::sleep};
use tracing::{error, trace};

const MQTT_HOST: &str = "broker.emqx.io";
const MQTT_PORT: u16 = 1883;
const MQTT_ID: &str = "ippras.ru/blca/server";
const MQTT_TOPIC_TEMPERATURE: &str = "ippras.ru/blca/temperature";
const CAPACITY: usize = 9;
const SLEEP: u64 = 1;

pub(crate) async fn serve(mut temperature_receiver: Receiver<Value>) {
    loop {
        if let Err(error) = run(&mut temperature_receiver).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

async fn run(temperatures_receiver: &mut Receiver<Value>) -> Result<()> {
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
        let temperatures = temperatures_receiver.recv().await?;
        let serialized = ron::to_string(&temperatures)?;
        client
            .publish(
                MQTT_TOPIC_TEMPERATURE,
                QoS::ExactlyOnce,
                false,
                serialized.as_bytes(),
            )
            .await?;
    }
}
