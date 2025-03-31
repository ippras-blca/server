use crate::logger::{reader::HEIGHT_LIMIT, LOGGER};
use anyhow::Result;
use ron::de;
use rumqttd::{
    local::{LinkRx, LinkTx},
    Notification,
};
// use p256::ecdsa::Signature;
use serde::{Deserialize, Serialize};
use std::{str, sync::atomic::Ordering, time::Duration};
use tokio::time::sleep;
use tracing::{error, info, warn};

const MQTT_TOPIC_COMMANDER: &str = "ippras.ru/blcs/commander";
const SLEEP: u64 = 1;

pub(crate) async fn run(mut tx: LinkTx, mut rx: LinkRx) -> Result<()> {
    loop {
        if let Err(error) = try_run(&mut tx, &mut rx).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

async fn try_run(tx: &mut LinkTx, rx: &mut LinkRx) -> Result<()> {
    tx.subscribe(MQTT_TOPIC_COMMANDER)?;
    loop {
        while let Some(Notification::Forward(forward)) = rx.next().await? {
            match str::from_utf8(&forward.publish.topic)? {
                MQTT_TOPIC_COMMANDER => match de::from_bytes(&forward.publish.payload) {
                    Ok(command) => {
                        info!(?command);
                        match command {
                            Command::LogOn => LOGGER.store(true, Ordering::Relaxed),
                            Command::LogOff => LOGGER.store(false, Ordering::Relaxed),
                            Command::LimitHeight(height) => {
                                HEIGHT_LIMIT.store(height, Ordering::Relaxed)
                            }
                        };
                    }
                    Err(error) => error!(%error),
                },
                topic => warn!(r#"unexpected MQTT topic: "{topic}""#),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum Command {
    LogOn,
    LogOff,
    LimitHeight(usize),
}

// /// Signined
// #[derive(Clone, Copy, Debug, Deserialize, Serialize)]
// pub struct Signined<T> {
//     pub signature: Signature,
//     pub value: T,
// }
