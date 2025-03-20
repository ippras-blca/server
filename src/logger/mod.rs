use self::{reader::reader, writer::writer};
use anyhow::Result;
use ron::de;
use rumqttd::{
    Notification,
    local::{LinkRx, LinkTx},
};
use std::{str, sync::atomic::AtomicBool, time::Duration};
use timed::Timed;
use tokio::{runtime::Handle, select, sync::mpsc::channel, task, time::sleep};
use tracing::{Level, debug, enabled, error, trace, warn};

const CHANNEL_BOUND: usize = 1024;
const MQTT_TOPIC_DDOC_C1: &str = "ippras.ru/blca/ddoc/c1";
const MQTT_TOPIC_DDOC_C2: &str = "ippras.ru/blca/ddoc/c2";
const MQTT_TOPIC_DDOC_T1: &str = "ippras.ru/blca/ddoc/t1";
const MQTT_TOPIC_DDOC_T2: &str = "ippras.ru/blca/ddoc/t2";
const MQTT_TOPIC_DDOC_V1: &str = "ippras.ru/blca/ddoc/v1";
const MQTT_TOPIC_DDOC_V2: &str = "ippras.ru/blca/ddoc/v2";
const MQTT_TOPIC_TEMPERATURE: &str = "ippras.ru/blca/temperature";
const MQTT_TOPIC_TURBIDITY: &str = "ippras.ru/blca/turbidity";
const NAME_DDOC_C1: &str = "DDOC.C1";
const NAME_DDOC_C2: &str = "DDOC.C2";
const NAME_DDOC_T1: &str = "DDOC.T1";
const NAME_DDOC_T2: &str = "DDOC.T2";
const NAME_DDOC_V1: &str = "DDOC.V1";
const NAME_DDOC_V2: &str = "DDOC.V2";
const NAME_TEMPERATURE: &str = "Temperature";
const NAME_TURBIDITY: &str = "Turbidity";
const SLEEP: u64 = 1;

pub(crate) static LOGGER: AtomicBool = AtomicBool::new(true);

pub(crate) async fn run(mut tx: LinkTx, mut rx: LinkRx) {
    loop {
        if let Err(error) = try_run(&mut tx, &mut rx).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

async fn try_run(tx: &mut LinkTx, rx: &mut LinkRx) -> Result<()> {
    // Writer
    let (writer_sender, receiver) = channel(CHANNEL_BOUND);
    let mut writer_join_handle = task::spawn(writer(receiver));
    // Reader
    let (reader_sender, receiver) = channel(CHANNEL_BOUND);
    let mut reader_join_handle = task::spawn(reader(receiver, writer_sender));
    tx.subscribe(MQTT_TOPIC_DDOC_C1)?;
    tx.subscribe(MQTT_TOPIC_DDOC_C2)?;
    tx.subscribe(MQTT_TOPIC_DDOC_T1)?;
    tx.subscribe(MQTT_TOPIC_DDOC_T2)?;
    tx.subscribe(MQTT_TOPIC_DDOC_V1)?;
    tx.subscribe(MQTT_TOPIC_DDOC_V2)?;
    tx.subscribe(MQTT_TOPIC_TEMPERATURE)?;
    tx.subscribe(MQTT_TOPIC_TURBIDITY)?;
    loop {
        if enabled!(Level::TRACE) {
            let metrics = Handle::current().metrics();
            let num_alive_tasks = metrics.num_alive_tasks();
            trace!(%num_alive_tasks);
        }
        select! {
            result = &mut writer_join_handle => result??,
            result = &mut reader_join_handle => result??,
            notification = rx.next() => match notification? {
                Some(Notification::Forward(forward)) => {
                    debug!(?forward);
                    let publish = forward.publish;
                    match str::from_utf8(&publish.topic)? {
                        MQTT_TOPIC_TEMPERATURE => {
                            reader_sender
                                .send((
                                    NAME_TEMPERATURE,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_TURBIDITY => {
                            reader_sender
                                .send((
                                    NAME_TURBIDITY,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_DDOC_C1 => {
                            reader_sender
                                .send((
                                    NAME_DDOC_C1,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_DDOC_C2 => {
                            reader_sender
                                .send((
                                    NAME_DDOC_C2,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_DDOC_T1 => {
                            reader_sender
                                .send((
                                    NAME_DDOC_T1,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_DDOC_T2 => {
                            reader_sender
                                .send((
                                    NAME_DDOC_T2,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_DDOC_V1 => {
                            reader_sender
                                .send((
                                    NAME_DDOC_V1,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        MQTT_TOPIC_DDOC_V2 => {
                            reader_sender
                                .send((
                                    NAME_DDOC_V2,
                                    de::from_bytes::<Timed<f64>>(&publish.payload)?,
                                ))
                                .await?
                        }
                        topic => warn!(r#"unexpected MQTT topic: "{topic}""#),
                    }
                }
                notification => {
                    if let Some(notification) = notification {
                        trace!(?notification);
                    }
                    continue
                },
            },
        };
    }
}

pub(crate) mod reader;
pub(crate) mod writer;
