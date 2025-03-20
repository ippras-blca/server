use anyhow::Result;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use rumqttd::{
    Notification,
    local::{LinkRx, LinkTx},
};
use std::time::Duration;
use tokio::{
    select,
    task::{self, JoinHandle},
    time::sleep,
};
use tracing::{debug, error, trace};

const CAPACITY: usize = 9;
const MQTT_HOST: &str = "broker.emqx.io";
const MQTT_ID: &str = "repeater";
const MQTT_PORT: u16 = 1883;
const MQTT_TOPIC: &str = "ippras.ru/blca/#";
const SLEEP: u64 = 1;

pub(crate) async fn run(mut tx: LinkTx, mut rx: LinkRx) {
    loop {
        if let Err(error) = try_run(&mut tx, &mut rx).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

async fn try_run(tx: &mut LinkTx, rx: &mut LinkRx) -> Result<()> {
    tx.subscribe(MQTT_TOPIC)?;
    let options = MqttOptions::new(MQTT_ID, MQTT_HOST, MQTT_PORT);
    let (client, mut event_loop) = AsyncClient::new(options, CAPACITY);
    // Event loop
    let mut join_handle: JoinHandle<Result<_>> = task::spawn(async move {
        loop {
            let event = event_loop.poll().await?;
            trace!(?event);
        }
    });
    // Publish loop
    loop {
        select! {
            result = &mut join_handle => result??,
            notification = rx.next() => match notification? {
                Some(Notification::Forward(forward)) => {
                    debug!(?forward);
                    let publish = forward.publish;
                    client
                        .publish(
                            String::from_utf8_lossy(&publish.topic),
                            QoS::ExactlyOnce,
                            false,
                            publish.payload,
                        )
                        .await?;
                }
                // Some(Notification::Unschedule) => {
                //     rx.wake().await?
                // }
                notification => {
                    if let Some(notification) = notification {
                        trace!(?notification);
                    }
                    continue
                },
            },
        }
    }
}

// loop {
//     // select! {
//     //     _ = interval.tick() => {
//     //         println!("time: {:?}", instant.elapsed());
//     //     }
//     //     notification = rx.next() => {
//     //         match notification? {
//     //             Some(Notification::Forward(forward)) => {
//     //                 debug!(?forward);
//     //                 let publish = forward.publish;
//     //                 client
//     //                 .publish(
//     //                     String::from_utf8_lossy(&publish.topic),
//     //                     QoS::ExactlyOnce,
//     //                     false,
//     //                     publish.payload,
//     //                 )
//     //                 .await?;
//     //         },
//     //         Some(Notification::Unschedule) => {
//     //                 debug!("unschedule");
//     //                 rx.wake().await?
//     //             },
//     //             _ => continue
//     //         };
//     //     }
//     // }
//     match rx.recv()? {
//         Some(Notification::Forward(forward)) => {
//             debug!(?forward);
//             let publish = forward.publish;
//             client
//                 .publish(
//                     String::from_utf8_lossy(&publish.topic),
//                     QoS::ExactlyOnce,
//                     false,
//                     publish.payload,
//                 )
//                 .await?;
//             // Yield every now and then.
//             yield_now().await
//         }
//         notification => {
//             if let Some(notification) = notification {
//                 trace!(?notification);
//             }
//             warn!("continue");
//           continue;
//         }
//     };
// }
