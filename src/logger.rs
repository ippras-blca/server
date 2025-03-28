use self::writer::Writer;
use crate::{temperature::Message as TemperatureMessage, turbidity::Message as TurbidityMessage};
use tokio::{spawn, sync::broadcast::Receiver};
use tracing::error;

// https://github.com/apache/arrow-rs/blob/main/parquet/src/bin/parquet-concat.rs
// https://github.com/apache/arrow-rs/issues/557
pub(crate) fn serve(
    mut temperature_receiver: Receiver<TemperatureMessage>,
    mut turbidity_receiver: Receiver<TurbidityMessage>,
) {
    spawn(async move {
        if let Err(error) = temperature::run(&mut temperature_receiver).await {
            error!(%error);
        }
    });
    spawn(async move {
        if let Err(error) = turbidity::run(&mut turbidity_receiver).await {
            error!(%error);
        }
    });
}

mod temperature;
mod turbidity;
mod writer;

// select! {
//     _ = token.cancelled() => {
//         error!("Ctrl + c");
//         break;
//     },
//     result = turbidity::run(&mut turbidity_receiver, token.clone()) => if let Err(error) = result {
//         error!(%error);
//         sleep(Duration::from_secs(SLEEP)).await;
//     }
// }
