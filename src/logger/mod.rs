use self::writer::Writer;
use crate::{temperature::Message as TemperatureMessage, turbidity::Message as TurbidityMessage};
use std::io;
use tokio::{
    select,
    sync::broadcast,
    task::{Builder, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

// https://github.com/apache/arrow-rs/blob/main/parquet/src/bin/parquet-concat.rs
// https://github.com/apache/arrow-rs/issues/557
pub(crate) fn spawn(
    temperature_receiver: broadcast::Receiver<TemperatureMessage>,
    turbidity_receiver: broadcast::Receiver<TurbidityMessage>,
    cancellation: CancellationToken,
) -> io::Result<JoinHandle<()>> {
    Builder::new().name("logger").spawn(Box::pin(async move {
        loop {
            let temperature =
                temperature::run(temperature_receiver.resubscribe(), cancellation.clone());
            let turbidity = turbidity::run(turbidity_receiver.resubscribe(), cancellation.clone());
            select! {
                biased;
                _ = cancellation.cancelled() => {
                    warn!("logger cancelled");
                    break;
                }
                _ = temperature => {},
                _ = turbidity => {},
            };
            warn!("loop logger");
        }
    }))
}

mod temperature;
mod turbidity;
mod writer;
