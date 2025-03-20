use anyhow::Result;
use polars::prelude::*;
use std::{
    collections::HashMap,
    mem::replace,
    sync::atomic::{AtomicUsize, Ordering},
};
use time::OffsetDateTime;
use timed::{Timed, MICROSECONDS};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::trace;

pub(crate) static HEIGHT_LIMIT: AtomicUsize = AtomicUsize::new(10);
// pub(crate) static TIME_LIMIT: AtomicUsize = AtomicUsize::new(60);

pub(crate) async fn reader(
    mut receiver: Receiver<(&str, Timed<f64>)>,
    sender: Sender<(String, DataFrame)>,
) -> Result<()> {
    let mut buffers = HashMap::new();
    let mut date = OffsetDateTime::now_utc().date();
    while let Some((name, Timed { time, value })) = receiver.recv().await {
        trace!(?time, ?value);
        let data_frame = {
            // Nanoseconds to milliseconds
            let timestamp = (time.unix_timestamp_nanos() / MICROSECONDS) as i64;
            let time = AnyValue::Datetime(timestamp, TimeUnit::Milliseconds, None);
            df! {
                "Time" => vec![time],
                name => vec![value],
            }?
        };
        let buffer = buffers.entry(name).or_insert(DataFrame::empty());
        if date < time.date() {
            if !buffer.is_empty() {
                sender
                    .send((format!("{name}.{date}.ron"), replace(buffer, data_frame)))
                    .await?;
            }
            date = time.date();
        } else if buffer.height() > HEIGHT_LIMIT.load(Ordering::Relaxed) {
            sender
                .send((format!("{name}.{date}.ron"), replace(buffer, data_frame)))
                .await?;
        } else {
            *buffer = buffer.vstack(&data_frame)?;
        }
        trace!("{buffer:?}");
    }
    Ok(())
}
