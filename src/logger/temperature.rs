use super::Writer;
use crate::temperature::Message as TemperatureMessage;
use anyhow::Result;
use arrow::{
    array::{Float32Array, RecordBatch, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tracing::{debug, info, warn};

const TEMPERATURE: &str = "temperature";
const FLUSH: usize = 120;
const WRITE: usize = 10;

/// parquet
pub async fn run(receiver: &mut Receiver<TemperatureMessage>) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Identifier", DataType::UInt64, false),
        Field::new("Temperature", DataType::Float32, false),
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let store = Arc::new(LocalFileSystem::new());
    let builder = Writer::builder()
        .schema(schema.clone())
        .store(store)
        .folder(TEMPERATURE);
    let mut maybe_writer = None;
    loop {
        if let Some(writer) = &maybe_writer {
            debug!(?writer);
        }
        let TemperatureMessage {
            identifiers,
            values,
            date_time,
        } = match receiver.recv().await {
            Ok(message) => message,
            Err(error @ RecvError::Lagged(_)) => {
                warn!(%error);
                continue;
            }
            Err(error) => Err(error)?,
        };
        let writer = match &mut maybe_writer {
            Some(writer) => writer,
            None => maybe_writer.insert(builder.clone().date_time(date_time).build()?),
        };
        let count = identifiers.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(identifiers)),
                Arc::new(Float32Array::from(values)),
                Arc::new(TimestampMillisecondArray::from_value(
                    date_time.timestamp_millis(),
                    count,
                )),
            ],
        )?;
        writer.write(&batch).await?;
        // Check for flush
        if writer.in_progress_rows() >= FLUSH {
            info!("Flush {}", writer.in_progress_rows());
            writer.flush().await?
        }
        // Check for writer
        if writer.flushed_row_groups().len() >= WRITE {
            info!("Close {}", writer.flushed_row_groups().len());
            writer.finish().await?;
            maybe_writer.take();
        }
    }
}
