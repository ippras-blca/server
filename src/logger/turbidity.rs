use super::Writer;
use crate::turbidity::Message as TurbidityMessage;
use anyhow::Result;
use arrow::{
    array::{RecordBatch, TimestampMillisecondArray, UInt16Array, UInt64Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tracing::{debug, info, warn};

const TURBIDITY: &str = "turbidity";
const FLUSH: usize = 60;
const WRITE: usize = 10;

/// parquet
pub async fn run(receiver: &mut Receiver<TurbidityMessage>) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Identifier", DataType::UInt64, false),
        Field::new("Turbidity", DataType::UInt16, false),
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
        .folder(TURBIDITY);
    let mut maybe_writer = None;
    loop {
        if let Some(writer) = &maybe_writer {
            debug!(?writer);
        }
        let TurbidityMessage {
            identifier,
            value,
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
        let count = 1;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_value(identifier, count)),
                Arc::new(UInt16Array::from_value(value, count)),
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
