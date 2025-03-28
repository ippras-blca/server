use super::{BATCH_LENGHT, FORMAT, OUTPUT, PARQUET};
use crate::{temperature::Message as TemperatureMessage, turbidity::Message as TurbidityMessage};
use anyhow::Result;
use arrow::{
    array::{
        ArrayRef, Float32Array, RecordBatch, TimestampMillisecondArray, UInt16Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
    ipc::{reader::FileReader, writer::FileWriter},
};
use chrono::Local;
use object_store::{ObjectStore as _, memory::InMemory, path::Path};
use parquet::{
    arrow::{
        ArrowWriter, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
        async_reader::ParquetObjectReader, async_writer::ParquetObjectWriter,
    },
    schema::printer::print_parquet_metadata,
};
use polars::{error::PolarsError, io::SerReader as _, prelude::IpcReader};
use std::{
    fs::{File, exists},
    io::{Error as IoError, stdout},
    mem::replace,
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    join, select, spawn,
    sync::broadcast::{Receiver, error::RecvError},
    task::JoinHandle,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

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

const TURBIDITY: &str = "turbidity";

/// parquet
pub async fn run(
    receiver: &mut Receiver<TurbidityMessage>,
    token: CancellationToken,
) -> Result<()> {
    use object_store::local::LocalFileSystem;
    use url::Url;

    let schema = Arc::new(Schema::new(vec![
        Field::new("Identifier", DataType::UInt64, false),
        Field::new("Turbidity", DataType::UInt16, false),
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));

    let today = Local::now().date_naive();
    // let store = Arc::new(InMemory::new());
    let store = Arc::new(LocalFileSystem::new());
    let path = PathBuf::from(OUTPUT)
        .join(TURBIDITY)
        .join(today.format(FORMAT).to_string())
        .with_extension(PARQUET);
    debug!("Turbidity logger file path: {}", path.display());
    let location = Path::from_filesystem_path(path)?;
    let writer = ParquetObjectWriter::new(store.clone(), location.clone());
    let mut writer = AsyncArrowWriter::try_new(writer, schema.clone(), None).unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from_value(9, 1)),
            Arc::new(UInt16Array::from_value(10, 1)),
            Arc::new(TimestampMillisecondArray::from_value(
                Local::now().timestamp_millis(),
                1,
            )),
        ],
    )?;
    writer.write(&batch).await.unwrap();
    writer.close().await.unwrap();
    let meta = store.head(&location).await.unwrap();
    let input = ParquetObjectReader::new(store, meta);
    let builder = ParquetRecordBatchStreamBuilder::new(input).await.unwrap();
    print_parquet_metadata(&mut stdout(), builder.metadata());

    // info!("Turbidity path: {}", path.display());

    // let location = Path::from(r#"D:\\g\\git\\ippras-blca\\server\\input1.parquet"#);
    // let meta = store.head(&location).await.unwrap();
    // println!("Found Blob with {}B at {}", meta.size, meta.location);
    // let context = SessionContext::new();

    // let path = PathBuf::from(format!("turbidity/{}.{PARQUET}", today.format(FORMAT)));
    // info!("Turbidity path: {}", path.display());
    // let mut batches = Vec::new();
    // let mut tomorrow = None;
    let mut batches = 0;
    // loop {
    //     info!("Turbidity bytes written: {}", writer.bytes_written());
    //     let TurbidityMessage {
    //         identifier,
    //         value,
    //         date_time,
    //     } = match receiver.recv().await {
    //         Ok(message) => message,
    //         Err(RecvError::Closed) => panic!("Turbidity channel closed"),
    //         Err(error) => {
    //             warn!(%error);
    //             continue;
    //         }
    //     };
    //     let count = 1;
    //     let batch = RecordBatch::try_new(
    //         schema.clone(),
    //         vec![
    //             Arc::new(UInt64Array::from_value(identifier, count)),
    //             Arc::new(UInt16Array::from_value(value, count)),
    //             Arc::new(TimestampMillisecondArray::from_value(
    //                 date_time.timestamp_millis(),
    //                 count,
    //             )),
    //         ],
    //     )?;
    //     // Check date
    //     let date_naive = date_time.date_naive();
    //     // if today != date_naive {
    //     //     assert!(today < date_naive);
    //     //     error!("today != date_naive: {} {}", today, date_time);
    //     //     today = date_naive;
    //     //     tomorrow = Some(batch);
    //     //     break;
    //     // }
    //     println!("batch: {batch:?}");
    //     writer.write(&batch).await?;
    //     // Check batch length
    //     // batches.push(batch);
    //     if batches % BATCH_LENGHT == 0 {
    //         break;
    //     }
    //     // info!(%identifiers, ?turbidity, %date_time);
    // }
    Ok(())
}
