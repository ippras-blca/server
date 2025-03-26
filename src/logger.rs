use crate::temperature::Value;
use arrow::{
    array::{ArrayRef, Float32Array, RecordBatch, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
    ipc::{reader::FileReader, writer::FileWriter},
};
use chrono::Local;
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
use polars::{error::PolarsError, io::SerReader as _, prelude::IpcReader};
use std::{
    fs::{File, exists},
    io::Error as IoError,
    mem::replace,
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::broadcast::{Receiver, error::RecvError},
    time::sleep,
};
use tracing::{error, info, trace, warn};

const BATCH_LENGHT: usize = 10;
// const BATCH_LENGHT: usize = 60;
const SLEEP: u64 = 1;
const FILE_FORMAT: &str = "%Y-%m-%d";
const EXTENSION: &str = "log.ipc";

// https://github.com/apache/arrow-rs/blob/main/parquet/src/bin/parquet-concat.rs
// https://github.com/apache/arrow-rs/issues/557
pub(crate) async fn serve(mut temperature_receiver: Receiver<Value>) {
    loop {
        if let Err(error) = run(&mut temperature_receiver).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

pub async fn run(receiver: &mut Receiver<Value>) -> Result<()> {
    ipc(receiver).await
}

pub async fn ipc(receiver: &mut Receiver<Value>) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Identifier", DataType::UInt64, false),
        Field::new("Temperature", DataType::Float32, false),
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let mut today = Local::now().date_naive();
    let mut path = PathBuf::from(today.format(FILE_FORMAT).to_string()).with_extension(EXTENSION);
    let mut batches = Vec::new();
    let mut tomorrow = None;
    if exists(&path)? {
        let reader = FileReader::try_new(File::open(&path)?, None)?;
        for batch in reader {
            batches.push(batch?);
        }

        let file = File::open(&path)?;
        let reader = IpcReader::new(file);
        let data = reader.finish().unwrap();
        info!("data: {data}");
    }
    loop {
        info!("batches: {}", batches.len());
        loop {
            let (identifiers, temperatures, date_time) = match receiver.recv().await {
                Ok(received) => received,
                Err(RecvError::Closed) => panic!("Temperature channel closed"),
                Err(error) => {
                    // temperature_receiver = temperature_receiver.resubscribe();
                    warn!(%error);
                    continue;
                }
            };
            let count = identifiers.len();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(identifiers)),
                    Arc::new(Float32Array::from(temperatures)),
                    Arc::new(TimestampMillisecondArray::from_value(
                        date_time.timestamp_millis(),
                        count,
                    )),
                ],
            )?;
            // Check date
            let date_naive = date_time.date_naive();
            if today != date_naive {
                assert!(today < date_naive);
                error!("today != date_naive: {} {}", today, date_time);
                today = date_naive;
                tomorrow = Some(batch);
                break;
            }
            // Check batch length
            batches.push(batch);
            if batches.len() % BATCH_LENGHT == 0 {
                break;
            }
        }
        let mut writer = FileWriter::try_new_buffered(File::create(&path)?, &schema)?;
        for batch in &batches {
            writer.write(batch)?;
            trace!("batch: {batch:?}");
        }
        writer.finish()?;
        if let Some(tomorrow) = tomorrow.take() {
            // batches.clear();
            // batches.push(tomorrow);
            let batches = replace(&mut batches, vec![tomorrow]);
            let schema = schema.clone();
            // thread::spawn(move || -> Result<()> {
            //     let file = File::create(&path)?;
            //     let mut writer = ArrowWriter::try_new(file, schema, None)?;
            //     for batch in batches {
            //         writer.write(&batch)?;
            //     }
            //     writer.close()?;
            //     Ok(())
            // });
            path = PathBuf::from(today.format(FILE_FORMAT).to_string()).with_extension(EXTENSION);
        }
    }
}

/// Result
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Receiver(#[from] RecvError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

// pub async fn parquet(receiver: &mut Receiver<Value>) -> Result<()> {
//     // let file = File::open("output.ipc")?;
//     // let reader = FileReader::try_new(&file, None)?;
//     // for batch in reader {
//     //     let batch = batch?;
//     //     println!("batch: {batch:?}");
//     // }

//     // let reader = ParquetRecordBatchStreamBuilder::new(file).await?.build()?;
//     // let mut batches = Vec::new();
//     // #[for_await]
//     // for batch in reader {
//     //     batches.push(batch?);
//     // }
//     // println!("batches: {batches:?}");

//     // let schema = Arc::new(Schema::new(vec![Field::new(
//     //     "Temperature",
//     //     DataType::Float32,
//     //     false,
//     // )]));
//     loop {
//         let temperatures = receiver.recv().await?;
//         let temperature =
//             Arc::new(Float32Array::from_iter_values(temperatures.into_values())) as ArrayRef;
//         let batch = RecordBatch::try_from_iter([("Temperature", temperature)])?;

//         let mut file = File::options()
//             .read(true)
//             .write(true)
//             // .append(true)
//             .create(true)
//             .open("output.ipc")?;
//         let reader = FileReader::try_new(&file, None)?;
//         let batches: Vec<_> = reader.into_iter().collect();
//         let mut writer = FileWriter::try_new(&mut file, &batch.schema())?;
//         for batch in batches {
//             writer.write(&batch?)?;
//         }
//         println!("batch: {batch:?}");
//         writer.write(&batch)?;
//         writer.finish()?;
//         // let mut reader = StreamReader::try_new(file, projection).unwrap();
//         // let reader = ParquetRecordBatchStreamBuilder::new(file.try_clone().await?)
//         //     .await?
//         //     .build()?;
//         // // let metadata = file.get_metadata().await?;
//         // let mut writer = AsyncArrowWriter::try_new(file, to_write.schema(), None)?;
//         // // for row_group in metadata.row_groups() {
//         // //     // row_group.into_builder().
//         // //     writer.write(row_group.clone()).await?;
//         // // }
//         // writer.write(&to_write).await?;
//         // writer.close().await?;
//     }

//     // let metadata = builder.metadata().file_metadata().clone();
//     // let inputs = (file, metadata);
//     // let props = Arc::new(WriterProperties::builder().build());
//     // let mut rg_out = writer.flushed_row_groups()?;
// }

// pub async fn run(mut receiver: Receiver<(String, DataFrame)>) -> Result<()> {
//     let hub = drive_hub().await?;
//     let mut index = HashMap::new();
//     while let Some((name, mut data_frame)) = receiver.recv().await {
//         if let Entry::Vacant(vacant) = index.entry(name.clone()) {
//             if let Some(id) = hub.contains_file(&name).await? {
//                 info!("File found {name:?}");
//                 vacant.insert(id.to_owned());
//             } else {
//                 info!("File not found {name:?}");
//                 let content = ser::to_string_pretty(&data_frame, Default::default())?;
//                 let id = hub.create_file(name, content).await?;
//                 vacant.insert(id);
//                 continue;
//             }
//         }
//         debug!("Update file {name:?}");
//         let file_id = &index[&name];
//         // Download
//         let downloaded: DataFrame = hub.download_file(file_id).await?;
//         data_frame = downloaded.vstack(&data_frame)?;
//         // Upload
//         let content = ser::to_string_pretty(&data_frame, Default::default())?;
//         hub.upload_file(file_id, content).await?;
//     }
//     Ok(())
// }
