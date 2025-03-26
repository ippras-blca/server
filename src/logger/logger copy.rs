use crate::temperature::Value;
use anyhow::Result;
use arrow::{
    array::{
        ArrayRef, Float32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampSecondArray, UInt64Array, record_batch,
    },
    datatypes::{DataType, Field, Schema},
    ipc::{
        Time,
        reader::{self, FileReader, StreamReader},
        writer::{FileWriter, StreamWriter},
    },
};
use chrono::prelude::*;
use futures_async_stream::for_await;
use google_drive::{DriveHubExt, drive_hub};
use parquet::{
    arrow::{
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask,
        arrow_reader::ParquetRecordBatchReaderBuilder, async_reader::AsyncFileReader as _,
    },
    column::writer::ColumnCloseResult,
    file::{
        metadata::ParquetMetaDataReader, properties::WriterProperties, writer::SerializedFileWriter,
    },
};
use polars::{io::SerReader as _, prelude::IpcReader};
use std::{
    fs::{File, exists},
    io::{Cursor, Read, Seek as _, SeekFrom},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncSeekExt, stream, sync::broadcast::Receiver, time::sleep};
use tracing::{debug, error, info, trace};

const SLEEP: u64 = 1;

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
    let path = Path::new("output.ipc");
    if exists(path)? {
        let file = File::open(path)?;
        let mut reader = IpcReader::new(file);
        // let meta = reader.metadata()?.unwrap_or_default();
        let data = reader.finish()?;
        println!("data {data}");
    }
    loop {
        let temperatures = receiver.recv().await?;
        let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from_value(
            Local::now().timestamp_millis(),
            temperatures.len(),
        ));
        let (keys, values): (Vec<_>, Vec<_>) = temperatures.into_iter().unzip();
        let keys: ArrayRef = Arc::new(UInt64Array::from(keys));
        let values: ArrayRef = Arc::new(Float32Array::from(values));
        let batch = RecordBatch::try_from_iter(vec![
            ("Identifier", keys),
            ("Temperature", values),
            ("Timestamp", timestamps),
        ])?;

        let mut batches = Vec::new();
        let mut file = if exists(path)? {
            let file = File::open(path)?;
            let reader = FileReader::try_new(&file, None)?;
            for batch in reader {
                batches.push(batch);
            }
            File::options().write(true).open(path)
        } else {
            File::create_new("output.ipc")
        }?;
        let mut writer = FileWriter::try_new(&mut file, &batch.schema())?;
        for batch in batches {
            writer.write(&batch?)?;
        }
        trace!("batch: {batch:?}");
        writer.write(&batch)?;
        writer.finish()?;
    }
}

pub async fn parquet(receiver: &mut Receiver<Value>) -> Result<()> {
    // let file = File::open("output.ipc")?;
    // let reader = FileReader::try_new(&file, None)?;
    // for batch in reader {
    //     let batch = batch?;
    //     println!("batch: {batch:?}");
    // }

    // let reader = ParquetRecordBatchStreamBuilder::new(file).await?.build()?;
    // let mut batches = Vec::new();
    // #[for_await]
    // for batch in reader {
    //     batches.push(batch?);
    // }
    // println!("batches: {batches:?}");

    // let schema = Arc::new(Schema::new(vec![Field::new(
    //     "Temperature",
    //     DataType::Float32,
    //     false,
    // )]));
    loop {
        let temperatures = receiver.recv().await?;
        let temperature =
            Arc::new(Float32Array::from_iter_values(temperatures.into_values())) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("Temperature", temperature)])?;

        let mut file = File::options()
            .read(true)
            .write(true)
            // .append(true)
            .create(true)
            .open("output.ipc")?;
        let reader = FileReader::try_new(&file, None)?;
        let batches: Vec<_> = reader.into_iter().collect();
        let mut writer = FileWriter::try_new(&mut file, &batch.schema())?;
        for batch in batches {
            writer.write(&batch?)?;
        }
        println!("batch: {batch:?}");
        writer.write(&batch)?;
        writer.finish()?;
        // let mut reader = StreamReader::try_new(file, projection).unwrap();
        // let reader = ParquetRecordBatchStreamBuilder::new(file.try_clone().await?)
        //     .await?
        //     .build()?;
        // // let metadata = file.get_metadata().await?;
        // let mut writer = AsyncArrowWriter::try_new(file, to_write.schema(), None)?;
        // // for row_group in metadata.row_groups() {
        // //     // row_group.into_builder().
        // //     writer.write(row_group.clone()).await?;
        // // }
        // writer.write(&to_write).await?;
        // writer.close().await?;
    }

    // let metadata = builder.metadata().file_metadata().clone();
    // let inputs = (file, metadata);
    // let props = Arc::new(WriterProperties::builder().build());
    // let mut rg_out = writer.flushed_row_groups()?;
}

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
