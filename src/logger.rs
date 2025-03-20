use crate::temperature::Value;
use anyhow::Result;
use google_drive::{DriveHubExt, drive_hub};
use parquet::file::{
    metadata::ParquetMetaDataReader, properties::WriterProperties, writer::SerializedFileWriter,
};
use ron::ser;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::Duration,
};
use tokio::{fs::File, sync::broadcast::Receiver, time::sleep};
use tracing::{debug, error, info};

const SLEEP: u64 = 1;

pub(crate) async fn serve(temperature_receiver: &mut Receiver<Value>) {
    loop {
        if let Err(error) = run(temperature_receiver).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

// https://github.com/apache/arrow-rs/blob/main/parquet/src/bin/parquet-concat.rs
pub async fn run(mut receiver: Receiver<Value>) -> Result<()> {
    let file = File::open("output.parquet").await?;
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;
    let inputs = (file, metadata);
    let props = Arc::new(WriterProperties::builder().build());
    let schema = inputs[0].1.file_metadata().schema_descr().root_schema_ptr();
    let mut writer = SerializedFileWriter::new(output, schema, props)?;
    Ok(())
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
