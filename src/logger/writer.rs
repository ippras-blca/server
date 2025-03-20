use anyhow::Result;
use google_drive::{drive_hub, DriveHubExt};
use polars::prelude::*;
use ron::ser;
use std::collections::{hash_map::Entry, HashMap};
use tokio::sync::mpsc::Receiver;
use tracing::{debug, info};

pub async fn writer(mut receiver: Receiver<(String, DataFrame)>) -> Result<()> {
    let hub = drive_hub().await?;
    let mut index = HashMap::new();
    while let Some((name, mut data_frame)) = receiver.recv().await {
        if let Entry::Vacant(vacant) = index.entry(name.clone()) {
            if let Some(id) = hub.contains_file(&name).await? {
                info!("File found {name:?}");
                vacant.insert(id.to_owned());
            } else {
                info!("File not found {name:?}");
                let content = ser::to_string_pretty(&data_frame, Default::default())?;
                let id = hub.create_file(name, content).await?;
                vacant.insert(id);
                continue;
            }
        }
        debug!("Update file {name:?}");
        let file_id = &index[&name];
        // Download
        let downloaded: DataFrame = hub.download_file(file_id).await?;
        data_frame = downloaded.vstack(&data_frame)?;
        // Upload
        let content = ser::to_string_pretty(&data_frame, Default::default())?;
        hub.upload_file(file_id, content).await?;
    }
    Ok(())
}
