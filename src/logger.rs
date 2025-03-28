use crate::{temperature::Message as TemperatureMessage, turbidity::Message as TurbidityMessage};
use anyhow::Result;
use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Local};
use object_store::{ObjectStore, path::Path};
use parquet::arrow::{AsyncArrowWriter, async_writer::ParquetObjectWriter};
use std::{
    fmt::{self, Debug, Formatter},
    fs::{File, create_dir_all},
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
};
use tokio::{spawn, sync::broadcast::Receiver};
use tracing::{debug, error};

const FLUSH: usize = 60;
const WRITE: usize = 10;

const FORMAT: &str = "%Y-%m-%d-%H-%M-%S";
const OUTPUT: &str = "D:/g/git/ippras-blca/server/output";
const PARQUET: &str = "log.parquet";

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

struct Writer(AsyncArrowWriter<ParquetObjectWriter>);

impl Writer {
    fn new(
        store: Arc<dyn ObjectStore>,
        schema: SchemaRef,
        folder: &str,
        date_time: DateTime<Local>,
    ) -> Result<Self> {
        let path = PathBuf::from(OUTPUT)
            .join(folder)
            .join(date_time.format(FORMAT).to_string())
            .with_extension(PARQUET);
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        debug!("Logger file path: {}", path.display());
        File::create_new(&path)?;
        let location = Path::from_filesystem_path(path)?;
        let writer = ParquetObjectWriter::new(store, location);
        Ok(Self(AsyncArrowWriter::try_new(writer, schema, None)?))
    }
}

impl Debug for Writer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Writer")
            .field("in_progress_rows", &self.0.in_progress_rows())
            .field("flushed_row_groups", &self.0.flushed_row_groups().len())
            .finish()
    }
}

impl Deref for Writer {
    type Target = AsyncArrowWriter<ParquetObjectWriter>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Writer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

mod temperature;
mod turbidity;

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
