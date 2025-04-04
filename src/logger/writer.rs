use crate::SETTINGS;
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
use typed_builder::TypedBuilder;

const FORMAT: &str = "%Y-%m-%d-%H-%M-%S";
const PARQUET: &str = "log.parquet";

type ALL<'a> = (
    (Arc<dyn ObjectStore>,),
    (&'a str,),
    (DateTime<Local>,),
    (SchemaRef,),
);

type NONE = ((), (), (), ());

/// Inner writer
#[derive(TypedBuilder)]
#[builder(builder_type(vis="pub(crate)", name=WriterBuilder))]
#[builder(build_method(vis="", name=_build))]
struct _Writer<'a> {
    store: Arc<dyn ObjectStore>,
    folder: &'a str,
    date_time: DateTime<Local>,
    schema: SchemaRef,
}

impl<'a> WriterBuilder<'a, ALL<'a>> {
    pub(crate) fn build(self) -> Result<Writer> {
        let build = self._build();
        let path = PathBuf::from(&SETTINGS.output)
            .join(build.folder)
            .join(build.date_time.format(FORMAT).to_string())
            .with_extension(PARQUET);
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        File::create_new(&path)?;
        let location = Path::from_filesystem_path(path)?;
        let writer = ParquetObjectWriter::new(build.store, location);
        Ok(Writer(AsyncArrowWriter::try_new(
            writer,
            build.schema,
            None,
        )?))
    }
}

/// Writer
pub(crate) struct Writer(AsyncArrowWriter<ParquetObjectWriter>);

impl Writer {
    pub(crate) fn builder<'a>() -> WriterBuilder<'a, NONE> {
        _Writer::builder()
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
