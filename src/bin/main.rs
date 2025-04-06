use anyhow::Result;
use futures_util::stream::StreamExt;
use object_store::{GetOptions, GetRange, ObjectStore, PutPayload, http::HttpBuilder, path::Path};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        async_reader::{AsyncFileReader, ParquetObjectReader},
        async_writer::ParquetObjectWriter,
    },
    file::reader::SerializedFileReader,
};
use polars::{io::SerReader as _, prelude::ParquetReader};
use std::{io::Cursor, sync::Arc};
use tokio_util::bytes::Buf as _;

// ba9ef89ad1700e99743edbe3cc896b17
// gxvfqijtupobvequ
/// Tests that even when reqwest has the `gzip` feature enabled, the HTTP store
/// does not error on a missing `Content-Length` header.
#[tokio::main]
async fn main() -> Result<()> {
    github().await
}

async fn github() -> Result<()> {
    // https://github.com/ippras-blcs/storage/blob/main/temperature/2025-03-28-17-11-35.log.parquet
    let http_store = HttpBuilder::new()
        .with_url("https://raw.githubusercontent.com/ippras-blcs/storage/refs/heads/main")
        .build()?;
    let path = Path::parse("temperature/2025-03-28-17-11-35.log.parquet")?;
    let result = http_store
        .get_opts(
            &path,
            GetOptions {
                // range: Some(GetRange::Bounded(0..100)),
                ..Default::default()
            },
        )
        .await?;
    let bytes = result.bytes().await?;
    let mut reader = ParquetReader::new(Cursor::new(bytes));
    let meta = reader.get_metadata()?;
    println!("meta: {meta:?}");
    let data = reader.finish()?;
    println!("data: {data}");

    // let meta = http_store.head(&path).await?;
    // let reader = ParquetObjectReader::new(Arc::new(http_store), meta);
    // let stream = ParquetRecordBatchStreamBuilder::new(reader).await?.build()?;
    // let t = stream.next_row_group().await?;

    // let prefix = Path::from("temperature");
    // // Get an `async` stream of Metadata objects:
    // let mut list = http_store.list(Some(&prefix));
    // // Print a line about each object
    // while let Some(meta) = list.next().await.transpose()? {
    //     println!("Name: {}, size: {}", meta.location, meta.size);
    // }

    // let t = http_store
    //     .put(&Path::parse("TEST.parquet")?, PutPayload::from_bytes(bytes))
    //     .await?;
    Ok(())
}
