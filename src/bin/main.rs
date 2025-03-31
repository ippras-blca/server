use std::sync::Arc;

use anyhow::Result;
use futures_util::stream::StreamExt;
use object_store::{GetOptions, GetRange, ObjectStore, PutPayload, http::HttpBuilder, path::Path};
use parquet::arrow::async_writer::ParquetObjectWriter;

// ba9ef89ad1700e99743edbe3cc896b17
// gxvfqijtupobvequ
/// Tests that even when reqwest has the `gzip` feature enabled, the HTTP store
/// does not error on a missing `Content-Length` header.
#[tokio::main]
async fn main() -> Result<()> {
    // https://github.com/ippras-blcs/storage/blob/main/temperature/2025-03-28-13-22-02.log.parquet
    let http_store = HttpBuilder::new()
        .with_url("https://raw.githubusercontent.com/ippras-blcs/storage/refs/heads/main")
        .build()?;

    // let result = http_store
    //     .get_opts(
    //         &Path::parse("temperature/2025-03-28-13-22-02.log.parquet")?,
    //         GetOptions {
    //             // range: Some(GetRange::Bounded(0..100)),
    //             ..Default::default()
    //         },
    //     )
    //     .await?;
    // println!("result: {result:?}");
    // let bytes = result.bytes().await?;
    // println!("bytes: {bytes:?}");

    let prefix = Path::from("temperature");
    // Get an `async` stream of Metadata objects:
    let mut list = http_store.list(Some(&prefix));
    // Print a line about each object
    while let Some(meta) = list.next().await.transpose()? {
        println!("Name: {}, size: {}", meta.location, meta.size);
    }

    // let t = http_store
    //     .put(&Path::parse("TEST.parquet")?, PutPayload::from_bytes(bytes))
    //     .await?;
    Ok(())
}
