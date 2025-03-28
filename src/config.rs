use anyhow::Result;
use config::{ConfigBuilder, File, FileFormat, builder::DefaultState};
use serde::Deserialize;
use std::net::SocketAddrV4;

static DEFAULT_CONFIG: &str = include_str!("../default_config.toml");

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    temperature: Logger,
    turbidity: Logger,
}

impl Config {
    pub(crate) fn new(path: &Option<String>) -> Result<Self> {
        let mut builder = Config::builder();
        builder = match path {
            Some(path) => builder.add_source(File::with_name(path)),
            None => builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml)),
        };
        let config = builder.build()?.try_deserialize()?;
        Ok(config)
    }

    fn builder() -> ConfigBuilder<DefaultState> {
        ConfigBuilder::<DefaultState>::default()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Logger {
    address: SocketAddrV4,
    count: usize,
    interval: u64,
}
