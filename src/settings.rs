use anyhow::Result;
use config::{Config, ConfigBuilder, File, FileFormat, builder::DefaultState};
use serde::Deserialize;
use std::net::SocketAddrV4;

static DEFAULT_CONFIG: &str = include_str!("../default_config.toml");

#[derive(Debug, Deserialize)]
pub(crate) struct Settings {
    pub(crate) temperature: Logger,
    pub(crate) turbidity: Logger,
}

impl Settings {
    pub(crate) fn new(path: &Option<String>) -> Result<Self> {
        let mut builder = Config::builder();
        builder = match path {
            Some(path) => builder.add_source(File::with_name(path)),
            None => builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml)),
        };
        let settings = builder.build()?.try_deserialize()?;
        Ok(settings)
    }

    fn builder() -> ConfigBuilder<DefaultState> {
        ConfigBuilder::<DefaultState>::default()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Logger {
    pub(crate) address: SocketAddrV4,
    pub(crate) count: u16,
    pub(crate) interval: u64,
}
