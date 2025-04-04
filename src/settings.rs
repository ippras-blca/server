use anyhow::Result;
use config::{Config, File, FileFormat};
use serde::Deserialize;
use std::{fs::exists, net::SocketAddrV4, path::Path};
use tracing::info;

static DEFAULT_CONFIG: &str = include_str!("../default_config.toml");
static DEFAULT_CONFIG_PATH: &str = "./config.toml";

#[derive(Debug, Deserialize)]
pub(crate) struct Settings {
    pub(crate) output: String,
    pub(crate) temperature: Logger,
    pub(crate) turbidity: Logger,
}

impl Settings {
    pub(crate) fn new(path: Option<&String>) -> Result<Self> {
        let mut builder = Config::builder();
        if let Some(path) = path {
            builder = builder.add_source(File::with_name(path));
            info!("Config: {path}");
        } else {
            let path = Path::new(DEFAULT_CONFIG_PATH);
            match exists(path) {
                Ok(true) => {
                    info!("Config: DEFAULT_PATH ({DEFAULT_CONFIG_PATH})");
                    builder = builder.add_source(File::from(path));
                }
                _ => {
                    info!("Config: DEFAULT");
                    builder = builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml));
                }
            }
        }
        let settings = builder.build()?.try_deserialize()?;
        Ok(settings)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Logger {
    pub(crate) address: SocketAddrV4,
    pub(crate) count: u16,
    pub(crate) finish: usize,
    pub(crate) flush: usize,
    pub(crate) interval: u64,
}

impl Logger {
    pub(crate) fn flush(&self) -> usize {
        self.count as usize * self.flush
    }
}
