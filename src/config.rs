use crate::log::ReloadHandle;
use anyhow::Result;
use config::{File, FileFormat};
use rumqttd::Config;
use tracing::trace;

static DEFAULT_CONFIG: &str = include_str!("../default_config.toml");

pub(crate) fn config(path: &Option<String>, reload_handle: Option<ReloadHandle>) -> Result<Config> {
    let mut config_builder = config::Config::builder();
    config_builder = match path {
        Some(config) => config_builder.add_source(File::with_name(config)),
        None => config_builder.add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml)),
    };

    let mut config: Config = config_builder.build()?.try_deserialize()?;
    if let Some(console_config) = &mut config.console {
        if let Some(reload_handle) = reload_handle {
            console_config.set_filter_reload_handle(reload_handle)
        }
    }
    config.validate();
    Ok(config)
}

/// [`Config`] extension methods
trait ConfigExt {
    fn validate(&self);
}

impl ConfigExt for rumqttd::Config {
    // Do any extra validation that needs to be done before starting the broker here.
    fn validate(&self) {
        if let Some(v4) = &self.v4 {
            for (name, server_setting) in v4 {
                if let Some(tls_config) = &server_setting.tls {
                    if !tls_config.validate_paths() {
                        panic!("Certificate path not valid for server v4.{name}.")
                    }
                    trace!("Validated certificate paths for server v4.{name}.");
                }
            }
        }
        if let Some(v5) = &self.v5 {
            for (name, server_setting) in v5 {
                if let Some(tls_config) = &server_setting.tls {
                    if !tls_config.validate_paths() {
                        panic!("Certificate path not valid for server v5.{name}.")
                    }
                    trace!("Validated certificate paths for server v5.{name}.");
                }
            }
        }
        if let Some(ws) = &self.ws {
            for (name, server_setting) in ws {
                if let Some(tls_config) = &server_setting.tls {
                    if !tls_config.validate_paths() {
                        panic!("Certificate path not valid for server ws.{name}.")
                    }
                    trace!("Validated certificate paths for server ws.{name}.");
                }
            }
        }
    }
}
