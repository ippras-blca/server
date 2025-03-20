use tracing_subscriber::{
    fmt::{
        format::{Format, Pretty},
        Layer,
    },
    layer::Layered,
    reload::Handle,
    EnvFilter, Registry,
};

pub(crate) type ReloadHandle =
    Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

pub(crate) fn init() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

#[allow(unused)]
pub(crate) fn with_reload_handle() -> ReloadHandle {
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .with_filter_reloading();
    let reload_handle = builder.reload_handle();
    builder.init();
    reload_handle
}
