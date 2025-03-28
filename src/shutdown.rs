use tokio::{signal::ctrl_c, spawn};
use tokio_util::sync::CancellationToken;
use tracing::error;

pub(crate) fn serve(token: CancellationToken) {
    spawn(async move {
        loop {
            if let Err(error) = ctrl_c().await {
                error!("Await shutdown signal: {error}");
            }
            token.cancel();
        }
    });
}
