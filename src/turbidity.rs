use crate::SETTINGS;
use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    sync::LazyLock,
};
use tokio::{
    sync::broadcast::Sender,
    task::{Builder, JoinHandle},
    time::{Duration, interval, timeout},
};
use tokio_modbus::{client::Context, prelude::*};
use tracing::{debug, instrument};

// const ID: u64 = 0xc0a80094;

const INPUT_REGISTER_SIZE: u16 = 1;
const TIMEOUT: LazyLock<u64> = LazyLock::new(|| 2 * SETTINGS.turbidity.interval);

pub(crate) fn spawn(sender: Sender<Message>) -> Result<JoinHandle<Result<()>>> {
    Ok(Builder::new().name("turbidity").spawn(run(sender))?)
}

#[instrument(err)]
async fn run(sender: Sender<Message>) -> Result<()> {
    let mut context = tcp::connect(SETTINGS.turbidity.address.into()).await?;
    let mut interval = interval(Duration::from_secs(SETTINGS.turbidity.interval));
    loop {
        interval.tick().await;
        let message = timeout(Duration::from_secs(*TIMEOUT), read(&mut context)).await??;
        debug!("turbidity message: {message}");
        sender.send(message)?;
    }
}

#[instrument(err)]
async fn read(context: &mut Context) -> Result<Message> {
    let date_time = Local::now();
    let identifier = SETTINGS.turbidity.address.ip().to_bits() as _;
    let data = context
        .read_input_registers(0, SETTINGS.turbidity.count * INPUT_REGISTER_SIZE)
        .await??;
    Ok(Message {
        identifier,
        value: data[0],
        date_time,
    })
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub(crate) struct Message {
    pub(crate) identifier: u64,
    pub(crate) value: u16,
    pub(crate) date_time: DateTime<Local>,
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(
            f,
            "identifier = {:x}, value = {}, date_time = {}",
            self.identifier, self.value, self.date_time,
        )
    }
}
