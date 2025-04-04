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

const INPUT_REGISTER_SIZE: u16 = 6;
const TIMEOUT: LazyLock<u64> = LazyLock::new(|| 2 * SETTINGS.temperature.interval);

pub(crate) fn spawn(sender: Sender<Message>) -> Result<JoinHandle<Result<()>>> {
    Ok(Builder::new().name("temperature").spawn(run(sender))?)
}

#[instrument(err)]
async fn run(sender: Sender<Message>) -> Result<()> {
    let mut context = tcp::connect(SETTINGS.temperature.address.into()).await?;
    let mut interval = interval(Duration::from_secs(SETTINGS.temperature.interval));
    loop {
        interval.tick().await;
        let message = timeout(Duration::from_secs(*TIMEOUT), read(&mut context)).await??;
        debug!("temperature message: {message:x?}");
        sender.send(message)?;
    }
}

#[instrument(err)]
async fn read(context: &mut Context) -> Result<Message> {
    let date_time = Local::now();
    let data = context
        .read_input_registers(0, SETTINGS.temperature.count * INPUT_REGISTER_SIZE)
        .await??;
    let (identifiers, values) = data
        .into_iter()
        .array_chunks()
        .map(|[ab, cd, ef, gh, ij, kl]| {
            let [a, b] = ab.to_be_bytes();
            let [c, d] = cd.to_be_bytes();
            let [e, f] = ef.to_be_bytes();
            let [g, h] = gh.to_be_bytes();
            let [i, j] = ij.to_be_bytes();
            let [k, l] = kl.to_be_bytes();
            (
                u64::from_be_bytes([a, b, c, d, e, f, g, h]),
                f32::from_be_bytes([i, j, k, l]),
            )
        })
        .unzip();
    Ok(Message {
        identifiers,
        values,
        date_time,
    })
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct Message {
    pub(crate) identifiers: Vec<u64>,
    pub(crate) values: Vec<f32>,
    pub(crate) date_time: DateTime<Local>,
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(
            f,
            "identifiers = {:x?}, values = {:?}, date_time = {}",
            self.identifiers, self.values, self.date_time,
        )
    }
}
