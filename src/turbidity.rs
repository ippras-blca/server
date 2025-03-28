use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use tokio::{
    sync::broadcast::Sender,
    time::{Duration, interval, sleep, timeout},
};
use tokio_modbus::{client::Context, prelude::*};
use tracing::{debug, error};

const INPUT_REGISTER_SIZE: u16 = 1;
const MODBUS_SERVER_ADDRESS: &str = "192.168.0.148:5502";
const ID: u64 = 0xc0a80094;
const COUNT: u16 = 1;
const INTERVAL: u64 = 1;
const TIMEOUT: u64 = 2 * INTERVAL;
const SLEEP: u64 = 10 * INTERVAL;

pub(crate) async fn start(mut sender: Sender<Message>) {
    loop {
        if let Err(error) = run(&mut sender).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

async fn run(sender: &mut Sender<Message>) -> Result<()> {
    let socket_addr = MODBUS_SERVER_ADDRESS.parse().unwrap();
    let mut context = tcp::connect(socket_addr).await?;
    let mut interval = interval(Duration::from_secs(INTERVAL));
    loop {
        interval.tick().await;
        let message = timeout(Duration::from_secs(TIMEOUT), read(&mut context)).await??;
        debug!("turbidity message: {message}");
        sender.send(message)?;
    }
}

async fn read(context: &mut Context) -> Result<Message> {
    let date_time = Local::now();
    let data = context
        .read_input_registers(0, COUNT * INPUT_REGISTER_SIZE)
        .await??;
    Ok(Message {
        identifier: ID,
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
