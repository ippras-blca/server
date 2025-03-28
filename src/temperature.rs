use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use tokio::{
    sync::broadcast::Sender,
    task::JoinHandle,
    time::{Duration, interval, sleep, timeout},
};
use tokio_modbus::{client::Context, prelude::*};
use tracing::{debug, error};

const INPUT_REGISTER_SIZE: u16 = 6;
const MODBUS_SERVER_ADDRESS: &str = "192.168.0.113:5502";
const COUNT: u16 = 2;
const INTERVAL: u64 = 1;
const TIMEOUT: u64 = 2 * INTERVAL;
const SLEEP: u64 = 10 * INTERVAL;

pub(crate) async fn start(mut sender: Sender<Message>) -> JoinHandle<()> {
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
        let response = timeout(Duration::from_secs(TIMEOUT), read(&mut context)).await??;
        debug!("temperature response: {response:x?}");
        sender.send(response)?;
    }
}

async fn read(context: &mut Context) -> Result<Message> {
    let date_time = Local::now();
    let data = context
        .read_input_registers(0, COUNT * INPUT_REGISTER_SIZE)
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
