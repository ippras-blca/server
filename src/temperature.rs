use anyhow::Result;
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    spawn,
    sync::broadcast::{self, Receiver, Sender},
    time::{interval, sleep, timeout},
};
use tokio_modbus::{client::Context, prelude::*};
use tracing::{debug, error};

const INPUT_REGISTER_SIZE: u16 = 6;
const MODBUS_SERVER_ADDRESS: &str = "192.168.0.113:5502";
const COUNT: u16 = 3;
const INTERVAL: u64 = 1;
const TIMEOUT: u64 = 2 * INTERVAL;
const SLEEP: u64 = 10 * INTERVAL;

pub(crate) type Value = BTreeMap<u64, f32>;

pub(crate) async fn serve(mut sender: Sender<Value>) {
    loop {
        if let Err(error) = run(&mut sender).await {
            error!(%error);
        }
        sleep(Duration::from_secs(SLEEP)).await;
    }
}

async fn run(sender: &mut Sender<Value>) -> Result<()> {
    let socket_addr = MODBUS_SERVER_ADDRESS.parse().unwrap();
    let mut context = tcp::connect(socket_addr).await?;
    let mut interval = interval(Duration::from_secs(INTERVAL));
    loop {
        interval.tick().await;
        let temperatures = timeout(Duration::from_secs(TIMEOUT), read(&mut context)).await??;
        debug!("temperatures: {temperatures:x?}");
        sender.send(temperatures)?;
    }
}

async fn read(context: &mut Context) -> Result<BTreeMap<u64, f32>> {
    let data = context
        .read_input_registers(0, COUNT * INPUT_REGISTER_SIZE)
        .await??;
    Ok(data
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
        .collect())
}
