use anyhow::Result;
use chrono::{DateTime, Local};
use std::time::Duration;
use tokio::{
    sync::broadcast::Sender,
    time::{interval, sleep, timeout},
};
use tokio_modbus::{client::Context, prelude::*};
use tracing::{debug, error};

const INPUT_REGISTER_SIZE: u16 = 1;
const MODBUS_SERVER_ADDRESS: &str = "192.168.0.148:5502";
const COUNT: u16 = 1;
const INTERVAL: u64 = 1;
const TIMEOUT: u64 = 2 * INTERVAL;
const SLEEP: u64 = 10 * INTERVAL;

pub(crate) type Value = (Vec<u16>, DateTime<Local>);

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
        let turbidity = timeout(Duration::from_secs(TIMEOUT), read(&mut context)).await??;
        error!("turbidity: {turbidity:x?}");
        sender.send(turbidity)?;
    }
}

async fn read(context: &mut Context) -> Result<Value> {
    let date_time = Local::now();
    let data = context
        .read_input_registers(0, COUNT * INPUT_REGISTER_SIZE)
        .await??;
    let turbidity = data;
    Ok((turbidity, date_time))
}
