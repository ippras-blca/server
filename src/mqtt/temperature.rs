use super::MQTT_TOPIC_DTEC;
use crate::temperature::Message;
use anyhow::Result;
use arrow::{
    array::{Float32Array, RecordBatch, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::writer::StreamWriter,
};
use rumqttc::{AsyncClient, QoS};
use std::sync::Arc;
use tokio::{
    select,
    sync::{broadcast, watch},
    task::{Builder, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

#[instrument(err)]
pub(super) async fn run(
    client: AsyncClient,
    receiver: broadcast::Receiver<Message>,
    cancellation: CancellationToken,
) -> Result<()> {
    let channel = watch::channel(Message::default());
    let reader = reader(receiver, channel.0, cancellation.clone())?;
    let writer = writer(channel.1, client, cancellation.clone())?;
    select! {
        result = reader => result?,
        result = writer => result?,
    }
    Ok(())
}

fn reader(
    receiver: broadcast::Receiver<Message>,
    sender: watch::Sender<Message>,
    cancellation: CancellationToken,
) -> Result<JoinHandle<()>> {
    Ok(Builder::new().name("reader").spawn(Box::pin(async move {
        select! {
            biased;
            _ = cancellation.cancelled() => warn!("mqtt temperature reader cancelled"),
            _ = read(receiver, sender) => warn!("mqtt temperature reader returned"),
        }
    }))?)
}

#[instrument(err)]
pub(crate) async fn read(
    mut receiver: broadcast::Receiver<Message>,
    sender: watch::Sender<Message>,
) -> Result<()> {
    loop {
        let message = receiver.recv().await?;
        sender.send(message)?;
    }
}

fn writer(
    receiver: watch::Receiver<Message>,
    client: AsyncClient,
    cancellation: CancellationToken,
) -> Result<JoinHandle<()>> {
    Ok(Builder::new()
        .name("writer")
        .spawn_local(Box::pin(async move {
            select! {
                biased;
                _ = cancellation.cancelled() => warn!("mqtt temperature writer cancelled"),
                _ = write(receiver, client.clone()) => warn!("mqtt temperature writer returned"),
            }
        }))?)
}

#[instrument(err)]
async fn write(mut receiver: watch::Receiver<Message>, client: AsyncClient) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Identifier", DataType::UInt64, false),
        Field::new("Temperature", DataType::Float32, false),
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    loop {
        receiver.changed().await?;
        let message = receiver.borrow_and_update();
        let count = message.identifiers.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(message.identifiers.clone())),
                Arc::new(Float32Array::from(message.values.clone())),
                Arc::new(TimestampMillisecondArray::from_value(
                    message.date_time.timestamp_millis(),
                    count,
                )),
            ],
        )?;
        debug!(?batch);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        client
            .publish(MQTT_TOPIC_DTEC, QoS::ExactlyOnce, false, payload)
            .await?;
    }
}
