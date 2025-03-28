/// ipc
pub async fn run(receiver: &mut Receiver<TemperatureMessage>) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Identifier", DataType::UInt64, false),
        Field::new("Temperature", DataType::Float32, false),
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let mut today = Local::now().date_naive();
    let mut path = PathBuf::from(today.format(FORMAT).to_string()).with_extension(IPC);
    let mut batches = Vec::new();
    let mut tomorrow = None;
    if exists(&path)? {
        let reader = FileReader::try_new(File::open(&path)?, None)?;
        for batch in reader {
            batches.push(batch?);
        }

        let file = File::open(&path)?;
        let reader = IpcReader::new(file);
        let data = reader.finish().unwrap();
        info!("data: {data}");
    }
    loop {
        info!("Temperature batches: {}", batches.len());
        loop {
            let TemperatureMessage {
                identifiers,
                values,
                date_time,
            } = match receiver.recv().await {
                Ok(received) => received,
                Err(RecvError::Closed) => panic!("Temperature channel closed"),
                Err(error) => {
                    // temperature_receiver = temperature_receiver.resubscribe();
                    warn!(%error);
                    continue;
                }
            };
            let count = identifiers.len();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(identifiers)),
                    Arc::new(Float32Array::from(values)),
                    Arc::new(TimestampMillisecondArray::from_value(
                        date_time.timestamp_millis(),
                        count,
                    )),
                ],
            )?;
            // Check date
            let date_naive = date_time.date_naive();
            if today != date_naive {
                assert!(today < date_naive);
                error!("today != date_naive: {} {}", today, date_time);
                today = date_naive;
                tomorrow = Some(batch);
                break;
            }
            // Check batch length
            batches.push(batch);
            if batches.len() % BATCH_LENGHT == 0 {
                break;
            }
        }
        let mut writer = FileWriter::try_new_buffered(File::create(&path)?, &schema)?;
        for batch in &batches {
            writer.write(batch)?;
            trace!("batch: {batch:?}");
        }
        writer.finish()?;
        if let Some(tomorrow) = tomorrow.take() {
            // batches.clear();
            // batches.push(tomorrow);
            let batches = replace(&mut batches, vec![tomorrow]);
            let schema = schema.clone();
            // thread::spawn(move || -> Result<()> {
            //     let file = File::create(&path)?;
            //     let mut writer = ArrowWriter::try_new(file, schema, None)?;
            //     for batch in batches {
            //         writer.write(&batch)?;
            //     }
            //     writer.close()?;
            //     Ok(())
            // });
            path = PathBuf::from(today.format(FORMAT).to_string()).with_extension(IPC);
        }
    }
}
