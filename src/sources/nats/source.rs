use std::time::Duration;

use async_nats::jetstream::{
    AckKind,
    consumer::{AckPolicy, PullConsumer},
    message::Acker,
};
use chrono::Utc;
use futures::StreamExt;
use snafu::ResultExt;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{DecoderFramedRead, decoding::StreamDecodingError},
    config::{LegacyKey, LogNamespace},
    finalization::BatchStatus,
    finalizer::UnorderedFinalizer,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, EventsReceived, EventsReceivedHandle,
        InternalEventHandle as _, Protocol,
    },
    lookup::owned_value_path,
};

use crate::{
    SourceSender,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    event::{BatchNotifier, Event},
    internal_events::StreamClosedError,
    shutdown::ShutdownSignal,
    sources::nats::config::{
        BuildError, ConsumerSnafu, JetStreamConfig, MessagesSnafu, NatsSourceConfig, StreamSnafu,
        SubscribeSnafu,
    },
};

/// The outcome of processing a single NATS message.
pub enum ProcessingStatus {
    /// The message payload was fully decoded and sent downstream.
    Success,
    /// A non-recoverable error occurred while decoding the payload.
    Failed,
    /// The downstream channel is closed, and the source should shut down.
    ChannelClosed,
}

/// Processes a single NATS message, sending decoded events downstream.
///
/// This function contains the common logic for both Core and JetStream NATS.
pub async fn process_message(
    msg: &async_nats::Message,
    config: &NatsSourceConfig,
    decoder: &Decoder,
    log_namespace: LogNamespace,
    out: &mut SourceSender,
    events_received: &EventsReceivedHandle,
    batch: &Option<BatchNotifier>,
) -> ProcessingStatus {
    let mut framed = DecoderFramedRead::new(msg.payload.as_ref(), decoder.clone());
    let mut success = true;

    while let Some(next) = framed.next().await {
        match next {
            Ok((events, _byte_size)) => {
                let count = events.len();
                if count == 0 {
                    continue;
                }

                let byte_size = events.estimated_json_encoded_size_of();
                events_received.emit(CountByteSize(count, byte_size));
                let now = Utc::now();
                let events = events.into_iter().map(|mut event| {
                    if let Event::Log(ref mut log) = event {
                        log_namespace.insert_standard_vector_source_metadata(
                            log,
                            NatsSourceConfig::NAME,
                            now,
                        );
                        let legacy_subject_key_field = config
                            .subject_key_field
                            .path
                            .as_ref()
                            .map(LegacyKey::InsertIfEmpty);
                        log_namespace.insert_source_metadata(
                            NatsSourceConfig::NAME,
                            log,
                            legacy_subject_key_field,
                            &owned_value_path!("subject"),
                            msg.subject.as_str(),
                        );
                    }
                    event.with_batch_notifier_option(batch)
                });

                if out.send_batch(events).await.is_err() {
                    emit!(StreamClosedError { count });
                    return ProcessingStatus::ChannelClosed;
                }
            }
            Err(error) => {
                success = false;
                // Error is logged by `vector_lib::codecs::Decoder`, no further
                // handling is needed here.
                if !error.can_continue() {
                    break;
                }
            }
        }
    }

    if success {
        ProcessingStatus::Success
    } else {
        ProcessingStatus::Failed
    }
}

pub(crate) async fn create_consumer_stream(
    connection: &async_nats::Client,
    js_config: &JetStreamConfig,
    acknowledgements: bool,
) -> Result<async_nats::jetstream::consumer::pull::Stream, BuildError> {
    let js = async_nats::jetstream::new(connection.clone());
    let stream = js
        .get_stream(&js_config.stream)
        .await
        .context(StreamSnafu)?;
    let consumer: PullConsumer = stream
        .get_consumer(&js_config.consumer)
        .await
        .context(ConsumerSnafu)?;
    // With `all`, acking a later message would also ack an earlier one still in flight (or
    // failed); with `none`, the server never waits for an ack at all.
    let policy = consumer.cached_info().config.ack_policy;
    if acknowledgements && policy != AckPolicy::Explicit {
        return Err(BuildError::ConsumerAckPolicy { policy });
    }
    consumer
        .stream()
        .max_messages_per_batch(js_config.batch_config.batch)
        .max_bytes_per_batch(js_config.batch_config.max_bytes)
        .messages()
        .await
        .context(MessagesSnafu)
}

/// A JetStream delivery whose ack waits for its events to be finalized downstream.
struct PendingAck {
    acker: Acker,
    stream_sequence: u64,
    delivered: i64,
}

impl std::fmt::Debug for PendingAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAck")
            .field("stream_sequence", &self.stream_sequence)
            .field("delivered", &self.delivered)
            .finish()
    }
}

/// Upper bound of the redelivery delay for an errored batch.
const MAX_NAK_DELAY: Duration = Duration::from_secs(60);

/// Maps the downstream outcome of a delivery to the JetStream ack sent for it.
///
/// `Errored` is transient (retries exhausted), so the message is redelivered, later the more
/// often it has failed; the consumer's `max_deliver` bounds the attempts. `Rejected` is a sink's
/// permanent verdict: redelivering would loop forever, so the message is terminated.
fn ack_kind(status: BatchStatus, delivered: i64) -> AckKind {
    match status {
        BatchStatus::Delivered => AckKind::Ack,
        BatchStatus::Errored => {
            let secs = 5u64.saturating_mul(delivered.max(1) as u64);
            AckKind::Nak(Some(Duration::from_secs(secs).min(MAX_NAK_DELAY)))
        }
        BatchStatus::Rejected => AckKind::Term,
    }
}

async fn finalize(status: BatchStatus, entry: PendingAck) {
    let kind = ack_kind(status, entry.delivered);
    match status {
        BatchStatus::Delivered => {}
        BatchStatus::Errored => warn!(
            message = "Delivery of a JetStream message errored downstream, redelivering.",
            stream_sequence = entry.stream_sequence,
            delivered = entry.delivered,
        ),
        BatchStatus::Rejected => error!(
            message = "JetStream message rejected downstream, terminating it.",
            stream_sequence = entry.stream_sequence,
            delivered = entry.delivered,
        ),
    }
    if let Err(err) = entry.acker.ack_with(kind).await {
        error!(message = "Failed to acknowledge JetStream message.", %err);
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_nats_jetstream(
    config: NatsSourceConfig,
    connection: async_nats::Client,
    initial_messages: async_nats::jetstream::consumer::pull::Stream,
    decoder: Decoder,
    log_namespace: LogNamespace,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
    acknowledgements: bool,
) -> Result<(), ()> {
    let events_received = register!(EventsReceived);
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));
    let mut backoff = ExponentialBackoff::default().max_delay(std::time::Duration::from_secs(30));
    // The ack stream stops taking entries on shutdown but still yields the pending ones.
    let (finalizer, mut acks) =
        UnorderedFinalizer::<PendingAck>::maybe_new(acknowledgements, Some(shutdown.clone()));

    let js_config = config
        .jetstream
        .as_ref()
        .expect("jetstream config must be present");

    let mut messages = initial_messages;

    'run: loop {
        // `ShutdownSignal` fires once then polls `Pending` forever, so shutdown must be handled here via `select!`, not re-polled afterwards.
        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => break 'run,

                Some((status, entry)) = acks.next() => finalize(status, entry).await,

                maybe_msg = messages.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            backoff.reset();
                            bytes_received.emit(ByteSize(msg.payload.len()));
                            let (batch, receiver) =
                                BatchNotifier::maybe_new_with_receiver(acknowledgements);

                            let status = process_message(
                                &msg,
                                &config,
                                &decoder,
                                log_namespace,
                                &mut out,
                                &events_received,
                                &batch,
                            )
                            .await;
                            drop(batch);

                            match status {
                                ProcessingStatus::Success => match (&finalizer, receiver) {
                                    (Some(finalizer), Some(receiver)) => {
                                        let (stream_sequence, delivered) = msg
                                            .info()
                                            .map(|info| (info.stream_sequence, info.delivered))
                                            .unwrap_or_default();
                                        let (_, acker) = msg.split();
                                        finalizer.add(
                                            PendingAck { acker, stream_sequence, delivered },
                                            receiver,
                                        );
                                    }
                                    _ => {
                                        if let Err(err) = msg.ack().await {
                                            error!(message = "Failed to acknowledge JetStream message.", %err);
                                        }
                                    }
                                },
                                ProcessingStatus::ChannelClosed => return Err(()),
                                // The payload does not decode and never will: redelivering it
                                // would loop forever. The decoder has logged the error.
                                ProcessingStatus::Failed => {
                                    if let Err(err) = msg.ack_with(AckKind::Term).await {
                                        error!(message = "Failed to terminate JetStream message.", %err);
                                    }
                                }
                            }
                        }
                        Some(Err(err)) => {
                            warn!(message = "JetStream consumer stream error, recreating.", %err);
                            break;
                        }
                        // The pull stream ended; recover the consumer.
                        None => break,
                    }
                }
            }
        }

        // Reconnect: rebuild the consumer stream with backoff.
        // The durable consumer on the server tracks delivery state,
        // so we pick up where we left off.
        warn!(message = "JetStream pull stream terminated. Recovering consumer...");
        // Drop the failed stream so its background pull task stops issuing pulls and
        // buffering ack-pending deliveries while we back off.
        drop(messages);
        loop {
            let delay = backoff.next().expect("backoff never ends");
            let sleep = tokio::time::sleep(delay);
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut shutdown => break 'run,
                    // Deliveries of the old stream still finalize while we wait.
                    Some((status, entry)) = acks.next() => finalize(status, entry).await,
                    _ = &mut sleep => break,
                }
            }

            match create_consumer_stream(&connection, js_config, acknowledgements).await {
                Ok(m) => {
                    // Don't reset backoff on construction; a built stream hasn't pulled
                    // yet. Backoff is reset only after a message is successfully pulled.
                    messages = m;
                    break;
                }
                Err(err) => {
                    warn!(message = "Failed to recreate JetStream consumer stream, retrying.", %err);
                }
            }
        }
    }

    // Shutdown: stop pulling, let the topology flush what is in flight, and ack it as it
    // lands. Whatever is still unacked when the shutdown deadline kills us is redelivered.
    drop(out);
    // Without acknowledgements `acks` is an empty stream that never ends.
    if let Some(finalizer) = finalizer {
        drop(finalizer);
        while let Some((status, entry)) = acks.next().await {
            finalize(status, entry).await;
        }
    }
    if let Err(err) = connection.flush().await {
        error!(message = "Failed to flush JetStream acknowledgements.", %err);
    }
    Ok(())
}

pub async fn run_nats_core(
    config: NatsSourceConfig,
    _connection: async_nats::Client,
    mut subscriber: async_nats::Subscriber,
    decoder: Decoder,
    log_namespace: LogNamespace,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    let events_received = register!(EventsReceived);
    let bytes_received = register!(BytesReceived::from(Protocol::TCP));

    loop {
        tokio::select! {
            biased;

             _ = &mut shutdown => {
                info!("Shutdown signal received. Draining NATS subscription...");
                if let Err(err) = subscriber.drain().await {
                    error!(message = "Failed to drain NATS subscription.", %err);
                }
            },

            maybe_msg = subscriber.next() => {
                match maybe_msg {
                    Some(msg) => {
                        bytes_received.emit(ByteSize(msg.payload.len()));
                        let status = process_message(
                            &msg,
                            &config,
                            &decoder,
                            log_namespace,
                            &mut out,
                            &events_received,
                            &None,
                        )
                        .await;

                        if let ProcessingStatus::ChannelClosed = status {
                            return Err(());
                        }
                    },
                    None => {
                        // The stream has ended. This happens naturally after a successful
                        // drain or if the connection is lost.
                        break;
                    }
                }
            }
        }
    }

    info!("NATS source drained and shut down gracefully.");
    Ok(())
}

pub async fn create_subscription(
    config: &NatsSourceConfig,
) -> Result<(async_nats::Client, async_nats::Subscriber), BuildError> {
    let nc = config.connect().await?;

    let subscription = match &config.queue {
        None => nc.subscribe(config.subject.clone()).await,
        Some(queue) => {
            nc.queue_subscribe(config.subject.clone(), queue.clone())
                .await
        }
    };

    let subscription = subscription.context(SubscribeSnafu)?;

    Ok((nc, subscription))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivered_is_acked() {
        assert!(matches!(ack_kind(BatchStatus::Delivered, 1), AckKind::Ack));
    }

    #[test]
    fn rejected_is_terminated_not_redelivered() {
        assert!(matches!(ack_kind(BatchStatus::Rejected, 1), AckKind::Term));
        assert!(matches!(ack_kind(BatchStatus::Rejected, 50), AckKind::Term));
    }

    #[test]
    fn errored_is_nakked_with_growing_capped_delay() {
        let delay = |delivered| match ack_kind(BatchStatus::Errored, delivered) {
            AckKind::Nak(Some(delay)) => delay,
            _ => panic!("errored must be a delayed NAK"),
        };
        assert_eq!(delay(0), Duration::from_secs(5));
        assert_eq!(delay(1), Duration::from_secs(5));
        assert_eq!(delay(3), Duration::from_secs(15));
        assert_eq!(delay(1000), MAX_NAK_DELAY);
    }
}
