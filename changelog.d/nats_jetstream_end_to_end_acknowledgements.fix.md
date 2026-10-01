The `nats` source in JetStream mode now acknowledges a message only after every connected sink
with acknowledgements enabled has accepted its events. Errored deliveries are negatively
acknowledged with a delay for redelivery, and so are rejected ones (the sink driver reports
exhausted retries as rejected); poison messages are bounded by the consumer's `max_deliver`.
Payloads that fail to decode are terminated, and in-flight acknowledgements are sent on a
graceful shutdown. End-to-end acknowledgements require the consumer's `explicit` ack policy.
Core NATS behavior is unchanged.

authors: maestra-io
