The `nats` source in JetStream mode now acknowledges a message only after every connected sink
with acknowledgements enabled has accepted its events. Errored deliveries are negatively
acknowledged with a delay for redelivery, rejected ones and payloads that fail to decode are
terminated instead of being redelivered forever, and in-flight acknowledgements are sent on a
graceful shutdown. End-to-end acknowledgements require the consumer's `explicit` ack policy.
Core NATS behavior is unchanged.

authors: maestra-io
