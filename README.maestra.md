# maestra-io/vector-fork

Fork of https://github.com/vectordotdev/vector for the Maestra NATS JetStream consumers
(`vector-siem-nats`, `vector-ingress-logs-nats`), https://github.com/maestra-io/issues-maestra/issues/1480.

Branch `maestra/v0.58` = upstream `v0.58.0` plus:

- upstream #25042 (cherry-picked from master): the JetStream pull stream is recreated after an
  error instead of the source silently stopping;
- end-to-end acknowledgements in the `nats` source (upstream draft #26217, reworked): a message is
  acked when every ack-enabled sink accepted its events (for a disk buffer, after the buffer
  write). `Errored` and `Rejected` → delayed NAK (5 s × deliveries, max 60 s); only payloads that do not
  decode at all → TERM; in-flight acks are drained on graceful shutdown. `Rejected` is not
  dropped because the sink driver reports exhausted retries as `Rejected`: a bounded
  `retry_attempts` must never mean loss. Poison messages are bounded by the consumer's
  `max_deliver` (alert on its MAX_DELIVERIES advisory). Needs `ack_policy: explicit`, and the
  consumer's `ack_wait` must exceed delivery-to-buffer-write latency;
- `.github/workflows/maestra.yml` instead of the upstream workflows: nats source tests against a
  real nats-server, then the image.

## Image

`515260921971.dkr.ecr.us-west-2.amazonaws.com/vector-fork:<upstream>-maestra.<n>`: upstream
`timberio/vector:<upstream>-distroless-libc` with `/usr/bin/vector` replaced by this build
(release profile, `target-x86_64-unknown-linux-gnu` features). A release is a merge into
`maestra/v0.58` with `IMAGE_VERSION` bumped in the workflow; an existing tag is never overwritten.

## Rebasing onto a new upstream release

```bash
git fetch upstream tag v0.59.0 --no-tags
git checkout -b maestra/v0.59 v0.59.0
git cherry-pick <the maestra commits of maestra/v0.58>   # drop what upstream already has
# bump UPSTREAM_VERSION / IMAGE_VERSION (…-maestra.1) in .github/workflows/maestra.yml
```

Local tests: `docker run -d -p 4222:4222 nats:2.12 -js`, then the `cargo test` line from the workflow
and `tests/maestra/bounded-retries-e2e.sh <vector binary>` (real binary, http sink with
`retry_attempts: 2`, endpoint down 40 s; 0.58.0-maestra.1 lost 2000/2000 there, maestra.2 0).
