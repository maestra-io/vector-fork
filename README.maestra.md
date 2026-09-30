# maestra-io/vector-fork

Fork of https://github.com/vectordotdev/vector for the Maestra NATS JetStream consumers
(`vector-siem-nats`, `vector-ingress-logs-nats`), https://github.com/maestra-io/issues-maestra/issues/1480.

Branch `maestra/v0.58` = upstream `v0.58.0` plus:

- upstream #25042 (cherry-picked from master): the JetStream pull stream is recreated after an
  error instead of the source silently stopping;
- end-to-end acknowledgements in the `nats` source (upstream draft #26217, reworked): a message is
  acked when every ack-enabled sink accepted its events (for a disk buffer, after the buffer
  write). `Errored` → delayed NAK (5 s × deliveries, max 60 s), `Rejected` and undecodable
  payloads → TERM, in-flight acks are drained on graceful shutdown. Needs `ack_policy: explicit`,
  and the consumer's `ack_wait` must exceed delivery-to-buffer-write latency;
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

Local tests: `docker run -d -p 4222:4222 nats:2.12 -js`, then the `cargo test` line from the workflow.
