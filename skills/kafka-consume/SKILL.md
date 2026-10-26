---
name: kafka-consume
description: Collect bounded Kafka samples with the consume-only kcl CLI, using partition, key, and time or offset filters; save JSON output.
---

Use the caller's binary or `kcl` on PATH. Verify `--version` reports
`kcl consume-only`. Syntax: `kcl [flags] TOPIC...`; no `consume` subcommand,
legacy config files, or `-X`.

- Connection: `-B HOST:PORT,...`. For existing tunnels, repeat
  `--broker-map ADVERTISED:PORT=LOCAL:PORT` for each broker.
- Auth: `KCL_SASL_USER`, `KCL_SASL_PASS`, `KCL_SASL_MECHANISM`.
  Passwords are environment-only. TLS: `--tls`, `--tls-ca`,
  paired `--tls-cert` / `--tls-key`.
- Bounds: `-o :end` snapshots; `-o N:M` selects offsets;
  `-o '@START:END'` selects timestamps. Starts inclusive, ends exclusive.
  Use RFC3339 with timezone. Default `end` tails new records.
- Partitions: `-p N[,N...]`; default all. Derive from keys only with a known
  partitioning scheme; retain key filters because partitions contain many keys.
- Key: `-G 'k:^KEY$'`, regex-escaped and shell-quoted, without base64 encoding.
  Repeated filters AND; `!` negates. Filters match raw bytes, not decoded fields.
- Limit: requested count or `-n 50` for a sample; counts matches across partitions,
  not scanned records. `--timeout 30s` is an idle timeout, not a total deadline.
- Output: `-f json` gives JSONL with base64 byte fields. Add `--msgpack` for
  MessagePack values; decoded values become JSON, failures retain raw bytes.

Example (substitute connection, topic, key, and output paths):

```sh
kcl -B localhost:9092 events -o '@-30m:end' \
  -G 'k:^user-123$' -n 50 -f json > records.jsonl 2> records.stderr
```

Check exit status and stderr. Report file path, count, scope, and errors;
failed reads may leave partial output. A capped sample is not full coverage.
Preserve scope on zero matches. Perform decoded-field filtering on saved output.

Timestamp bounds select offsets, not strict per-record times. Bounded reads
clamp to the log at startup. Reads include uncommitted records; no groups or
offset commits.
