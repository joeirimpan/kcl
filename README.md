# kcl

A small Kafka consumer. All application code lives in `consume.go`.
It assigns partitions directly: no consumer groups, offset commits,
producing, administration, or Schema Registry commands.

```sh
go build -o /tmp/kcl .
/tmp/kcl -B localhost:9092 events -o start -n 10
```

This replaces the previous multi-command CLI. Topics are positional arguments;
there is no `consume` subcommand. Old configuration files, profiles, and `-X`
overrides are no longer loaded. Use `--help` for the complete flag list.

## Reading

The default offset is `end` (new records). Without `-p`, all existing partitions
of each named topic are assigned at startup. New partitions require restarting.
Offsets and limits apply independently to each partition; `-n` limits the total
records printed. Ordering is preserved within each partition.

```sh
# Read partition 0 from offset 100 through 199.
/tmp/kcl -B localhost:9092 events -p 0 -o 100:200

# Read the existing log and stop at the end captured at startup.
/tmp/kcl -B localhost:9092 events -o :end

# Read the last 30 minutes through the end captured at startup.
/tmp/kcl -B localhost:9092 events -o '@-30m:end'

# Read a timestamp window; its end is exclusive.
/tmp/kcl -B localhost:9092 events \
  -o '@2026-10-21T09:15:00+05:30:2026-10-21T09:30:00+05:30'

# The same window expressed with a duration.
/tmp/kcl -B localhost:9092 events -o '@2026-10-21T09:15:00+05:30:15m'
```

Offsets support `start`, `end`, `start+N`, `end-N`, `N`, `N:M`, `N:end`,
`:M`, and `:end`. Starts are inclusive and ends exclusive. Bounds are clamped
to the available log at startup, so a bounded read does not wait for future
records. Timestamp bounds resolve to the first offset at or after the given
Kafka record timestamp. They are offset boundaries, not per-record timestamp
filters: producer timestamps can arrive out of order.

Timestamps accept RFC3339 with a timezone, `YYYY-MM-DD` in UTC, Unix seconds
(9–10 digits), milliseconds (13 digits), nanoseconds (19 digits), and Go durations
such as `-30m`. A positive duration at the end of a timestamp range is relative
to its start. A negative duration is relative to the invocation time.

The default idle timeout is 30 seconds. `--timeout 0` waits indefinitely.
An idle timeout during a bounded read returns an error if the end has not been
observed; this also avoids silently reporting success when compaction has
removed the records needed to observe the boundary. Ctrl-C stops the consumer.
Reads use Kafka's `read_uncommitted` isolation, so transactional data may include
records from aborted or open transactions; transaction control records are hidden.

## Output and MessagePack

The default output is the raw value followed by a newline. `-f` accepts franz-go
record templates, for example `-f '%t[%p]@%o %k\t%v\n'`.

```sh
/tmp/kcl -B localhost:9092 events -o start -n 10 --msgpack
/tmp/kcl -B localhost:9092 events -o start -n 10 --msgpack -f json
```

`--msgpack` decodes values into JSON. Keys remain unchanged. Tombstones remain
null; undecodable values retain their original bytes and produce one warning
on stderr. It supports standard MessagePack encodings without generated model
code or application-specific imports.

`-f json` prints one object per record with topic, partition, offset, timestamp,
key, value, and any headers. Raw byte fields are base64 encoded; null byte fields
are JSON null. Successfully decoded MessagePack values are embedded JSON.
This JSON shape replaces the previous CLI's output format.

## SSH tunnels and broker mapping

Kafka returns every broker's advertised address. Forward each broker reachable
from your SSH host, then map those advertised addresses to the local forwards:

```sh
ssh -N -T -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:19092:10.0.0.11:9092 \
  -L 127.0.0.1:19093:10.0.0.12:9092 \
  -L 127.0.0.1:19094:10.0.0.13:9092 user@bastion.example

/tmp/kcl -B 10.0.0.11:9092 \
  --broker-map 10.0.0.11:9092=127.0.0.1:19092 \
  --broker-map 10.0.0.12:9092=127.0.0.1:19093 \
  --broker-map 10.0.0.13:9092=127.0.0.1:19094 \
  events -o :end --msgpack
```

Mappings apply to bootstrap and discovered brokers. Unmapped addresses are
used directly. Mapping changes the dial address only; TLS verifies the original
broker hostname unless `--tls-server-name` explicitly overrides it.

## Authentication and TLS

```sh
export KCL_SASL_USER=reader
read -rs KCL_SASL_PASS
export KCL_SASL_PASS
/tmp/kcl -B broker.example:9093 events --sasl-mechanism plain --tls
```

Supported mechanisms are `plain`, `scram-sha-256`, and `scram-sha-512`.
`KCL_SASL_MECHANISM` and `KCL_SASL_USER` can supply defaults. Providing credentials
without a mechanism selects PLAIN. Passwords are read only from `KCL_SASL_PASS`.
TLS uses system CAs, or `--tls-ca ca.pem`. Mutual TLS uses
`--tls-cert client.pem --tls-key client-key.pem`. Certificate verification
cannot be disabled.

## Read-only boundary

Every broker connection rejects requests except Fetch, ListOffsets, Metadata,
OffsetForLeaderEpoch, ApiVersions, SASLHandshake, and SASLAuthenticate.
Metadata must explicitly disable topic auto-creation. Partial, combined,
malformed, and unknown request frames are rejected before any bytes are sent.
The guard also applies to TLS connections and blocks accidental producer calls
inside the underlying Kafka library. There is no flag to disable it.

Kafka 0.11 or newer is required: older Metadata versions cannot explicitly
disable auto-creation, and legacy unframed SASL authentication is rejected.
Use a dedicated account with broker-enforced topic READ/DESCRIBE permissions
and no group, write, or administrative grants for independent enforcement.
Authentication alone does not restrict an account's permissions.

## Development

```sh
go test -race -count=1 ./...
go vet ./...
go build -o /tmp/kcl .
```

Tests use local fake brokers, including three brokers behind TCP forwards,
SASL, TLS hostname checks, offset/timestamp ranges, and forbidden request tests.
They do not require a running Kafka cluster.
