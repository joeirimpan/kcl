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

## Flags

Usage: `kcl [flags] TOPIC [TOPIC...]`. Examples below are flag fragments to add
to a command, such as `/tmp/kcl events -o :end -n 50 -f json`.

| Flag | Purpose and default | Example use |
| --- | --- | --- |
| `-B`, `--brokers` | Comma-separated bootstrap brokers; default `localhost:9092`. | `-B broker1:9092,broker2:9092` |
| `--broker-map` | Map an advertised broker address to a forwarded address; repeat for each broker. | `--broker-map broker1:9092=127.0.0.1:19092` |
| `-p`, `--partitions` | Comma-separated partition IDs; default all existing partitions of each topic. | `-p 0,2` |
| `-o`, `--offset` | Starting offset or bounded range; default `end` (new records). | `-o :end` or `-o '@-30m:end'` |
| `-n`, `--num` | Maximum total records printed after filtering; default `0` (unlimited). | `-n 50` |
| `--timeout` | Idle timeout; default `30s`. `0` waits indefinitely. | `--timeout 10s` |
| `-f`, `--format` | Record template or `json`; default `%v\n` (value and newline). | `-f json` or `-f '%t[%p]@%o %k\t%v\n'` |
| `-G`, `--grep` | Local regex filter; repeat to require all filters to match. Default no filtering. | `-G 'k:^user-123$' -G '!v:warning'` |
| `--msgpack` | Decode MessagePack values into JSON; default off. | `--msgpack -f json` |
| `--sasl-mechanism` | `plain`, `scram-sha-256`, or `scram-sha-512`; defaults to `KCL_SASL_MECHANISM`, or PLAIN when credentials are supplied. | `--sasl-mechanism scram-sha-256` |
| `--sasl-user` | SASL username; defaults to `KCL_SASL_USER`. Password comes from `KCL_SASL_PASS`. | `--sasl-user reader` |
| `--tls` | Enable TLS with certificate verification and system CAs; default off. | `--tls` |
| `--tls-ca` | Use a PEM CA bundle; also enables TLS. | `--tls-ca ca.pem` |
| `--tls-cert` | Client certificate PEM for mutual TLS; requires `--tls-key` and enables TLS. | `--tls-cert client.pem --tls-key client-key.pem` |
| `--tls-key` | Client private key PEM; requires `--tls-cert`. | `--tls-key client-key.pem --tls-cert client.pem` |
| `--tls-server-name` | Override the verified TLS hostname; also enables TLS. Default is the original broker hostname. | `--tls-server-name broker.example` |
| `-h`, `--help` | Print help and exit; no topic required. | `/tmp/kcl --help` |
| `--version` | Print the version and exit; no topic required. | `/tmp/kcl --version` |

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

## Record filtering

`-G` / `--grep` filters records locally using Go regular expressions. Match an
exact Kafka key with anchors; no base64 encoding is needed:

```sh
/tmp/kcl -B localhost:9092 events -p 0 -o :end -G 'k:^user-123$'
/tmp/kcl -B localhost:9092 events -o :end -G 'k:^user-123$' -G '!v:warning'
```

Prefixes select the key (`k:`), value (`v:`), header key (`hk:`), header value
(`hv:`), a named header's value (`h:NAME=`), or topic (`t:`). A leading `!`
negates a filter. Repeated flags must all match (AND); use regex alternation
such as `k:^(user-123|user-456)$` for OR. Escape regex metacharacters in literal
keys. Patterns without anchors can match substrings.

Filtering runs on the original bytes before MessagePack decoding and output
formatting. `v:` does not query decoded JSON fields; use a downstream JSON
filter for those. `-n` counts records that pass all `-G` filters. Bounded reads
still finish when the final records do not match, including zero-match reads.
Partition selection and broker fetches are unchanged by these local filters.

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

## Read-only guarantees

kcl only reads records and metadata. A connection-level guard blocks writes,
offset commits, and topic auto-creation. Kafka 0.11 or newer is required.
For independent enforcement, use a dedicated account with only topic
READ/DESCRIBE permissions.

## Development

```sh
go test -race -count=1 ./...
go vet ./...
go build -o /tmp/kcl .
```

Tests use local fake brokers, including three brokers behind TCP forwards,
SASL, TLS hostname checks, offset/timestamp ranges, and forbidden request tests.
They do not require a running Kafka cluster.
