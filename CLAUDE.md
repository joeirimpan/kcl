# kcl development context

This repository is a direct Kafka consumer. Keep production Go code in
`consume.go`; tests may use separate `*_test.go` files. Do not add producing,
admin commands, groups, commits, or general client option injection.

Every connection must retain the read-only request guard, including TLS and
mapped broker connections. Metadata must disable topic auto-creation. Unknown
APIs and unsupported protocol encodings must fail closed. Keep broker-map
addresses, examples, and tests generic.

Use explicit partition assignments. Offset bounds are per partition with an
exclusive end; bounded reads snapshot the available log at startup. Raw keys
and values must survive failed decoding. Propagate output errors.

Separate distinct logical steps with a blank line. Keep operations with their
immediate error checks. Follow the Go comment style skill when editing comments,
and run gofmt. Test against local kfake brokers; never use production clusters
for verification. Run `go test -race -count=1 ./...`, `go vet ./...`, and build
`/tmp/kcl`. Keep the version variable compatible with release.sh and Dockerfile.

The maintainer updates CHANGELOG.md at release time. Do not rewrite historical
release notes as part of a code change. Do not copy BSL-licensed rpk code.
