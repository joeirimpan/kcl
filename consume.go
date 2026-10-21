package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/tinylib/msgp/msgp"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var version = "dev"

type options struct {
	brokers, mappings, topics []string
	partitions                []int32
	offset, format            string
	num                       int
	timeout                   time.Duration
	msgpack                   bool
	mechanism, user, password string
	tls                       bool
	ca, cert, key, serverName string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "kcl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var o options
	var help, showVersion bool
	flags := pflag.NewFlagSet("kcl", pflag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringSliceVarP(&o.brokers, "brokers", "B", []string{"localhost:9092"}, "Bootstrap brokers, comma separated")
	flags.StringArrayVar(&o.mappings, "broker-map", nil, "Advertised HOST:PORT=forwarded HOST:PORT (repeatable)")
	flags.Int32SliceVarP(&o.partitions, "partitions", "p", nil, "Partition IDs, comma separated (default all)")
	flags.StringVarP(&o.offset, "offset", "o", "end", "start, end, N, N:M, :end, @timestamp, @T1:T2, @-30m:end")
	flags.IntVarP(&o.num, "num", "n", 0, "Stop after this many records (0 means unlimited)")
	flags.DurationVar(&o.timeout, "timeout", 30*time.Second, "Idle timeout (0 disables; incomplete bounded reads fail)")
	flags.StringVarP(&o.format, "format", "f", "%v\\n", "Record format, e.g. %k\\t%v\\n, or json")
	flags.BoolVar(&o.msgpack, "msgpack", false, "Decode MessagePack values as JSON")
	flags.StringVar(&o.mechanism, "sasl-mechanism", os.Getenv("KCL_SASL_MECHANISM"), "plain, scram-sha-256, scram-sha-512 (or KCL_SASL_MECHANISM)")
	flags.StringVar(&o.user, "sasl-user", os.Getenv("KCL_SASL_USER"), "SASL username (or KCL_SASL_USER; password from KCL_SASL_PASS)")
	flags.BoolVar(&o.tls, "tls", false, "Enable TLS with certificate verification")
	flags.StringVar(&o.ca, "tls-ca", "", "PEM CA bundle (enables TLS)")
	flags.StringVar(&o.cert, "tls-cert", "", "Client certificate PEM (enables TLS)")
	flags.StringVar(&o.key, "tls-key", "", "Client private key PEM")
	flags.StringVar(&o.serverName, "tls-server-name", "", "Override the TLS server name (enables TLS)")
	flags.BoolVarP(&help, "help", "h", false, "Show help")
	flags.BoolVar(&showVersion, "version", false, "Show version")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if help {
		fmt.Fprint(stdout, "Usage: kcl [flags] TOPIC [TOPIC...]\n\nDirect consumer only. No groups, commits, producing, or admin commands.\n")
		flags.SetOutput(stdout)
		flags.PrintDefaults()
		return nil
	}

	if showVersion {
		_, err := fmt.Fprintln(stdout, "kcl consume-only", version)
		return err
	}

	o.topics = flags.Args()
	o.password = os.Getenv("KCL_SASL_PASS")
	if len(o.topics) == 0 {
		return errors.New("at least one topic is required; see --help")
	}

	if o.num < 0 || o.timeout < 0 {
		return errors.New("--num and --timeout must not be negative")
	}

	for _, p := range o.partitions {
		if p < 0 {
			return errors.New("partition IDs must not be negative")
		}
	}

	spec, err := parseOffset(o.offset, time.Now())
	if err != nil {
		return err
	}

	var formatter *kgo.RecordFormatter
	if o.format != "json" {
		formatter, err = kgo.NewRecordFormatter(o.format)
		if err != nil {
			return err
		}
	}

	cl, err := newConsumer(o)
	if err != nil {
		return err
	}
	defer cl.Close()

	setupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	bounds, err := resolveBounds(setupCtx, cl, o, spec)
	cancel()
	if err != nil {
		return err
	}

	assignments := make(map[string]map[int32]kgo.Offset)
	for tp, b := range bounds {
		if b.end >= 0 && b.start >= b.end {
			delete(bounds, tp)
			continue
		}

		if assignments[tp.topic] == nil {
			assignments[tp.topic] = make(map[int32]kgo.Offset)
		}

		assignments[tp.topic][tp.partition] = kgo.NewOffset().At(b.start)
	}

	if len(bounds) == 0 {
		return nil
	}

	cl.AddConsumePartitions(assignments)
	output := recordOutput{writer: stdout, errors: stderr, formatter: formatter, msgpack: o.msgpack}
	return consume(ctx, cl, bounds, o, &output)
}

func newConsumer(o options) (*kgo.Client, error) {
	mappings, err := parseBrokerMaps(o.mappings)
	if err != nil {
		return nil, err
	}

	config, err := consumerTLS(o)
	if err != nil {
		return nil, err
	}

	dial := newBrokerDialer(mappings, config, 10*time.Second)
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.brokers...),
		kgo.ClientID("kcl-consume/" + version),
		kgo.DisableClientMetrics(),
		kgo.KeepControlRecords(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}

			// Check Kafka frames before the TLS connection encrypts them.
			return &readOnlyConn{Conn: conn}, nil
		}),
	}

	mechanism := strings.ToLower(o.mechanism)
	if mechanism == "" && (o.user != "" || o.password != "") {
		mechanism = "plain"
	}

	if mechanism != "" {
		if o.user == "" || o.password == "" {
			return nil, errors.New("SASL requires a username and KCL_SASL_PASS")
		}

		switch mechanism {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{User: o.user, Pass: o.password}.AsMechanism()))
		case "scram-sha-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: o.user, Pass: o.password}.AsSha256Mechanism()))
		case "scram-sha-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: o.user, Pass: o.password}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism %q", mechanism)
		}
	}

	return kgo.NewClient(opts...)
}

func consumerTLS(o options) (*tls.Config, error) {
	if (o.cert == "") != (o.key == "") {
		return nil, errors.New("--tls-cert and --tls-key must be supplied together")
	}

	if !o.tls && o.ca == "" && o.cert == "" && o.serverName == "" {
		return nil, nil
	}

	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.serverName}
	if o.ca != "" {
		pem, err := os.ReadFile(o.ca)
		if err != nil {
			return nil, err
		}

		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("no CA certificates found in --tls-ca")
		}
	}

	if o.cert != "" {
		cert, err := tls.LoadX509KeyPair(o.cert, o.key)
		if err != nil {
			return nil, err
		}

		config.Certificates = []tls.Certificate{cert}
	}

	return config, nil
}

type readOnlyConn struct {
	net.Conn
}

func (c *readOnlyConn) Write(frame []byte) (int, error) {
	if err := checkReadRequest(frame); err != nil {
		return 0, err
	}

	return c.Conn.Write(frame)
}

func checkReadRequest(frame []byte) error {
	// franz-go writes one complete request per call. Reject partial or combined frames.
	if len(frame) < 14 || uint64(binary.BigEndian.Uint32(frame[:4])) != uint64(len(frame)-4) {
		return errors.New("read-only guard: invalid Kafka frame")
	}

	key := int16(binary.BigEndian.Uint16(frame[4:6]))
	var req kmsg.Request
	switch key {
	case 1:
		req = kmsg.NewPtrFetchRequest()
	case 2:
		req = kmsg.NewPtrListOffsetsRequest()
	case 3:
		req = kmsg.NewPtrMetadataRequest()
	case 17:
		req = kmsg.NewPtrSASLHandshakeRequest()
	case 18:
		req = kmsg.NewPtrApiVersionsRequest()
	case 23:
		req = kmsg.NewPtrOffsetForLeaderEpochRequest()
	case 36:
		req = kmsg.NewPtrSASLAuthenticateRequest()
	default:
		return fmt.Errorf("read-only guard: Kafka API %d is forbidden", key)
	}

	v := int16(binary.BigEndian.Uint16(frame[6:8]))
	if v < 0 || v > req.MaxVersion() || key == 3 && v < 4 {
		return fmt.Errorf("read-only guard: unsupported Kafka API %d version %d", key, v)
	}

	req.SetVersion(v)
	clientLen := int(int16(binary.BigEndian.Uint16(frame[12:14])))
	if clientLen < -1 || clientLen > len(frame)-14 {
		return errors.New("read-only guard: invalid client ID")
	}

	body := frame[14+max(clientLen, 0):]
	if req.IsFlexible() {
		// Our client does not send request header tags.
		if len(body) == 0 || body[0] != 0 {
			return errors.New("read-only guard: unsupported request header tags")
		}

		body = body[1:]
	}

	if err := req.ReadFrom(body); err != nil {
		return fmt.Errorf("read-only guard: invalid request: %w", err)
	}

	if !bytes.Equal(req.AppendTo(nil), body) {
		return errors.New("read-only guard: noncanonical request body")
	}

	switch r := req.(type) {
	case *kmsg.MetadataRequest:
		if r.AllowAutoTopicCreation {
			return errors.New("read-only guard: topic auto-creation is forbidden")
		}
	case *kmsg.FetchRequest:
		if r.ReplicaID != -1 || r.ReplicaState.ID != -1 {
			return errors.New("read-only guard: replica fetch is forbidden")
		}
	case *kmsg.ListOffsetsRequest:
		if r.ReplicaID != -1 {
			return errors.New("read-only guard: replica offset lookup is forbidden")
		}
	case *kmsg.OffsetForLeaderEpochRequest:
		if v >= 3 && r.ReplicaID != -1 {
			return errors.New("read-only guard: replica epoch lookup is forbidden")
		}
	}

	return nil
}

type position struct {
	kind  string
	value int64
}

type offsetSpec struct {
	start position
	end   *position
}

func parseOffset(s string, now time.Time) (offsetSpec, error) {
	timestamp := strings.HasPrefix(s, "@")
	s = strings.TrimPrefix(s, "@")
	if p, err := parsePosition(s, timestamp, now); err == nil {
		return offsetSpec{start: p}, nil
	}

	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != ':' {
			continue
		}

		start := position{kind: "start"}
		var err error
		if s[:i] != "" {
			start, err = parsePosition(s[:i], timestamp, now)
			if err != nil {
				continue
			}
		}

		if s[i+1:] == "" {
			return offsetSpec{start: start}, nil
		}

		base := now
		if start.kind == "timestamp" && !strings.HasPrefix(s[i+1:], "-") {
			base = time.UnixMilli(start.value)
		}

		end, err := parsePosition(s[i+1:], timestamp, base)
		if err != nil {
			continue
		}

		if start.kind == end.kind && (start.kind == "exact" || start.kind == "timestamp") && end.value < start.value {
			return offsetSpec{}, errors.New("range end must be at or after its start")
		}

		return offsetSpec{start: start, end: &end}, nil
	}

	return offsetSpec{}, fmt.Errorf("invalid offset %q; use start, end, N, N:M, :end, @timestamp, or @T1:T2", s)
}

func parsePosition(s string, timestamp bool, now time.Time) (position, error) {
	if s == "start" || s == "end" {
		return position{kind: s}, nil
	}

	if !timestamp {
		if strings.HasPrefix(s, "end-") || strings.HasPrefix(s, "start+") {
			kind, value, _ := strings.Cut(s, "+")
			if strings.HasPrefix(s, "end-") {
				kind, value = "end", s[3:]
			}

			n, err := strconv.ParseInt(value, 10, 64)
			if err == nil && (kind == "end" && n <= 0 || kind == "start" && n >= 0) {
				return position{kind: kind, value: n}, nil
			}

			return position{}, errors.New("invalid relative offset")
		}

		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil && n >= 0 {
			return position{kind: "exact", value: n}, nil
		}

		return position{}, errors.New("expected a nonnegative offset")
	}

	for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil && t.UnixMilli() >= 0 {
			return position{kind: "timestamp", value: t.UnixMilli()}, nil
		}
	}

	if d, err := time.ParseDuration(s); err == nil && now.Add(d).UnixMilli() >= 0 {
		return position{kind: "timestamp", value: now.Add(d).UnixMilli()}, nil
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err == nil && n >= 0 {
		switch len(s) {
		case 9, 10:
			return position{kind: "timestamp", value: n * 1000}, nil
		case 13:
			return position{kind: "timestamp", value: n}, nil
		case 19:
			return position{kind: "timestamp", value: n / 1_000_000}, nil
		}
	}

	return position{}, errors.New("invalid timestamp")
}

type topicPartition struct {
	topic     string
	partition int32
}

type offsetBounds struct {
	start, end int64
}

func resolveBounds(ctx context.Context, cl *kgo.Client, o options, spec offsetSpec) (map[topicPartition]offsetBounds, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	for _, topic := range o.topics {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = &topic
		req.Topics = append(req.Topics, t)
	}

	meta, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}

	var partitions []topicPartition
	for _, topic := range meta.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return nil, fmt.Errorf("topic %s: %w", *topic.Topic, err)
		}

		found := make(map[int32]bool)
		for _, p := range topic.Partitions {
			wanted := len(o.partitions) == 0
			for _, selected := range o.partitions {
				wanted = wanted || p.Partition == selected
			}

			if !wanted {
				continue
			}

			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("%s[%d]: %w", *topic.Topic, p.Partition, err)
			}

			found[p.Partition] = true
			partitions = append(partitions, topicPartition{*topic.Topic, p.Partition})
		}

		for _, p := range o.partitions {
			if !found[p] {
				return nil, fmt.Errorf("topic %s has no partition %d", *topic.Topic, p)
			}
		}
	}

	if len(partitions) == 0 {
		return nil, errors.New("no partitions found")
	}

	earliest, err := listOffsets(ctx, cl, partitions, -2)
	if err != nil {
		return nil, err
	}

	latest, err := listOffsets(ctx, cl, partitions, -1)
	if err != nil {
		return nil, err
	}

	resolve := func(p position) (map[topicPartition]int64, error) {
		if p.kind == "timestamp" {
			return listOffsets(ctx, cl, partitions, p.value)
		}

		result := make(map[topicPartition]int64)
		for _, tp := range partitions {
			value := p.value
			switch p.kind {
			case "start":
				value = earliest[tp] + min(p.value, latest[tp]-earliest[tp])
			case "end":
				value = latest[tp] + max(p.value, -latest[tp])
			}

			if value < 0 {
				value = 0
			}

			result[tp] = value
		}

		return result, nil
	}

	starts, err := resolve(spec.start)
	if err != nil {
		return nil, err
	}

	var ends map[topicPartition]int64
	if spec.end != nil {
		ends, err = resolve(*spec.end)
		if err != nil {
			return nil, err
		}
	}

	result := make(map[topicPartition]offsetBounds)
	for _, tp := range partitions {
		start := starts[tp]
		if start == -1 {
			start = latest[tp]
		}

		end := int64(-1)
		if spec.end != nil {
			end = ends[tp]
			if end == -1 || end > latest[tp] {
				end = latest[tp]
			}
		}

		result[tp] = offsetBounds{start: max(earliest[tp], min(start, latest[tp])), end: end}
	}

	return result, nil
}

func listOffsets(ctx context.Context, cl *kgo.Client, partitions []topicPartition, timestamp int64) (map[topicPartition]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	topics := make(map[string][]kmsg.ListOffsetsRequestTopicPartition)
	for _, tp := range partitions {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = tp.partition
		p.Timestamp = timestamp
		topics[tp.topic] = append(topics[tp.topic], p)
	}

	for topic, partitions := range topics {
		req.Topics = append(req.Topics, kmsg.ListOffsetsRequestTopic{Topic: topic, Partitions: partitions})
	}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}

	result := make(map[topicPartition]int64)
	for _, topic := range resp.Topics {
		for _, p := range topic.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("%s[%d] offsets: %w", topic.Topic, p.Partition, err)
			}

			result[topicPartition{topic.Topic, p.Partition}] = p.Offset
		}
	}

	for _, tp := range partitions {
		if _, ok := result[tp]; !ok {
			return nil, fmt.Errorf("missing offset for %s[%d]", tp.topic, tp.partition)
		}
	}

	return result, nil
}

func consume(ctx context.Context, cl *kgo.Client, bounds map[topicPartition]offsetBounds, o options, output *recordOutput) error {
	printed := 0
	lastRecord := time.Now()
	for len(bounds) > 0 {
		pollCtx := ctx
		cancel := func() {}
		if o.timeout > 0 {
			pollCtx, cancel = context.WithDeadline(ctx, lastRecord.Add(o.timeout))
		}

		fetches := cl.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}

		for _, err := range fetches.Errors() {
			if errors.Is(err.Err, context.DeadlineExceeded) && o.timeout > 0 {
				for _, b := range bounds {
					if b.end >= 0 {
						return errors.New("idle timeout before reaching the requested end offsets")
					}
				}

				return nil
			}

			return fmt.Errorf("%s[%d]: %w", err.Topic, err.Partition, err.Err)
		}

		for _, fetch := range fetches {
			for _, topic := range fetch.Topics {
				for _, p := range topic.Partitions {
					tp := topicPartition{topic.Topic, p.Partition}
					b, ok := bounds[tp]
					if !ok {
						continue
					}

					for _, r := range p.Records {
						lastRecord = time.Now()
						if r.Offset < b.start || b.end >= 0 && r.Offset >= b.end || r.Attrs.IsControl() {
							continue
						}

						if err := output.write(r, &p); err != nil {
							return err
						}

						printed++
						if o.num > 0 && printed >= o.num {
							return nil
						}
					}

					if n := len(p.Records); b.end >= 0 && n > 0 && p.Records[n-1].Offset+1 >= b.end {
						cl.RemoveConsumePartitions(map[string][]int32{tp.topic: {tp.partition}})
						delete(bounds, tp)
					}
				}
			}
		}
	}

	return nil
}

type recordOutput struct {
	writer, errors  io.Writer
	formatter       *kgo.RecordFormatter
	msgpack, warned bool
}

func (o *recordOutput) write(record *kgo.Record, partition *kgo.FetchPartition) error {
	r := *record
	decoded := false
	if o.msgpack && r.Value != nil {
		value, err := decodeMessagePack(r.Value)
		if err == nil {
			r.Value, decoded = value, true
		} else if !o.warned {
			fmt.Fprintf(o.errors, "unable to decode MessagePack value; preserving raw bytes: %v (further decode errors suppressed)\n", err)
			o.warned = true
		}
	}

	var data []byte
	if o.formatter != nil {
		data = o.formatter.AppendPartitionRecord(nil, partition, &r)
	} else {
		var value any = r.Value
		if decoded {
			value = json.RawMessage(r.Value)
		}

		result := struct {
			Topic     string             `json:"topic"`
			Partition int32              `json:"partition"`
			Offset    int64              `json:"offset"`
			Timestamp time.Time          `json:"timestamp"`
			Key       []byte             `json:"key"`
			Value     any                `json:"value"`
			Headers   []kgo.RecordHeader `json:"headers,omitempty"`
		}{r.Topic, r.Partition, r.Offset, r.Timestamp, r.Key, value, r.Headers}
		var err error
		data, err = json.Marshal(result)
		if err != nil {
			return err
		}

		data = append(data, '\n')
	}

	n, err := o.writer.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}

	return err
}

func parseBrokerMaps(entries []string) (map[string]string, error) {
	mappings := make(map[string]string, len(entries))
	for _, entry := range entries {
		source, target, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid --broker-map %q: expected HOST:PORT=HOST:PORT", entry)
		}

		source = strings.TrimSpace(source)
		target = strings.TrimSpace(target)
		for _, address := range []string{source, target} {
			if err := checkBrokerMapAddress(address); err != nil {
				return nil, fmt.Errorf("invalid --broker-map %q: %w", entry, err)
			}
		}

		if _, exists := mappings[source]; exists {
			return nil, fmt.Errorf("duplicate --broker-map for %q", source)
		}

		mappings[source] = target
	}

	return mappings, nil
}

func checkBrokerMapAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", address, err)
	}

	if host == "" || strings.ContainsAny(host, " \t\r\n/\\=") {
		return fmt.Errorf("invalid host in %q", address)
	}

	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("invalid port in %q: expected 1-65535", address)
	}

	return nil
}

func newBrokerDialer(mappings map[string]string, tlsConfig *tls.Config, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	dialer := &net.Dialer{Timeout: timeout}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		target := address
		if mapped, ok := mappings[address]; ok {
			target = mapped
		}

		if tlsConfig == nil {
			return dialer.DialContext(ctx, network, target)
		}

		config := tlsConfig.Clone()
		if config.ServerName == "" {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}

			// Verify the broker's identity even when dialing a local tunnel.
			config.ServerName = host
		}

		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: config}
		return tlsDialer.DialContext(ctx, network, target)
	}
}

func decodeMessagePack(b []byte) ([]byte, error) {
	rest, err := msgp.Skip(b)
	if err != nil {
		return nil, err
	}

	if len(rest) != 0 {
		return nil, fmt.Errorf("trailing bytes after MessagePack value")
	}

	var buf bytes.Buffer
	if _, err := msgp.UnmarshalAsJSON(&buf, b); err != nil {
		return nil, err
	}

	if !json.Valid(buf.Bytes()) {
		return nil, fmt.Errorf("MessagePack value cannot be represented as valid JSON")
	}

	return buf.Bytes(), nil
}
