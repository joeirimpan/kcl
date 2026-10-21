package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type recordingConn struct {
	net.Conn
	bytes.Buffer
}

func (c *recordingConn) Read(p []byte) (int, error) {
	return c.Buffer.Read(p)
}

func (c *recordingConn) Write(p []byte) (int, error) {
	return c.Buffer.Write(p)
}

func requestFrame(req kmsg.Request, version int16) []byte {
	req.SetVersion(version)
	return kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
}

func TestReadOnlyGuard(t *testing.T) {
	allowed := map[int16]bool{1: true, 2: true, 3: true, 17: true, 18: true, 23: true, 36: true}
	for key := int16(0); key < 100; key++ {
		req := kmsg.RequestForKey(key)
		if req == nil {
			continue
		}

		for version := int16(0); version <= req.MaxVersion(); version++ {
			t.Run(fmt.Sprintf("api%d/v%d", key, version), func(t *testing.T) {
				if metadata, ok := req.(*kmsg.MetadataRequest); ok {
					metadata.AllowAutoTopicCreation = false
				}

				if epoch, ok := req.(*kmsg.OffsetForLeaderEpochRequest); ok {
					epoch.ReplicaID = -1
				}

				frame := requestFrame(req, version)
				raw := &recordingConn{}
				conn := &readOnlyConn{Conn: raw}
				n, err := conn.Write(frame)
				wantAllowed := allowed[key] && (key != 3 || version >= 4)
				if wantAllowed {
					if err != nil || n != len(frame) || !bytes.Equal(raw.Bytes(), frame) {
						t.Fatalf("allowed write: n=%d, err=%v", n, err)
					}
				} else if err == nil || n != 0 || raw.Len() != 0 {
					t.Fatalf("forbidden request reached connection: n=%d, err=%v", n, err)
				}
			})
		}
	}
}

func TestGuardRejectsUnsafeFrames(t *testing.T) {
	autoCreate := kmsg.NewPtrMetadataRequest()
	autoCreate.AllowAutoTopicCreation = true
	oldFetch := kmsg.NewPtrFetchRequest()
	oldFetch.ReplicaID = 1
	replicaFetch := kmsg.NewPtrFetchRequest()
	replicaFetch.ReplicaState.ID = 1
	replicaOffsets := kmsg.NewPtrListOffsetsRequest()
	replicaOffsets.ReplicaID = 1
	valid := requestFrame(kmsg.NewPtrApiVersionsRequest(), 3)
	unknown := bytes.Clone(valid)
	binary.BigEndian.PutUint16(unknown[4:6], 999)
	invalidVersion := bytes.Clone(valid)
	binary.BigEndian.PutUint16(invalidVersion[6:8], 999)
	truncatedBody := bytes.Clone(valid[:len(valid)-1])
	binary.BigEndian.PutUint32(truncatedBody[:4], uint32(len(truncatedBody)-4))

	frames := [][]byte{
		nil, {1, 2, 3}, valid[:8], valid[:len(valid)-1],
		append(bytes.Clone(valid), valid...), unknown, invalidVersion, truncatedBody,
		requestFrame(autoCreate, 4), requestFrame(autoCreate, autoCreate.MaxVersion()),
		requestFrame(oldFetch, 11), requestFrame(replicaFetch, replicaFetch.MaxVersion()),
		requestFrame(replicaOffsets, replicaOffsets.MaxVersion()),
	}
	for i, frame := range frames {
		raw := &recordingConn{}
		conn := &readOnlyConn{Conn: raw}
		if n, err := conn.Write(frame); err == nil || n != 0 || raw.Len() != 0 {
			t.Fatalf("frame %d reached connection: n=%d, err=%v", i, n, err)
		}
	}
}

func TestParseOffset(t *testing.T) {
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		input string
		start position
		end   *position
	}{
		{"start", position{kind: "start"}, nil},
		{"end", position{kind: "end"}, nil},
		{"end-10", position{kind: "end", value: -10}, nil},
		{"start+10", position{kind: "start", value: 10}, nil},
		{"100:200", position{kind: "exact", value: 100}, &position{kind: "exact", value: 200}},
		{":end", position{kind: "start"}, &position{kind: "end"}},
		{"0:0", position{kind: "exact"}, &position{kind: "exact"}},
		{"@-30m:end", position{kind: "timestamp", value: now.Add(-30 * time.Minute).UnixMilli()}, &position{kind: "end"}},
		{"@2026-10-21T09:15:00+05:30:15m", position{kind: "timestamp", value: time.Date(2026, 10, 21, 3, 45, 0, 0, time.UTC).UnixMilli()}, &position{kind: "timestamp", value: time.Date(2026, 10, 21, 4, 0, 0, 0, time.UTC).UnixMilli()}},
		{"@2026-10-21T09:15:00+05:30:2026-10-21T09:30:00+05:30", position{kind: "timestamp", value: time.Date(2026, 10, 21, 3, 45, 0, 0, time.UTC).UnixMilli()}, &position{kind: "timestamp", value: time.Date(2026, 10, 21, 4, 0, 0, 0, time.UTC).UnixMilli()}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseOffset(tc.input, now)
			if err != nil || !reflect.DeepEqual(got, offsetSpec{start: tc.start, end: tc.end}) {
				t.Fatalf("parse = %+v, %v; want %+v, %+v", got, err, tc.start, tc.end)
			}
		})
	}

	for _, input := range []string{"", "@", "-1", "10:5", ":-1", "end--1", "start+-1", "@2026-10-22:2026-10-21", "bogus", "@1960-01-01", "@12345"} {
		if _, err := parseOffset(input, now); err == nil {
			t.Errorf("accepted invalid offset %q", input)
		}
	}
}

func cleanSASLEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{"KCL_SASL_USER", "KCL_SASL_PASS", "KCL_SASL_MECHANISM"} {
		t.Setenv(key, "")
	}
}

func seedRecords(t *testing.T, brokers []string, opts ...kgo.Opt) {
	t.Helper()

	opts = append(opts, kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	base := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	for p := int32(0); p < 3; p++ {
		for i := 0; i < 5; i++ {
			record := &kgo.Record{Topic: "events", Partition: p, Key: []byte("key"), Value: []byte(fmt.Sprint(i)), Timestamp: base.Add(time.Duration(i) * time.Minute)}
			if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func observeReads(t *testing.T, cluster *kfake.Cluster) *atomic.Int32 {
	t.Helper()

	var fetches atomic.Int32
	cluster.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if err := checkReadRequest(requestFrame(req, req.GetVersion())); err != nil {
			t.Errorf("unsafe request reached broker: %v", err)
		}

		if req.Key() == 1 {
			fetches.Add(1)
		}

		return nil, nil, false
	})
	return &fetches
}

func TestDirectConsume(t *testing.T) {
	cleanSASLEnv(t)
	cluster := kfake.MustCluster(kfake.NumBrokers(3), kfake.SeedTopics(3, "events"), kfake.SeedTopics(1, "empty"), kfake.AllowAutoTopicCreation())
	defer cluster.Close()

	seedRecords(t, cluster.ListenAddrs())
	fetches := observeReads(t, cluster)
	for _, tc := range []struct {
		name        string
		args        []string
		count       int
		wantOffsets []int64
		wantErr     string
	}{
		{"range", []string{"events", "-p", "1", "-o", "1:4"}, 3, []int64{1, 2, 3}, ""},
		{"all partitions", []string{"events", "-o", ":end"}, 15, nil, ""},
		{"bounded count", []string{"events", "-n", "2", "-o", "start"}, 2, nil, ""},
		{"timestamps", []string{"events", "-p", "0", "-o", "@2026-10-21T10:01:00Z:2m"}, 2, []int64{1, 2}, ""},
		{"future timestamp", []string{"events", "-o", "@2027-01-01:end"}, 0, nil, ""},
		{"empty range", []string{"events", "-o", "2:2"}, 0, nil, ""},
		{"empty topic", []string{"empty", "-o", ":end"}, 0, nil, ""},
		{"clamped end", []string{"events", "-p", "2", "-o", "3:100"}, 2, []int64{3, 4}, ""},
		{"idle tail", []string{"events", "--timeout", "100ms"}, 0, nil, ""},
		{"missing partition", []string{"events", "-p", "99"}, 0, nil, "no partition 99"},
		{"missing topic", []string{"missing"}, 0, nil, "UNKNOWN_TOPIC_OR_PARTITION"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			args := append([]string{"-B", strings.Join(cluster.ListenAddrs(), ","), "-f", "json"}, tc.args...)
			var stdout, stderr bytes.Buffer
			err := run(ctx, args, &stdout, &stderr)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want %q", err, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("consume: %v; stderr: %s", err, &stderr)
			}

			var offsets []int64
			decoder := json.NewDecoder(&stdout)
			for {
				var record struct {
					Offset int64 `json:"offset"`
				}
				if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}

				offsets = append(offsets, record.Offset)
			}

			if len(offsets) != tc.count || tc.wantOffsets != nil && !reflect.DeepEqual(offsets, tc.wantOffsets) {
				t.Fatalf("offsets = %v; want count %d, offsets %v", offsets, tc.count, tc.wantOffsets)
			}
		})
	}

	if fetches.Load() == 0 {
		t.Fatal("no fetch requests observed")
	}

	if got := cluster.PartitionInfos("missing"); len(got) != 0 {
		t.Fatal("consuming a missing topic created it")
	}
}

func TestBrokerMapThreeBrokers(t *testing.T) {
	cleanSASLEnv(t)
	t.Setenv("KCL_SASL_USER", "test")
	t.Setenv("KCL_SASL_PASS", "test-pass")
	var targets []string
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3), kfake.SeedTopics(3, "events"),
		kfake.EnableSASL(), kfake.Superuser("PLAIN", "test", "test-pass"),
		kfake.ListenFn(func(network, address string) (net.Listener, error) {
			listener, err := net.Listen(network, address)
			if err != nil {
				return nil, err
			}

			targets = append(targets, listener.Addr().String())
			advertised := &net.TCPAddr{IP: net.IPv4(192, 0, 2, byte(len(targets))), Port: 9092}
			return advertisedBrokerListener{Listener: listener, address: advertised}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	for p := int32(0); p < 3; p++ {
		if err := cluster.MoveTopicPartition("events", p, p); err != nil {
			t.Fatal(err)
		}
	}

	addresses := cluster.ListenAddrs()
	var flags, forwards []string
	mappings := make(map[string]string)
	var counters []*atomic.Int32
	for i, target := range targets {
		forward, counter := forwardBroker(t, target)
		flags = append(flags, "--broker-map", addresses[i]+"="+forward)
		mappings[addresses[i]] = forward
		forwards = append(forwards, forward)
		counters = append(counters, counter)
	}

	seedRecords(t, addresses, kgo.Dialer(newBrokerDialer(mappings, nil, time.Second)), kgo.SASL(plain.Auth{User: "test", Pass: "test-pass"}.AsMechanism()))
	for _, counter := range counters {
		counter.Store(0)
	}

	observeReads(t, cluster)
	for _, seed := range []string{addresses[0], forwards[0]} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		args := append([]string{"-B", seed, "events", "--sasl-mechanism", "plain", "-o", ":end"}, flags...)
		var stdout bytes.Buffer
		err := run(ctx, args, &stdout, io.Discard)
		cancel()
		if err != nil {
			t.Fatal(err)
		}

		if count := bytes.Count(stdout.Bytes(), []byte{'\n'}); count != 15 {
			t.Fatalf("consumed %d records, want 15", count)
		}
	}

	for i, counter := range counters {
		if counter.Load() == 0 {
			t.Errorf("consumer did not connect to broker %s through its forward", addresses[i])
		}
	}
}

func TestMessagePackOutput(t *testing.T) {
	packed := msgp.AppendMapHeader(nil, 1)
	packed = msgp.AppendString(packed, "n")
	packed = msgp.AppendUint64(packed, 18446744073709551615)
	for _, tc := range []struct {
		name    string
		value   []byte
		want    string
		warning bool
	}{
		{"decoded", packed, `{"n":18446744073709551615}`, false},
		{"tombstone", nil, "null", false},
		{"invalid", []byte{0xc1}, `"wQ=="`, true},
		{"empty", []byte{}, `""`, true},
		{"trailing bytes", []byte{1, 2}, `"AQI="`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			output := recordOutput{writer: &stdout, errors: &stderr, msgpack: true}
			record := &kgo.Record{Topic: "events", Key: []byte("key"), Value: tc.value}
			for range 2 {
				if err := output.write(record, &kgo.FetchPartition{}); err != nil {
					t.Fatal(err)
				}
			}

			var result struct {
				Key   []byte
				Value json.RawMessage
			}
			if err := json.NewDecoder(&stdout).Decode(&result); err != nil {
				t.Fatal(err)
			}

			if string(result.Value) != tc.want || string(result.Key) != "key" || !bytes.Equal(record.Value, tc.value) {
				t.Fatalf("unexpected output %s, key %q, original %x", result.Value, result.Key, record.Value)
			}

			if got := bytes.Count(stderr.Bytes(), []byte{'\n'}); got != 0 && !tc.warning || got != 1 && tc.warning {
				t.Fatalf("unexpected warnings: %s", &stderr)
			}
		})
	}
}

func TestRejectGroupAndInvalidOptions(t *testing.T) {
	cleanSASLEnv(t)
	for _, args := range [][]string{
		{}, {"events", "--group", "group"}, {"events", "-g", "group"},
		{"events", "--share-group", "group"}, {"events", "--commit"},
		{"events", "-X", "group.id=group"}, {"events", "--transactional-id", "tx"},
		{"events", "-p", "-1"}, {"events", "-n", "-1"},
		{"events", "--timeout", "-1s"}, {"events", "--broker-map", "bad"},
		{"events", "--tls-cert", "cert"}, {"events", "--sasl-mechanism", "plain"},
	} {
		if err := run(t.Context(), args, io.Discard, io.Discard); err == nil {
			t.Errorf("accepted invalid options: %v", args)
		}
	}
}

func TestGuardBlocksAccidentalProducer(t *testing.T) {
	cleanSASLEnv(t)
	cluster := kfake.MustCluster(kfake.SeedTopics(1, "events"))
	defer cluster.Close()

	var mu sync.Mutex
	var keys []int16
	cluster.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		mu.Lock()
		keys = append(keys, req.Key())
		mu.Unlock()
		return nil, nil, false
	})
	cl, err := newConsumer(options{brokers: cluster.ListenAddrs()})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err = cl.ProduceSync(ctx, &kgo.Record{Topic: "events", Value: []byte("must not arrive")}).FirstErr()
	cl.Close()
	if err == nil {
		t.Fatal("guarded client unexpectedly produced a record")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, key := range keys {
		if key == 0 || key == 22 {
			t.Fatalf("write API %d reached the broker", key)
		}
	}
}

func TestTLSGuard(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	cluster := kfake.MustCluster(kfake.TLS(&tls.Config{Certificates: server.TLS.Certificates}), kfake.SeedTopics(1, "events"))
	defer cluster.Close()

	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}

	observeReads(t, cluster)
	cl, err := newConsumer(options{brokers: cluster.ListenAddrs(), ca: ca, serverName: "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	if _, err := req.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}

	writeCtx, cancelWrite := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelWrite()

	if _, err := kmsg.NewPtrProduceRequest().RequestWith(writeCtx, cl); err == nil {
		t.Fatal("produce request passed through TLS guard")
	}
}

func TestIncompleteBoundedReadFails(t *testing.T) {
	cleanSASLEnv(t)
	cluster := kfake.MustCluster(kfake.SeedTopics(3, "events"), kfake.BlackholeProduce())
	defer cluster.Close()

	seedRecords(t, cluster.ListenAddrs())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := run(ctx, []string{"-B", cluster.ListenAddrs()[0], "events", "-o", ":end", "--timeout", "100ms"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "idle timeout before reaching") {
		t.Fatalf("incomplete read returned %v", err)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestOutputFailure(t *testing.T) {
	cleanSASLEnv(t)
	cluster := kfake.MustCluster(kfake.SeedTopics(3, "events"))
	defer cluster.Close()

	seedRecords(t, cluster.ListenAddrs())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := run(ctx, []string{"-B", cluster.ListenAddrs()[0], "events", "-o", "start"}, failedWriter{}, io.Discard)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("output failure returned %v", err)
	}
}
