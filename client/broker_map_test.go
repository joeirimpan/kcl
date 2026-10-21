package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseBrokerMaps(t *testing.T) {
	got, err := parseBrokerMaps([]string{
		"broker.example:9092=127.0.0.1:19092",
		" 192.0.2.1:9092 = localhost:19093 ",
		"[2001:db8::1]:9092=[::1]:19094",
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"broker.example:9092": "127.0.0.1:19092",
		"192.0.2.1:9092":      "localhost:19093",
		"[2001:db8::1]:9092":  "[::1]:19094",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mappings = %v, want %v", got, want)
	}

	for _, entries := range [][]string{
		{"broker:9092"},
		{"=localhost:19092"},
		{"broker:9092="},
		{"broker=localhost:19092"},
		{"broker:9092=localhost"},
		{":9092=localhost:19092"},
		{"broker:9092=:19092"},
		{"broker:0=localhost:19092"},
		{"broker:9092=localhost:65536"},
		{"broker:kafka=localhost:19092"},
		{"broker:9092=localhost:-1"},
		{"broker:9092=localhost:19092=extra"},
		{"bad host:9092=localhost:19092"},
		{"tcp://broker:9092=localhost:19092"},
		{"2001:db8::1:9092=localhost:19092"},
		{"broker:9092=localhost:19092", "broker:9092=localhost:19093"},
	} {
		t.Run(strings.Join(entries, ","), func(t *testing.T) {
			if _, err := parseBrokerMaps(entries); err == nil {
				t.Fatal("expected an invalid mapping error")
			}
		})
	}
}

func TestBrokerMapTLS(t *testing.T) {
	serverNames := make(chan string, 10)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			serverNames <- hello.ServerName
			return nil, nil
		},
	}
	server.StartTLS()
	defer server.Close()

	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())

	for _, tc := range []struct {
		name       string
		address    string
		serverName string
		wantSNI    string
		wantErr    bool
	}{
		{"original name", "broker.example.com:9092", "", "broker.example.com", false},
		{"explicit name", "broker.invalid:9092", "example.com", "example.com", false},
		{"wrong name", "broker.invalid:9092", "", "broker.invalid", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &tls.Config{RootCAs: roots, ServerName: tc.serverName}
			dial := newBrokerDialer(map[string]string{tc.address: server.Listener.Addr().String()}, config, time.Second)
			conn, err := dial(t.Context(), "tcp", tc.address)
			if conn != nil {
				conn.Close()
			}

			if (err != nil) != tc.wantErr {
				t.Fatalf("dial error = %v, wantErr = %v", err, tc.wantErr)
			}

			if got := <-serverNames; got != tc.wantSNI {
				t.Errorf("SNI = %q, want %q", got, tc.wantSNI)
			}

			if config.ServerName != tc.serverName {
				t.Errorf("shared TLS config was modified: %q", config.ServerName)
			}
		})
	}
}

func TestBrokerMapDialCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, config := range []*tls.Config{nil, {ServerName: "example.com"}} {
		dial := newBrokerDialer(map[string]string{"broker.invalid:9092": "127.0.0.1:1"}, config, time.Second)
		conn, err := dial(ctx, "tcp", "broker.invalid:9092")
		if conn != nil {
			conn.Close()
		}

		if !errors.Is(err, context.Canceled) {
			t.Errorf("dial error = %v, want cancellation", err)
		}
	}
}

func TestBrokerMapHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
		close(accepted)
	}()
	defer func() {
		listener.Close()
		if conn := <-accepted; conn != nil {
			conn.Close()
		}
	}()

	dial := newBrokerDialer(map[string]string{"broker.invalid:9092": listener.Addr().String()}, &tls.Config{}, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	conn, err := dial(ctx, "tcp", "broker.invalid:9092")
	if conn != nil {
		conn.Close()
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() || ctx.Err() != nil {
		t.Fatalf("expected dial timeout before context deadline, got %v (context %v)", err, ctx.Err())
	}
}

type advertisedBrokerListener struct {
	net.Listener
	address net.Addr
}

func (l advertisedBrokerListener) Addr() net.Addr { return l.address }

// forwardBroker carries TCP traffic like an SSH local forward.
func forwardBroker(t *testing.T, target string) (string, *atomic.Int32) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	var connections atomic.Int32
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()

		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}

			connections.Add(1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer incoming.Close()

				stop := context.AfterFunc(ctx, func() { incoming.Close() })
				defer stop()

				outgoing, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
				if err != nil {
					return
				}
				defer outgoing.Close()

				done := make(chan struct{})
				go func() {
					io.Copy(outgoing, incoming)
					outgoing.Close()
					close(done)
				}()
				io.Copy(incoming, outgoing)
				incoming.Close()
				<-done
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		workers.Wait()
	})

	return listener.Addr().String(), &connections
}

func TestBrokerMapThreeBrokers(t *testing.T) {
	var targets []string
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3),
		kfake.SeedTopics(3, "mapped"),
		kfake.EnableSASL(),
		kfake.Superuser("PLAIN", "test", "test-pass"),
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

	addresses := cluster.ListenAddrs()
	var mappings, forwards []string
	var counters []*atomic.Int32
	for i, target := range targets {
		forward, counter := forwardBroker(t, target)
		mappings = append(mappings, "--broker-map", addresses[i]+"="+forward)
		forwards = append(forwards, forward)
		counters = append(counters, counter)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	for _, seed := range []string{addresses[0], forwards[0]} {
		t.Run(seed, func(t *testing.T) {
			root := &cobra.Command{Use: "kcl"}
			client := New(root)
			args := append([]string{
				"--no-config-file", "--config-env-prefix", "KCL_BROKER_MAP_TEST_",
				"-B", seed,
				"-X", "sasl.mechanism=plain", "-X", "sasl.user=test", "-X", "sasl.pass=test-pass",
			}, mappings...)
			if err := root.ParseFlags(args); err != nil {
				t.Fatal(err)
			}

			client.AddOpt(kgo.RecordPartitioner(kgo.ManualPartitioner()))
			client.AddOpt(kgo.ConsumeTopics("mapped"))
			cl := client.Client()
			defer cl.Close()

			meta, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}

			leaders := make(map[int32]bool)
			for _, topic := range meta.Topics {
				for _, partition := range topic.Partitions {
					leaders[partition.Leader] = true
				}
			}
			if len(leaders) != 3 {
				t.Fatalf("expected partitions on three brokers, got leaders %v", leaders)
			}

			for partition := range int32(3) {
				record := &kgo.Record{Topic: "mapped", Partition: partition, Value: []byte(fmt.Sprint(partition))}
				if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}

			seen := make(map[int32]bool)
			for len(seen) < 3 {
				fetches := cl.PollFetches(ctx)
				if errs := fetches.Errors(); len(errs) > 0 {
					t.Fatal(errs)
				}

				fetches.EachRecord(func(record *kgo.Record) {
					if string(record.Value) != fmt.Sprint(record.Partition) {
						t.Errorf("partition %d value = %q", record.Partition, record.Value)
					}

					seen[record.Partition] = true
				})
			}
		})
	}

	for i, counter := range counters {
		if counter.Load() == 0 {
			t.Errorf("broker %s was never reached through its forward", addresses[i])
		}
	}
}
