package consume

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
	"github.com/twmb/franz-go/pkg/kgo"
)

func wrappedMessagePack(t *testing.T) []byte {
	t.Helper()

	b, err := hex.DecodeString("82a46d65746182a56d74797065cd0190aa726571756573745f6964c420" +
		strings.Repeat("00", 32) + "a46461746182a86f726465725f6964a3616263b07472616e73616374696f6e5f7479706500")
	if err != nil {
		t.Fatal(err)
	}

	return b
}

func TestDecodeMessagePack(t *testing.T) {
	stamp := time.Date(2026, 10, 21, 10, 0, 0, 123, time.UTC)
	timestamp := msgp.AppendTime([]byte{0x81, 0xa4, 'T', 'i', 'm', 'e'}, stamp)

	for _, tc := range []struct {
		name string
		in   []byte
		want string
	}{
		{"nested wrapper", wrappedMessagePack(t), `{"meta":{"mtype":400,"request_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},"data":{"order_id":"abc","transaction_type":0}}`},
		{"timestamp", timestamp, `{"Time":"` + stamp.Local().Format(time.RFC3339Nano) + `"}`},
		{"array", []byte{0x93, 0xc0, 0xc3, 0xa1, 'x'}, `[null,true,"x"]`},
		{"unsigned integer", msgp.AppendUint64(nil, math.MaxUint64), `18446744073709551615`},
		{"signed integer", msgp.AppendInt64(nil, math.MinInt64), `-9223372036854775808`},
		{"nil", []byte{0xc0}, `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeMessagePack(tc.in)
			if err != nil {
				t.Fatal(err)
			}

			if string(got) != tc.want {
				t.Errorf("decoded = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestMessagePackInvalidPreservesValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"empty", []byte{}},
		{"invalid prefix", []byte{0xc1}},
		{"truncated map", []byte{0x81, 0xa1, 'x'}},
		{"multiple objects", []byte{0x01, 0x02}},
		{"raw JSON", []byte(`{"x":1}`)},
		{"nan", msgp.AppendFloat64(nil, math.NaN())},
		{"infinity", msgp.AppendFloat64(nil, math.Inf(1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			co := new(consumeOutput)
			for range 2 {
				got, decoded := co.msgpackDecode(tc.in)
				if decoded || !bytes.Equal(got, tc.in) || got == nil {
					t.Fatalf("invalid value = %x, decoded = %v, want original %x", got, decoded, tc.in)
				}
			}

			if len(co.decodeErrSeen) != 1 {
				t.Errorf("remembered %d errors, want 1", len(co.decodeErrSeen))
			}
		})
	}

	co := new(consumeOutput)
	if got, decoded := co.msgpackDecode(nil); got != nil || decoded || len(co.decodeErrSeen) != 0 {
		t.Errorf("tombstone = %x, decoded = %v, errors = %v", got, decoded, co.decodeErrSeen)
	}
}

func TestConsumeMessagePack(t *testing.T) {
	_, addrs := seedCluster(t, 0)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	values := [][]byte{wrappedMessagePack(t), []byte(`{"raw":true}`), nil, {}, {0xc1}}
	for _, value := range values {
		if err := cl.ProduceSync(t.Context(), &kgo.Record{Topic: "t", Key: []byte("AB1234"), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	got := runConsume(t, addrs, "--msgpack", "-o", ":end")
	if len(got) != len(values) {
		t.Fatalf("got %d records, want %d", len(got), len(values))
	}

	for _, record := range got {
		if record["key"] != "AB1234" {
			t.Errorf("key = %v, want AB1234", record["key"])
		}
	}

	value, ok := got[0]["value"].(map[string]any)
	if !ok {
		t.Fatalf("value is not a JSON object: %v", got[0])
	}

	data, ok := value["data"].(map[string]any)
	if !ok || data["order_id"] != "abc" {
		t.Errorf("data = %v, want order_id abc", value["data"])
	}

	if got[1]["value"] != `{"raw":true}` {
		t.Errorf("failed decode should preserve JSON as a string: %v", got[1])
	}

	if v, ok := got[2]["value"]; !ok || v != nil {
		t.Errorf("tombstone not preserved: %v", got[2])
	}

	if got[3]["value"] != "" {
		t.Errorf("empty value not preserved: %v", got[3])
	}

	if got[4]["value_base64"] != base64.StdEncoding.EncodeToString(values[4]) {
		t.Errorf("invalid binary not preserved: %v", got[4])
	}

	formatted := runConsume(t, addrs, "--msgpack", "-n", "1", "-f", "%v\n")
	if len(formatted) != 1 || formatted[0]["data"] == nil || formatted[0]["meta"] == nil {
		t.Errorf("formatted value lost the wrapper: %v", formatted)
	}

	raw := runConsume(t, addrs, "-n", "1")
	if len(raw) != 1 || raw[0]["value_base64"] != base64.StdEncoding.EncodeToString(values[0]) {
		t.Errorf("consume without --msgpack should preserve the original bytes: %v", raw)
	}
}

func TestMessagePackConflictingFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--decode"},
		{"--decode=value"},
		{"--proto-file", "unused.proto", "--proto-message", "Order"},
	} {
		cmd := Command(nil)
		cmd.SetArgs(append([]string{"t", "--msgpack"}, args...))
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true

		if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "none of the others can be") {
			t.Errorf("flags %v: expected conflicting flags error, got %v", args, err)
		}
	}
}
