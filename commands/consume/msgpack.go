package consume

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

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

func (co *consumeOutput) msgpackDecode(b []byte) ([]byte, bool) {
	if b == nil {
		return nil, false
	}

	decoded, err := decodeMessagePack(b)
	if err != nil {
		co.reportDecodeError(fmt.Sprintf("unable to msgpack-decode value: %v", err))
		return b, false
	}

	return decoded, true
}
