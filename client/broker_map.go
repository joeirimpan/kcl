package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

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
