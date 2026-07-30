/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses.2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package wsproxy provides L4 transparent passthrough of WebSocket connections
// to an in-sandbox AgentServer, reusing the function_proxy tcp.tunnel that
// sshproxy already established. Unlike sshproxy (which runs SSH on the tunnel),
// wsproxy Hijacks the client HTTP connection into a raw net.Conn and io.Copy
// bytes both ways — the WS handshake (101) is completed by the AgentServer,
// not the frontend, so the frontend stays a byte pipe transparent to the WS
// protocol layer.
package wsproxy

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	tunnelVersion         = 1
	maxTunnelHeaderSize   = 16 * 1024
	tunnelFrameSizeBytes  = 4
	tunnelDialTimeout     = 10 * time.Second
	tunnelKeepAlive       = 30 * time.Second
	defaultAgentWSPort    = 18092
	defaultRouteWait      = 10 * time.Second
	defaultTunnelAttempts = 10
	defaultTunnelInterval = 500 * time.Millisecond

	// pipeDirections is the number of bidirectional copy goroutines
	// (client->tunnel and tunnel->client) run by pipe.
	pipeDirections = 2
	// Valid TCP port bounds used by resolveTargetPort.
	minPort = 1
	maxPort = 65535
)

// tunnelHeader mirrors sshproxy.tunnelHeader (byte-for-byte): function_proxy's
// tcp.tunnel server reads this framed JSON to learn which instance + port to
// connect inside the sandbox. Protocol="tcp" is the generic L4 marker, not
// SSH-specific — the same tunnel that sshproxy uses for SSH hops carries WS
// bytes for wsproxy.
type tunnelHeader struct {
	TunnelVersion int    `json:"tunnelVersion"`
	InstanceID    string `json:"instanceID"`
	Protocol      string `json:"protocol"`
	TargetPort    int    `json:"targetPort"`
	RequestID     string `json:"requestID"`
	TraceID       string `json:"traceID,omitempty"`
}

type tunnelResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// dialTunnel opens the function_proxy tcp.tunnel to the sandbox-internal
// (instanceID, targetPort). It negotiates the framed-JSON header, waits for
// the proxy's OK (which means ConnectLocalPort on the proxy side succeeded —
// the AgentServer port is actually listening), then returns a raw net.Conn
// whose writes arrive verbatim at the AgentServer. This is the same wire
// sequence sshproxy uses; only the upper layer differs (WS bytes vs SSH).
func dialTunnel(address string, tlsConfig *tls.Config, header tunnelHeader) (net.Conn, error) {
	return dialTunnelWithTimeout(address, tlsConfig, header, tunnelDialTimeout)
}

func dialTunnelWithTimeout(address string, tlsConfig *tls.Config, header tunnelHeader,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: tunnelDialTimeout, KeepAlive: tunnelKeepAlive}
	var conn net.Conn
	var err error
	if tlsConfig == nil {
		conn, err = dialer.Dial("tcp", address)
	} else {
		conn, err = tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
	}
	if err != nil {
		return nil, fmt.Errorf("dial function proxy tunnel: %w", err)
	}
	if err = conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, closeTunnelAfterError(conn, fmt.Errorf("set tunnel negotiation deadline: %w", err))
	}
	if err = writeFramedJSON(conn, header); err != nil {
		return nil, closeTunnelAfterError(conn, fmt.Errorf("send tunnel header: %w", err))
	}
	var response tunnelResponse
	if err = readFramedJSON(conn, &response); err != nil {
		return nil, closeTunnelAfterError(conn, fmt.Errorf("read tunnel response: %w", err))
	}
	if !response.OK {
		return nil, closeTunnelAfterError(conn,
			fmt.Errorf("function proxy rejected tunnel: %s", response.Message))
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return nil, closeTunnelAfterError(conn, fmt.Errorf("clear tunnel negotiation deadline: %w", err))
	}
	return conn, nil
}

func writeFramedJSON(writer io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > maxTunnelHeaderSize {
		return fmt.Errorf("invalid framed JSON size %d", len(payload))
	}
	var size [tunnelFrameSizeBytes]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
	if _, err = writer.Write(size[:]); err != nil {
		return err
	}
	_, err = writer.Write(payload)
	return err
}

func readFramedJSON(reader io.Reader, value any) error {
	var size [tunnelFrameSizeBytes]byte
	if _, err := io.ReadFull(reader, size[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > maxTunnelHeaderSize {
		return fmt.Errorf("invalid framed JSON size %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return err
	}
	return json.Unmarshal(payload, value)
}

func closeTunnelAfterError(conn net.Conn, cause error) error {
	if err := conn.Close(); err != nil {
		return fmt.Errorf("%w; close tunnel: %v", cause, err)
	}
	return cause
}
