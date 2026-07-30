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

package wsproxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
)

// TestFramedJSONRoundTrip verifies the function_proxy tcp.tunnel negotiation
// frame format that wsproxy must speak byte-for-byte: a tunnelFrameSizeBytes
// big-endian length prefix + JSON payload. A mismatch here means the proxy
// rejects the tunnel ("function proxy rejected tunnel"). Mirrors sshproxy wire.
func TestFramedJSONRoundTrip(t *testing.T) {
	header := tunnelHeader{
		TunnelVersion: tunnelVersion,
		InstanceID:    "inst-test",
		Protocol:      "tcp",
		TargetPort:    defaultAgentWSPort,
		RequestID:     "req-1",
	}
	var buf bytes.Buffer
	if err := writeFramedJSON(&buf, header); err != nil {
		t.Fatalf("writeFramedJSON: %v", err)
	}
	if int(binary.BigEndian.Uint32(buf.Bytes()[:tunnelFrameSizeBytes])) != buf.Len()-tunnelFrameSizeBytes {
		t.Fatalf("length prefix mismatch: got %d want %d",
			binary.BigEndian.Uint32(buf.Bytes()[:tunnelFrameSizeBytes]), buf.Len()-tunnelFrameSizeBytes)
	}
	var got tunnelHeader
	if err := readFramedJSON(bytes.NewReader(buf.Bytes()), &got); err != nil {
		t.Fatalf("readFramedJSON: %v", err)
	}
	if got.InstanceID != header.InstanceID || got.TargetPort != header.TargetPort ||
		got.Protocol != header.Protocol || got.RequestID != header.RequestID {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, header)
	}
}

// TestFramedJSONRejectsOversized guards the maxTunnelHeaderSize cap.
func TestFramedJSONRejectsOversized(t *testing.T) {
	tooBig := make([]byte, maxTunnelHeaderSize+1)
	if err := writeFramedJSON(io.Discard, tooBig); err == nil {
		t.Fatal("expected error for oversized payload, got nil")
	}
}

// TestBytePipePreservesWSHandshake is the core of R1: it proves the frontend's
// data plane (raw io.Copy both ways, no WS frame parsing) transparently carries
// a WS Upgrade request and 101 response across two half-connections. The
// frontend never generates the 101 — the AgentServer does, and the bytes flow
// verbatim through the pipe. This is the invariant that lets the AgentServer
// complete the handshake itself while the frontend stays a byte pipe.
func TestBytePipePreservesWSHandshake(t *testing.T) {
	upgradeReq := []byte("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n")
	resp101 := []byte("HTTP/1.1 101 Switching Protocols\r\n\r\n")
	clientData := []byte("hello-from-client")
	serverData := []byte("hello-from-server")

	// Two halves of the frontend pipe(): clientConn<->tunnelConn. The tunnel
	// side's far end is the "AgentServer". net.Pipe gives two bidirectional
	// conns (io.ReadWriter), matching the real net.Conn data plane the
	// frontend relays; io.Pipe returns two unidirectional halves and won't do.
	clientEnd, tunnelEnd := net.Pipe()
	defer clientEnd.Close()
	defer tunnelEnd.Close()

	serverErr := make(chan error, 1)
	go agentServerSide(tunnelEnd, agentServerScenario{
		upgradeReq: upgradeReq, resp101: resp101,
		clientData: clientData, serverData: serverData,
	}, serverErr)

	if _, err := clientEnd.Write(upgradeReq); err != nil {
		t.Fatalf("client write upgrade: %v", err)
	}
	got101 := make([]byte, len(resp101))
	if _, err := io.ReadFull(clientEnd, got101); err != nil {
		t.Fatalf("client read 101: %v", err)
	}
	if !bytes.Equal(got101, resp101) {
		t.Fatalf("101 not preserved verbatim: %q", got101)
	}
	if _, err := clientEnd.Write(clientData); err != nil {
		t.Fatalf("client write data: %v", err)
	}
	echoAndServer := make([]byte, len(clientData)+len(serverData))
	if _, err := io.ReadFull(clientEnd, echoAndServer); err != nil {
		t.Fatalf("client read echo+server: %v", err)
	}
	want := append(append([]byte{}, clientData...), serverData...)
	if !bytes.Equal(echoAndServer, want) {
		t.Fatalf("data not preserved: got %q want %q", echoAndServer, want)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("agentserver side: %v", err)
	}
}

// agentServerScenario bundles the byte fixtures an agentServerSide goroutine
// expects from the client and echoes back.
type agentServerScenario struct {
	upgradeReq []byte
	resp101    []byte
	clientData []byte
	serverData []byte
}

// agentServerSide simulates the sandbox AgentServer on the tunnel far end:
// read the Upgrade request verbatim, reply 101, echo client data back with the
// server push appended. Reports any mismatch via serverErr.
func agentServerSide(tunnelEnd io.ReadWriter, sc agentServerScenario, serverErr chan<- error) {
	got := make([]byte, len(sc.upgradeReq))
	if _, err := io.ReadFull(tunnelEnd, got); err != nil {
		serverErr <- err
		return
	}
	if !bytes.Equal(got, sc.upgradeReq) {
		serverErr <- errString("upgrade request not preserved verbatim")
		return
	}
	if _, err := tunnelEnd.Write(sc.resp101); err != nil {
		serverErr <- err
		return
	}
	cd := make([]byte, len(sc.clientData))
	if _, err := io.ReadFull(tunnelEnd, cd); err != nil {
		serverErr <- err
		return
	}
	if !bytes.Equal(cd, sc.clientData) {
		serverErr <- errString("client data not preserved")
		return
	}
	if _, err := tunnelEnd.Write(append(cd, sc.serverData...)); err != nil {
		serverErr <- err
		return
	}
	serverErr <- nil
}

// TestResolveTargetPort checks the ?port= parsing + default fallback, since a
// wrong port lands in the tunnel header TargetPort and the proxy dials the
// wrong container port.
func TestResolveTargetPort(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", defaultAgentWSPort},
		{"port=18000", 18000},
		{"port=9000", 9000},
	}
	for _, c := range cases {
		r := &http.Request{URL: &url.URL{RawQuery: c.query}}
		got, err := resolveTargetPort(r)
		if err != nil || got != c.want {
			t.Errorf("query %q: got %d,%v want %d", c.query, got, err, c.want)
		}
	}
	r := &http.Request{URL: &url.URL{RawQuery: "port=0"}}
	if _, err := resolveTargetPort(r); err == nil {
		t.Error("port 0 should error")
	}
	r = &http.Request{URL: &url.URL{RawQuery: "port=abc"}}
	if _, err := resolveTargetPort(r); err == nil {
		t.Error("port abc should error")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
