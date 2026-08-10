/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package httpproxy is the HTTP peer of sshproxy (SSH) and wsproxy (WebSocket):
// a transparent L4 byte pipe from an external HTTP client to an in-sandbox
// HTTP server. Like wsproxy it reuses the function_proxy tcp.tunnel (an L4 byte
// pipe, Protocol="tcp") via the shared wsproxy.DialSandboxTunnel pipeline
// (authenticate -> resolve -> authorize -> dial), then Hijacks the client HTTP
// connection into a raw net.Conn, writes the inbound HTTP request verbatim to
// the tunnel, and io.Copy bytes both ways. The frontend never parses the HTTP
// request or response — the in-sandbox server produces the response (status
// line, headers, body, including chunked/SSE streaming), relayed back verbatim.
//
// This is NOT an HTTP<->WS gateway: the frontend is protocol-transparent, the
// same byte-pipe shape as wsproxy, only carrying HTTP bytes instead of WS
// bytes. Whatever HTTP server listens in-sandbox on ?port is the caller's
// concern (AgentServer HTTP listener, sidecar, etc.) — out of scope here.
package httpproxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"

	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/wsproxy"
)

const pipeDirections = 2

// HandleHTTP is the httpproxy entry point registered on
// /serverless/v1/http?instance=<id>&port=<n>&token=<jwt>.
//
// It performs pure L4 passthrough: authenticate + resolve + dial the
// function_proxy tcp.tunnel (via wsproxy.DialSandboxTunnel, identical to the
// WS path), Hijack the client HTTP connection into a raw net.Conn, write the
// inbound HTTP request bytes to the tunnel, then io.Copy bytes both ways. The
// frontend never terminates the HTTP layer or parses frames — the in-sandbox
// HTTP server produces the response, relayed back verbatim. Same shape as
// wsproxy.HandleWebSocket's post-dial body, only carrying HTTP bytes.
//
// All failure paths (auth, route resolution, tunnel dial) happen BEFORE Hijack
// so a clean HTTP 4xx/502 can be returned — once Hijack takes the connection
// there is no HTTP layer left to write an error response to.
func HandleHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Acquire the authenticated tunnel conn to the in-sandbox HTTP server.
	//    All 4xx/502 failure handling lives inside DialSandboxTunnel.
	tunnelConn, ok := wsproxy.DialSandboxTunnel(w, r)
	if !ok {
		return // DialSandboxTunnel already wrote the error response.
	}
	defer closeWithLog(tunnelConn, "httpproxy tunnel")

	// 2. Hijack the client HTTP connection into a raw net.Conn. After this
	//    succeeds there is no HTTP layer — we can no longer return error codes,
	//    so every prior failure path returned above.
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "http hijack unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, clientBuf, err := hj.Hijack()
	if err != nil {
		log.GetLogger().Warnf("httpproxy hijack failed: %v", err)
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		return
	}
	defer closeWithLog(clientConn, "httpproxy client")

	// 3. gin has already read the entire HTTP request off the client conn before
	//    routing here, so the hijacked clientBuf holds no complete request.
	//    Rewrite it to the tunnel or the in-sandbox server never answers.
	if werr := r.Write(tunnelConn); werr != nil {
		log.GetLogger().Warnf("httpproxy write request to tunnel failed: %v", werr)
		return
	}
	flushPrefetch(clientBuf, tunnelConn)

	// 4. Bidirectional byte copy: two goroutines, the first to finish closes
	//    the peer. No HTTP frame parsing — the client and in-sandbox server
	//    reassemble HTTP from the byte stream themselves (TCP is a stream).
	//    Streaming responses (chunked/SSE) flow back through the tunnel->client
	//    copy without any buffering or framing by the frontend. Same shape as
	//    wsproxy.pipe.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	pipe(ctx, cancel, clientConn, tunnelConn)
}

// flushPrefetch flushes bytes the HTTP reader prefetched past the request
// (e.g. an early request body) into the tunnel, so nothing the client sent is
// lost. Mirrors wsproxy.flushPrefetch.
func flushPrefetch(clientBuf *bufio.ReadWriter, tunnelConn net.Conn) {
	if clientBuf == nil {
		return
	}
	unread := clientBuf.Reader.Buffered()
	if unread <= 0 {
		return
	}
	prefetched := make([]byte, unread)
	if _, rerr := clientBuf.Reader.Read(prefetched); rerr != nil && !errors.Is(rerr, io.EOF) {
		log.GetLogger().Warnf("httpproxy drain client prefetch failed: %v", rerr)
		return
	}
	if werr := writeAll(tunnelConn, prefetched); werr != nil {
		log.GetLogger().Warnf("httpproxy forward prefetched request bytes failed: %v", werr)
	}
}

// pipe runs two io.Copy goroutines (client->tunnel and tunnel->client) until
// one side ends, then closes both. It is the entire data-plane body of the
// handler after Hijack. Mirrors wsproxy.pipe.
func pipe(ctx context.Context, cancel context.CancelFunc, client, tunnel net.Conn) {
	var wg sync.WaitGroup
	wg.Add(pipeDirections)
	var once sync.Once
	closeBoth := func(dir string, err error) {
		once.Do(func() {
			if err != nil && !isClosedNetErr(err) {
				log.GetLogger().Infof("httpproxy copy %s ended: %v", dir, err)
			}
			closeWithLog(client, "httpproxy client")
			closeWithLog(tunnel, "httpproxy tunnel")
			cancel()
		})
	}
	go func() {
		defer wg.Done()
		_, err := io.Copy(tunnel, client)
		closeBoth("client->tunnel", err)
	}()
	go func() {
		defer wg.Done()
		_, err := io.Copy(client, tunnel)
		closeBoth("tunnel->client", err)
	}()
	wg.Wait()
	<-ctx.Done()
}

// closeWithLog closes a connection and logs only non-trivial errors.
func closeWithLog(c io.Closer, name string) {
	if err := c.Close(); err != nil && !isClosedNetErr(err) {
		log.GetLogger().Warnf("close %s failed: %s", name, err)
	}
}

func isClosedNetErr(err error) bool {
	return errors.Is(err, net.ErrClosed) ||
		(err != nil && err.Error() == "use of closed network connection")
}

// writeAll writes all of b to w, retrying on short writes. net.Conn.Write does
// not guarantee a single call writes the whole buffer; without this loop a
// short write would drop part of the HTTP request bytes and the in-sandbox
// server would never see a complete request.
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
