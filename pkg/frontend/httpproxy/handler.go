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

// Package httpproxy is the HTTP peer of wsproxy (WebSocket) and sshproxy (SSH):
// it forwards external HTTP requests to an in-sandbox HTTP server via the shared
// function_proxy tcp.tunnel (L4 byte pipe). The frontend never parses the HTTP
// layer beyond a single rewriteRequest of the request line — the in-sandbox
// server produces the response, relayed back verbatim through a bidirectional
// io.Copy after Hijack.
package httpproxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/wsproxy"
)

const pipeDirections = 2

// httpRoutePrefix is the frontend entry route that prefixes every passthrough
// request; rewriteRequest strips it to restore the in-sandbox request path.
var httpRoutePrefix = "/serverless/v1/http"

// routeQueryKeys lists the query keys consumed by the routing pipeline
// (instance/port/tenant_id/token). rewriteRequest strips them from the
// forwarded request so only business query keys reach the in-sandbox server.
var routeQueryKeys = map[string]struct{}{
	"instance":  {},
	"port":      {},
	"tenant_id": {},
	"token":     {},
}

// HandleHTTP is the httpproxy entry point registered on
// /serverless/v1/http and /serverless/v1/http/*splat. It rewrites the request
// line (strip the route prefix and routing query keys, drop Authorization,
// set X-Forwarded-Proto), dials the function_proxy tcp.tunnel via
// wsproxy.DialSandboxTunnel, Hijacks the client connection, writes the
// rewritten request to the tunnel, then pipes bytes both ways. All failure
// paths (bad path, auth, route resolution, dial) return before Hijack so a
// clean 4xx/502 can be written; once Hijack takes the connection there is no
// HTTP layer left to report errors on.
func HandleHTTP(w http.ResponseWriter, r *http.Request) {
	pr, ok := rewriteRequest(w, r)
	if !ok {
		return
	}

	tunnelConn, ok := wsproxy.DialSandboxTunnel(w, r)
	if !ok {
		return
	}
	defer closeWithLog(tunnelConn, "httpproxy tunnel")

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

	if werr := pr.Write(tunnelConn); werr != nil {
		log.GetLogger().Warnf("httpproxy write request to tunnel failed: %v", werr)
		return
	}
	flushPrefetch(clientBuf, tunnelConn)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	pipe(ctx, cancel, clientConn, tunnelConn)
}

func rewriteRequest(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	pr := r.Clone(r.Context())
	rest := strings.TrimPrefix(r.URL.Path, httpRoutePrefix)
	if rest == "" {
		rest = "/"
	}
	pr.URL.Path = rest
	pr.URL.RawPath = ""
	pr.Host = r.Host
	pr.Header.Del("Authorization")

	bizQ := url.Values{}
	for k, vs := range r.URL.Query() {
		if _, isRoute := routeQueryKeys[k]; isRoute {
			continue
		}
		bizQ[k] = vs
	}
	pr.URL.RawQuery = bizQ.Encode()

	if pr.Header.Get("X-Forwarded-Proto") == "" {
		if r.TLS != nil {
			pr.Header.Set("X-Forwarded-Proto", "https")
		} else {
			pr.Header.Set("X-Forwarded-Proto", "http")
		}
	}
	return pr, true
}

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

func closeWithLog(c io.Closer, name string) {
	if err := c.Close(); err != nil && !isClosedNetErr(err) {
		log.GetLogger().Warnf("close %s failed: %s", name, err)
	}
}

func isClosedNetErr(err error) bool {
	return errors.Is(err, net.ErrClosed) ||
		(err != nil && err.Error() == "use of closed network connection")
}

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
