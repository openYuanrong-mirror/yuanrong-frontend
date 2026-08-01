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
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"frontend/pkg/common/faas_common/logger/log"
	fronttls "frontend/pkg/common/faas_common/tls"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/common/tenantauth"
	"frontend/pkg/frontend/config"
)

// agentWSHeaderXAuth mirrors the JWT header posixws/webterm read.
const agentWSHeaderXAuth = jwtauth.HeaderXAuth

// HandleWebSocket is the entry point registered on
// GET /serverless/v1/ws?instance=<id>&port=<n>&token=<jwt>.
//
// It performs pure L4 passthrough: authenticate (JWT), resolve the owning
// function_proxy's tcp.tunnel (reusing sshproxy's resolveInstance shape),
// dial it, then Hijack the client HTTP connection into a raw net.Conn and
// io.Copy bytes both ways. The frontend never completes the WS handshake
// itself — the Upgrade request bytes are forwarded verbatim to the
// in-sandbox AgentServer, which replies 101; that 101 is relayed back to
// the client. So the frontend is a transparent byte pipe on the WS layer,
// identical in spirit to sshproxy's channel copy but carrying WS bytes.
//
// All failure paths (auth, route resolution, tunnel dial) happen BEFORE
// Hijack so a clean HTTP 4xx/502 can be returned — once Hijack takes the
// connection there is no HTTP layer left to write an error response to.
// tunnelContext carries the per-request state produced before Hijack and
// consumed by the post-Hijack byte pipe.
type tunnelContext struct {
	tunnelConn net.Conn
	instanceID string
	port       int
	requestID  string
}

// dialSandboxTunnel authenticates, resolves the instance, authorizes the
// caller, and dials the function_proxy tcp.tunnel. Every failure path here is
// still inside the HTTP layer, so it writes a clean 4xx/502 and returns ok=false.
// On success the tunnel is open; the caller is responsible for closing
// tunnelConn.
func dialSandboxTunnel(w http.ResponseWriter, r *http.Request) (tunnelContext, bool) {
	tenantID, err := authenticateWebSocket(r)
	if err != nil {
		log.GetLogger().Warnf("wsproxy auth failed from %s: %v", r.RemoteAddr, err)
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return tunnelContext{}, false
	}

	instanceID := strings.TrimSpace(r.URL.Query().Get("instance"))
	if instanceID == "" {
		http.Error(w, "instance is required", http.StatusBadRequest)
		return tunnelContext{}, false
	}
	port, err := resolveTargetPort(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return tunnelContext{}, false
	}

	tunnelAddress, ok := resolveAndAuthorize(w, r, tenantID, instanceID)
	if !ok {
		return tunnelContext{}, false
	}
	return dialInstanceTunnel(w, instanceID, port, tunnelAddress)
}

// resolveAndAuthorize resolves the instance route and enforces the cross-tenant
// guard (system tenant may reach any instance). Writes a clean HTTP status on
// failure and returns ok=false.
func resolveAndAuthorize(w http.ResponseWriter, r *http.Request, tenantID, instanceID string) (string, bool) {
	instance, tunnelAddress, err := resolveInstance(r.Context(), instanceID)
	if err != nil {
		log.GetLogger().Infof("wsproxy resolve instance %s failed: %v", instanceID, err)
		http.Error(w, "failed to resolve instance", http.StatusBadGateway)
		return "", false
	}
	if !tenantOwnsInstance(tenantID, instance) {
		log.GetLogger().Warnf("wsproxy tenant %s not authorized for instance %s (owner %s)",
			tenantID, instanceID, tenantOfInstance(instance))
		http.Error(w, "not authorized for instance", http.StatusForbidden)
		return "", false
	}
	return tunnelAddress, true
}

// dialInstanceTunnel loads the platform TLS config and dials the function_proxy
// tcp.tunnel. Writes a clean HTTP status on failure and returns ok=false.
func dialInstanceTunnel(w http.ResponseWriter, instanceID string, port int,
	tunnelAddress string) (tunnelContext, bool) {
	tunnelTLSConfig, terr := loadTunnelTLSConfig()
	if terr != nil {
		log.GetLogger().Errorf("wsproxy load tunnel TLS config failed: %v", terr)
		http.Error(w, "tunnel TLS config error", http.StatusInternalServerError)
		return tunnelContext{}, false
	}
	requestID := newRequestID()
	tunnelConn, err := dialTunnel(tunnelAddress, tunnelTLSConfig, tunnelHeader{
		TunnelVersion: tunnelVersion,
		InstanceID:    instanceID,
		Protocol:      "tcp",
		TargetPort:    port,
		RequestID:     requestID,
		TraceID:       requestID,
	})
	if err != nil {
		log.GetLogger().Infof("wsproxy dialTunnel instance=%s port=%d failed: %v",
			instanceID, port, err)
		http.Error(w, "failed to dial sandbox", http.StatusBadGateway)
		return tunnelContext{}, false
	}
	log.GetLogger().Infof("wsproxy tunnel established instance=%s port=%d requestID=%s",
		instanceID, port, requestID)
	return tunnelContext{tunnelConn: tunnelConn, instanceID: instanceID, port: port, requestID: requestID}, true
}

// HandleWebSocket is the wsproxy entry point: L4-passthrough a client WebSocket
// to the in-sandbox AgentServer via the function_proxy tcp.tunnel. The frontend
// never terminates the WS or parses E2A — it only relays bytes after Hijack.
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	tc, ok := dialSandboxTunnel(w, r)
	if !ok {
		return
	}
	defer closeWithLog(tc.tunnelConn, "wsproxy tunnel")

	// Hijack the client HTTP connection into a raw net.Conn. After this
	// succeeds there is no HTTP layer — we can no longer return error codes,
	// so every prior failure path returned above.
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket hijack unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, clientBuf, err := hj.Hijack()
	if err != nil {
		log.GetLogger().Warnf("wsproxy hijack failed: %v", err)
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		return
	}
	defer closeWithLog(clientConn, "wsproxy client")

	// gin has already read the entire Upgrade request off the client conn before
	// routing here, so the hijacked clientBuf holds no complete request. Rewrite
	// it to the tunnel or the AgentServer never answers 101.
	if werr := r.Write(tc.tunnelConn); werr != nil {
		log.GetLogger().Warnf("wsproxy write upgrade request to tunnel failed: %v", werr)
		return
	}
	flushPrefetch(clientBuf, tc.tunnelConn)

	// Bidirectional byte copy: two goroutines, the first to finish closes the
	// peer. No WS frame parsing — the AgentServer and client reassemble frames
	// from the byte stream themselves (TCP is a stream). Same shape as
	// sshproxy.copyChannel for SSH bytes.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	pipe(ctx, cancel, clientConn, tc)
}

// flushPrefetch flushes bytes the HTTP reader prefetched past the request
// (e.g. an early WS frame) into the tunnel.
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
		log.GetLogger().Warnf("wsproxy drain client prefetch failed: %v", rerr)
		return
	}
	if werr := writeAll(tunnelConn, prefetched); werr != nil {
		log.GetLogger().Warnf("wsproxy forward prefetched upgrade bytes failed: %v", werr)
	}
}

// pipe runs two io.Copy goroutines (client->tunnel and tunnel->client) until
// one side ends, then closes both. It is the entire data-plane body of the
// handler after Hijack.
func pipe(ctx context.Context, cancel context.CancelFunc, client net.Conn, tc tunnelContext) {
	tunnel := tc.tunnelConn
	var wg sync.WaitGroup
	wg.Add(pipeDirections)
	// Close-once: the first finishing direction closes the peer.
	var once sync.Once
	closeBoth := func(dir string, err error) {
		once.Do(func() {
			if err != nil && !isClosedNetErr(err) {
				log.GetLogger().Infof("wsproxy copy %s ended instance=%s requestID=%s: %v",
					dir, tc.instanceID, tc.requestID, err)
			}
			closeWithLog(client, "wsproxy client")
			closeWithLog(tunnel, "wsproxy tunnel")
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

// authenticateWebSocket reuses the posixws/webterm token-acquisition contract:
// when function-token auth is disabled, ?tenant_id (or "default") is trusted
// for local/CI smoke; otherwise the JWT is pulled from X-Auth header, ?token,
// the iam_token cookie, or the Sec-WebSocket-Protocol subprotocol (the browser
// trick for browsers that cannot set custom headers on a WS handshake) and
// validated by ParseJWT + ValidateWithIamServer. The returned id is the JWT
// subject (tenant).
func authenticateWebSocket(r *http.Request) (string, error) {
	if !config.GetConfig().IamConfig.EnableFuncTokenAuth {
		if t := r.URL.Query().Get("tenant_id"); t != "" {
			return t, nil
		}
		return "default", nil
	}
	token := r.Header.Get(agentWSHeaderXAuth)
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		token = tokenFromCookie(r)
	}
	if token == "" {
		token = tokenFromSubprotocol(r)
	}
	if token == "" {
		return "", errors.New("no authentication token provided")
	}
	parsed, err := jwtauth.ParseJWT(token)
	if err != nil {
		return "", err
	}
	if err := jwtauth.ValidateWithIamServer(token, r.Header.Get("X-Trace-ID")); err != nil {
		return "", err
	}
	sub := parsed.Payload.Sub
	if sub == "" {
		sub = "default"
	}
	return sub, nil
}

func tokenFromCookie(r *http.Request) string {
	c, err := r.Cookie("iam_token")
	if err != nil {
		return ""
	}
	return c.Value
}

// tokenFromSubprotocol reads the first non-empty value from the
// Sec-WebSocket-Protocol request header (comma-separated). Browsers cannot set
// custom headers on a WS handshake, so the JWT is smuggled as a subprotocol
// there; the AgentServer's own 101 will echo it back.
func tokenFromSubprotocol(r *http.Request) string {
	proto := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Protocol"))
	if proto == "" {
		return ""
	}
	for _, p := range strings.Split(proto, ",") {
		if v := strings.TrimSpace(p); v != "" {
			return v
		}
	}
	return ""
}

func resolveTargetPort(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("port"))
	if raw == "" {
		return defaultAgentWSPort, nil
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port < minPort || port > maxPort {
		return 0, fmt.Errorf("invalid port %q", raw)
	}
	return port, nil
}

// tenantOwnsInstance authorizes the caller against the instance owner. The
// system tenant may reach any instance; otherwise the JWT sub must equal the
// instance's createOptions["tenantId"]. Mirrors sandbox.authorizeSandboxDelete
// semantics.
func tenantOwnsInstance(callerTenant string, instance *types.InstanceSpecification) bool {
	owner := tenantOfInstance(instance)
	if owner == "" {
		// Instance without a recorded tenant (rare; legacy/CI). Allow only when
		// auth is disabled (caller would be "default") — matches posixws posture.
		return callerTenant == "default" || callerTenant == ""
	}
	if callerTenant == "" {
		return false
	}
	return callerTenant == owner || isSystemTenant(callerTenant)
}

// isSystemTenant reports whether the caller is the platform operator tenant,
// which may reach any instance (mirrors sandbox.authorizeSandboxDelete's
// callerTenantID == tenantauth.SystemTenantID branch).
func isSystemTenant(callerTenant string) bool {
	return callerTenant == tenantauth.SystemTenantID
}

// loadTunnelTLSConfig returns the platform component mTLS config used for the
// frontend<->function_proxy tunnel hop, identical to sshproxy's tunnelTLSConfig
// (both call NewComponentClientTLSConfig from the global ComponentMTLSEnable
// setting). Returns nil when component mTLS is disabled — the tunnel then runs
// plain TCP, with a security warning already logged by the proxy side. The
// function_proxy<->in-sandbox AgentServer hop is always plain ws:// because it
// is a same-node loopback; the two TLS domains are independent.
func loadTunnelTLSConfig() (*tls.Config, error) {
	if !config.GetConfig().ComponentMTLSEnable {
		return nil, nil
	}
	httpsConfig := config.GetConfig().HTTPSConfig
	if httpsConfig == nil {
		return nil, errors.New("frontend HTTPS config is required for component certificate paths")
	}
	return fronttls.NewComponentClientTLSConfig(*httpsConfig)
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
// short write would drop part of the prefetched Upgrade-request bytes and
// break the WS handshake at the AgentServer.
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
