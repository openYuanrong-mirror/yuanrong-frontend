/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package proxy is the sandbox router data plane: it parses the inbound
// /{safeID}/{port}/path, resolves a backend, and reverse-proxies to it with
// Traefik-consistent status codes.
package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/sandboxrouter/route"
)

// Resolver is the subset of the route resolver the proxy consumes. Declared
// here (consumer side) so the proxy stays free of the etcd-backed resolver
// package and remains unit-testable.
type Resolver interface {
	Resolve(ctx context.Context, key route.Key) (*route.Target, error)
}

// reqInfo is the per-request routing decision passed to the reverse proxy via
// the request context.
type reqInfo struct {
	parsed                *route.ParsedRequest
	target                *route.Target
	frontendProxied       bool
	frontendAuthenticated bool
}

type ctxKey int

const (
	reqInfoKey                    ctxKey = 0
	internalSrcHeader                    = "X-Internal-Src"
	internalSrcAuthenticatedValue        = "1"
	internalSrcProxyValue                = "2"
	internalTenantHeader                 = "X-Internal-Tenant"
	defaultTunnelAliasPort        uint16 = 8765
	explicitTunnelPortSegments           = 2
	decimalBase                          = 10
	defaultMaxIdleConns                  = 1000
	defaultMaxIdleConnsPerHost           = 200
	defaultIdleConnTimeout               = 90 * time.Second
)

// Server parses, resolves, and reverse-proxies sandbox traffic. http and https
// backends use separate transports so https targets can carry backend TLS
// config (the equivalent of FunctionMaster's serversTransport).
type Server struct {
	resolver       Resolver
	httpTransport  *http.Transport
	httpsTransport *http.Transport
	proxy          *httputil.ReverseProxy

	// auth (set via SetAuth): when authEnabled, only the RRT atomic-ops
	// control port requires a valid JWT. Reverse-tunnel WS is deliberately
	// public at the router layer: SDK tunnel defaults to plaintext ws:// and
	// must not carry platform JWTs on that hop. User-forwarded service ports
	// are also PUBLIC (no token). Legacy direct-to-router rrtPort traffic
	// preserves X-Auth for compatibility; frontend-authenticated /direct traffic
	// and all non-RRT ports strip it before reaching the backend.
	authEnabled bool
	validateIAM bool
	rrtPort     uint16
	tunnelPort  uint16
}

// SetAuth configures router-side JWT authentication. rrtPort is the RRT
// built-in atomic-ops port and is the only router-authenticated control port
// (0 = none). tunnelPort is retained for routing/config compatibility but is
// intentionally public because reverse tunnel clients do not send JWT on ws://.
// Other ports are public.
func (s *Server) SetAuth(enabled, validateIAM bool, rrtPort, tunnelPort uint16) {
	s.authEnabled = enabled
	s.validateIAM = validateIAM
	s.rrtPort = rrtPort
	s.tunnelPort = tunnelPort
}

// isControlPort reports whether a port is a sandbox built-in control port
// that requires router authentication. Only RRT atomic-ops is gated here;
// reverse-tunnel WS and user-forwarded service ports are public.
func (s *Server) isControlPort(port uint16) bool {
	return s.rrtPort != 0 && port == s.rrtPort
}

func requestFromLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func frontendAuthenticatedRequest(r *http.Request) bool {
	return requestFromLoopback(r) && r.Header.Get(internalSrcHeader) == internalSrcAuthenticatedValue
}

func frontendProxiedRequest(r *http.Request) bool {
	if !requestFromLoopback(r) {
		return false
	}
	source := r.Header.Get(internalSrcHeader)
	return source == internalSrcAuthenticatedValue || source == internalSrcProxyValue
}

// normalizeTunnelAliasPath lets the sandboxRouter serve the public tunnel URL
// shape directly. The canonical router grammar remains /{safeID}/{port}/...,
// while /tunnel/{safeID} is a static alias for /{safeID}/{tunnelPort}.
// /tunnel/{safeID}/{port}/... is also accepted as a legacy explicit-port form
// by simply stripping the /tunnel prefix.
func (s *Server) normalizeTunnelAliasPath(path string) string {
	const prefix = "/tunnel/"
	if path == "/tunnel" || !strings.HasPrefix(path, prefix) {
		return path
	}

	rest := strings.TrimPrefix(path, prefix)
	if rest == "" {
		return path
	}

	segments := strings.Split(rest, "/")
	safeID := segments[0]
	if safeID == "" {
		return path
	}

	if len(segments) >= explicitTunnelPortSegments {
		if port, err := strconv.ParseUint(segments[1], 10, 16); err == nil && port != 0 {
			return "/" + rest
		}
	}

	port := s.tunnelPort
	if port == 0 {
		port = defaultTunnelAliasPort
	}
	tail := ""
	if len(segments) > 1 {
		tail = "/" + strings.Join(segments[1:], "/")
	}
	return "/" + safeID + "/" + strconv.FormatUint(uint64(port), decimalBase) + tail
}

// New builds a Server over the given resolver with default transports. The
// https transport uses Go defaults until SetHTTPSTransport configures backend
// TLS; callers serving https targets should set it.
func New(r Resolver) *Server {
	s := &Server{
		resolver:       r,
		httpTransport:  tunedTransport(&http.Transport{}),
		httpsTransport: tunedTransport(&http.Transport{}),
	}
	s.proxy = &httputil.ReverseProxy{
		Rewrite:      s.rewrite,
		Transport:    roundTripperFunc(s.roundTrip),
		ErrorHandler: errorHandler,
	}
	return s
}

// SetHTTPSTransport sets the transport used for https backends, carrying the
// backend TLS configuration. Connection-pool defaults are applied (preserving
// the caller's TLS settings) so https backends pool connections too.
func (s *Server) SetHTTPSTransport(t *http.Transport) {
	s.httpsTransport = tunedTransport(t)
}

// tunedTransport sets backend connection-pool defaults on t so the data plane
// reuses keep-alive connections under concurrency instead of churning a new
// TCP connection per request: a bare http.Transport caps MaxIdleConnsPerHost
// at 2 (DefaultMaxIdleConnsPerHost), which collapses under load. Mirrors
// Traefik's default backend transport (maxIdleConnsPerHost=200). Only zero
// fields are set, so explicit caller settings (e.g. TLS) are preserved.
func tunedTransport(t *http.Transport) *http.Transport {
	if t == nil {
		t = &http.Transport{}
	}
	if t.MaxIdleConns == 0 {
		t.MaxIdleConns = defaultMaxIdleConns
	}
	if t.MaxIdleConnsPerHost == 0 {
		t.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}
	if t.IdleConnTimeout == 0 {
		t.IdleConnTimeout = defaultIdleConnTimeout
	}
	return t
}

// roundTrip dispatches to the http or https transport by target scheme.
func (s *Server) roundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		return s.httpsTransport.RoundTrip(req)
	}
	return s.httpTransport.RoundTrip(req)
}

// ServeHTTP parses the path, resolves the backend, and proxies. Error codes
// follow Traefik: an unmatched route is 404 (never 400); a resolver that is
// erroring (not "not found") is 503; upstream failures are mapped in
// errorHandler to 502/504.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	targetForLog := ""
	defer func() {
		log.GetLogger().Infof(
			"sandboxrouter proxy method(%s) path(%s) target(%s) durationMs(%d) traceID(%s)",
			r.Method,
			r.URL.Path,
			targetForLog,
			time.Since(started).Milliseconds(),
			r.Header.Get(constant.HeaderTraceID),
		)
	}()

	s.serveHTTP(w, r, &targetForLog)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request, targetForLog *string) {
	parsed, err := route.ParsePath(s.normalizeTunnelAliasPath(r.URL.Path))
	if err != nil {
		http.NotFound(w, r) // 404: no router would match this shape
		return
	}

	// Authenticate (token validity) BEFORE resolving, so an unauthenticated
	// caller gets 401 regardless of whether the route exists. Only RRT
	// atomic-ops is gated; reverse-tunnel WS and user-forwarded service ports are
	// public. Requests authenticated by the local frontend do not need router
	// authentication. Frontend-proxied requests with deferred authentication do,
	// and their token is stripped before the RRT hop below.
	frontendProxied := frontendProxiedRequest(r)
	frontendAuthenticated := frontendAuthenticatedRequest(r)
	authRequired := s.authEnabled && s.isControlPort(parsed.Key.Port) && !frontendAuthenticated
	var jwtPayload *jwtauth.JWTPayload
	if authRequired {
		payload, code, msg := s.authenticateToken(r)
		if code != 0 {
			http.Error(w, msg, code)
			return
		}
		jwtPayload = payload
	}

	target, err := s.resolver.Resolve(r.Context(), parsed.Key)
	if errors.Is(err, route.ErrRouteNotFound) {
		http.NotFound(w, r) // 404
		return
	}
	if err != nil {
		http.Error(w, "route unavailable", http.StatusServiceUnavailable) // 503
		return
	}
	if target.TargetURL != nil {
		*targetForLog = target.TargetURL.Redacted()
	}

	// Authorize (tenant ownership) AFTER resolving, against the target's
	// authoritative tenant. Frontend-authenticated control traffic skips the
	// duplicate JWT validation, but never skips tenant authorization.
	authorizationRequired := authRequired ||
		(s.authEnabled && frontendAuthenticated && s.isControlPort(parsed.Key.Port))
	if authorizationRequired {
		if frontendAuthenticated {
			jwtPayload = &jwtauth.JWTPayload{Sub: r.Header.Get(internalTenantHeader)}
		}
		if code, msg := authorize(jwtPayload, target); code != 0 {
			http.Error(w, msg, code)
			return
		}
	}

	ctx := context.WithValue(r.Context(), reqInfoKey, &reqInfo{
		parsed:                parsed,
		target:                target,
		frontendProxied:       frontendProxied,
		frontendAuthenticated: frontendAuthenticated,
	})
	s.proxy.ServeHTTP(w, r.WithContext(ctx))
}

// authenticateToken validates the JWT carried in X-Auth (or the ?token= query
// fallback used by WebSocket clients): presence, decodability, expiry, and —
// when validateIAM is set — an IAM-server round-trip. It returns the decoded
// payload plus (0, "") on success, or an HTTP status + message to reject with.
func (s *Server) authenticateToken(r *http.Request) (*jwtauth.JWTPayload, int, string) {
	authHeader := r.Header.Get(jwtauth.HeaderXAuth)
	if authHeader == "" {
		authHeader = r.URL.Query().Get("token")
	}
	if authHeader == "" {
		return nil, http.StatusUnauthorized, "missing X-Auth token"
	}
	parsedJWT, err := jwtauth.ParseJWT(authHeader)
	if err != nil || parsedJWT.Payload == nil {
		return nil, http.StatusUnauthorized, "invalid token"
	}
	if parsedJWT.Payload.IsExpired(time.Now()) {
		return nil, http.StatusUnauthorized, "token expired"
	}
	if s.validateIAM {
		if err := jwtauth.ValidateWithIamServer(authHeader, r.Header.Get("X-Trace-ID")); err != nil {
			return nil, http.StatusUnauthorized, "token validation failed"
		}
	}
	return parsedJWT.Payload, 0, ""
}

// authorize checks that the token's tenant (Sub) owns the target sandbox, using
// the instance owner resolved from instance metadata. It fails closed when
// either identity is missing: an unattributed route must not become a
// cross-tenant escape hatch.
func authorize(payload *jwtauth.JWTPayload, target *route.Target) (int, string) {
	if payload == nil || target == nil || target.Tenant == "" || payload.Sub == "" {
		return http.StatusForbidden, "tenant ownership is unavailable"
	}
	if target.Tenant != payload.Sub {
		return http.StatusForbidden, "tenant not authorized for this sandbox"
	}
	return 0, ""
}

// rewrite rewrites the outbound request to the resolved target, stripping the
// /{safeID}/{port} prefix and preserving the query string.
func (s *Server) rewrite(pr *httputil.ProxyRequest) {
	info, ok := pr.In.Context().Value(reqInfoKey).(*reqInfo)
	if !ok || info == nil {
		return
	}

	pr.SetURL(info.target.TargetURL)
	pr.Out.URL.Path = info.parsed.StrippedPath
	pr.Out.URL.RawPath = ""
	pr.Out.URL.RawQuery = pr.In.URL.RawQuery
	pr.SetXForwarded()
	pr.Out.Header.Set("X-Instance-Id", info.parsed.Key.SafeInstanceID)
	pr.Out.Header.Set("X-Instance-Port", strconv.Itoa(int(info.parsed.Key.Port)))

	// Token handling: any local frontend /direct hop strips platform credentials
	// before RRT. Legacy direct-to-router RRT traffic still preserves the token
	// for compatibility; user service ports must never see it.
	stripToken := info.frontendProxied || (s.authEnabled &&
		!(s.rrtPort != 0 && info.parsed.Key.Port == s.rrtPort))
	if stripToken {
		pr.Out.Header.Del(jwtauth.HeaderXAuth)
		if q := pr.Out.URL.Query(); q.Has("token") {
			q.Del("token")
			pr.Out.URL.RawQuery = q.Encode()
		}
	}
	pr.Out.Header.Del(internalSrcHeader)
	pr.Out.Header.Del(internalTenantHeader)
}

// errorHandler maps upstream transport failures to Traefik-consistent codes:
// timeouts to 504 Gateway Timeout, everything else to 502 Bad Gateway.
func errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		status = http.StatusGatewayTimeout
	}

	target := ""
	traceID := ""
	if r != nil {
		traceID = r.Header.Get(constant.HeaderTraceID)
		if r.URL != nil {
			target = r.URL.Scheme + "://" + r.URL.Host
		}
	}
	log.GetLogger().Errorf(
		"sandboxrouter upstream request failed target(%s) traceID(%s) status(%d) error(%s)",
		target,
		traceID,
		status,
		err,
	)
	w.WriteHeader(status)
}

// roundTripperFunc adapts a function to http.RoundTripper so the proxy reads
// the Server's current transport at call time (lets tests swap it).
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
