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

package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/frontend/sandboxrouter/route"
)

const (
	proxyTestBackendSleep     = 200 * time.Millisecond
	proxyTestHeaderTimeout    = 20 * time.Millisecond
	proxyTestHTTPSBackendPort = 8443
)

type fakeResolver struct {
	target *route.Target
	err    error
}

type captureErrorLogger struct {
	utils.FakeLogger
	errors []string
}

func (l *captureErrorLogger) Errorf(format string, args ...interface{}) {
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func (f fakeResolver) Resolve(_ context.Context, _ route.Key) (*route.Target, error) {
	return f.target, f.err
}

func targetTo(t *testing.T, rawURL string) *route.Target {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse %q: %v", rawURL, err)
	}
	return &route.Target{TargetURL: u, Scheme: u.Scheme}
}

func do(s *Server, method, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

// Happy path: prefix stripped, query preserved, headers added, body proxied.
func TestProxyForwardsStrippedPath(t *testing.T) {
	var gotPath, gotQuery, gotInstID string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotInstID = r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Instance-Id")
		if _, err := io.WriteString(w, "pong"); err != nil {
			t.Errorf("write upstream response: %v", err)
		}
	}))
	defer upstream.Close()

	s := New(fakeResolver{target: targetTo(t, upstream.URL)})
	rec := do(s, http.MethodGet, "/inst-a/8080/api/foo?x=1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "pong" {
		t.Errorf("body = %q, want pong", rec.Body.String())
	}
	if gotPath != "/api/foo" {
		t.Errorf("upstream path = %q, want /api/foo", gotPath)
	}
	if gotQuery != "x=1" {
		t.Errorf("upstream query = %q, want x=1", gotQuery)
	}
	if gotInstID != "inst-a" {
		t.Errorf("X-Instance-Id = %q, want inst-a", gotInstID)
	}
}

// Error codes must match Traefik: unmatched route -> 404, not 400.
func TestProxyErrorCodes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	cases := []struct {
		name     string
		resolver Resolver
		path     string
		want     int
	}{
		{"unparseable path", fakeResolver{target: targetTo(t, upstream.URL)}, "/", http.StatusNotFound},
		{"port not numeric", fakeResolver{target: targetTo(t, upstream.URL)}, "/inst/nope", http.StatusNotFound},
		{"route not found", fakeResolver{err: route.ErrRouteNotFound}, "/inst/8080/x", http.StatusNotFound},
		{"instance paused", fakeResolver{err: route.ErrInstancePaused}, "/inst/8080/x", http.StatusConflict},
		{"resolver unavailable", fakeResolver{err: errors.New("boom")}, "/inst/8080/x", http.StatusServiceUnavailable},
		{"upstream refused", fakeResolver{target: targetTo(t, "http://127.0.0.1:1")}, "/inst/8080/x",
			http.StatusBadGateway},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(New(c.resolver), http.MethodGet, c.path)
			if rec.Code != c.want {
				t.Errorf("status = %d, want %d", rec.Code, c.want)
			}
		})
	}
}

// Upstream timeout maps to 504, not 502.
func TestProxyUpstreamTimeoutIs504(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(proxyTestBackendSleep)
	}))
	defer upstream.Close()

	s := New(fakeResolver{target: targetTo(t, upstream.URL)})
	s.httpTransport = &http.Transport{ResponseHeaderTimeout: proxyTestHeaderTimeout}

	rec := do(s, http.MethodGet, "/inst/8080/x")
	if rec.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}
}

func TestProxyUpstreamFailureLogsErrorTargetAndTraceID(t *testing.T) {
	originalLogger := log.GetLogger()
	capture := &captureErrorLogger{}
	log.SetupLogger(capture)
	t.Cleanup(func() {
		log.SetupLogger(originalLogger)
	})

	req := httptest.NewRequest(http.MethodPost, "http://192.168.26.87:21007/invoke?token=secret", nil)
	req.Header.Set(constant.HeaderTraceID, "trace-direct-123")
	rec := httptest.NewRecorder()

	errorHandler(rec, req, errors.New("connection reset by peer"))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
	if len(capture.errors) != 1 {
		t.Fatalf("error logs = %v, want exactly one entry", capture.errors)
	}
	entry := capture.errors[0]
	for _, want := range []string{
		"connection reset by peer",
		"http://192.168.26.87:21007",
		"trace-direct-123",
	} {
		if !strings.Contains(entry, want) {
			t.Errorf("log entry %q does not contain %q", entry, want)
		}
	}
	if strings.Contains(entry, "secret") {
		t.Errorf("log entry must not expose the upstream query string: %q", entry)
	}
}

// An https target is dispatched to the https transport; with skip-verify
// configured (the platform serversTransport default) a self-signed backend works.
func TestProxyHTTPSBackendSkipVerify(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.WriteString(w, "secure-pong"); err != nil {
			t.Errorf("write secure upstream response: %v", err)
		}
	}))
	defer upstream.Close()

	s := New(fakeResolver{target: targetTo(t, upstream.URL)}) // upstream.URL is https://...
	s.SetHTTPSTransport(&http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}})

	rec := do(s, http.MethodGet, fmt.Sprintf("/inst/%d/api", proxyTestHTTPSBackendPort))
	if rec.Code != http.StatusOK || rec.Body.String() != "secure-pong" {
		t.Errorf("status = %d body = %q, want %d secure-pong", rec.Code, rec.Body.String(), http.StatusOK)
	}
}

// Without skip-verify, the self-signed backend cert fails verification -> 502.
func TestProxyHTTPSBackendVerifyFails(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	s := New(fakeResolver{target: targetTo(t, upstream.URL)})
	s.SetHTTPSTransport(&http.Transport{TLSClientConfig: &tls.Config{}}) // verify against system roots

	rec := do(s, http.MethodGet, fmt.Sprintf("/inst/%d/api", proxyTestHTTPSBackendPort))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d (TLS verify failure)", rec.Code, http.StatusBadGateway)
	}
}

func TestProxyTunnelAliasUsesConfiguredTunnelPort(t *testing.T) {
	var gotPath, gotInstID, gotPort string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotInstID = r.Header.Get("X-Instance-Id")
		gotPort = r.Header.Get("X-Instance-Port")
		writeString(t, w, "ok")
	}))
	defer upstream.Close()

	s := New(fakeResolver{target: targetTo(t, upstream.URL)})
	s.SetAuth(true, false, proxyTestRRTPort, proxyTestTunnelWSPort)
	rec := do(s, http.MethodGet, "/tunnel/default-rtt/ws")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotPath != "/ws" {
		t.Errorf("upstream path = %q, want /ws", gotPath)
	}
	if gotInstID != "default-rtt" || gotPort != "8765" {
		t.Errorf("upstream instance/port = %q/%q, want default-rtt/8765", gotInstID, gotPort)
	}
}

func TestProxyTunnelAliasExplicitPortStripsTunnelPrefix(t *testing.T) {
	var gotPath, gotPort string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotPort = r.Header.Get("X-Instance-Port")
		writeString(t, w, "ok")
	}))
	defer upstream.Close()

	s := New(fakeResolver{target: targetTo(t, upstream.URL)})
	s.SetAuth(true, false, proxyTestRRTPort, proxyTestTunnelWSPort)
	rec := do(s, http.MethodGet, "/tunnel/default-rtt/8766/health")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotPath != "/health" {
		t.Errorf("upstream path = %q, want /health", gotPath)
	}
	if gotPort != "8766" {
		t.Errorf("upstream port = %q, want 8766", gotPort)
	}
}
