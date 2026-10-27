/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

package wsproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/tracer"
	"frontend/pkg/common/faas_common/utils"
)

const testTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// testWaitTimeout bounds the waits for background cooperation in these tests
// (fake-proxy dial observation, pipe shutdown): generous enough to avoid
// flakes on slow CI, small enough to fail fast when something is stuck.
const testWaitTimeout = 5 * time.Second

// fakeSpan implements trace.Span and records the calls relevant to the tunnel
// failure bookkeeping (RecordError/SetStatus/End).
type fakeSpan struct {
	embedded.Span // satisfies the unexported embedded.Span method
	recording     bool
	recorded      error
	statusCode    codes.Code
	statusMsg     string
	ended         int
}

func (f *fakeSpan) End(...trace.SpanEndOption)                    { f.ended++ }
func (f *fakeSpan) SpanContext() trace.SpanContext                { return trace.SpanContext{} }
func (f *fakeSpan) IsRecording() bool                             { return f.recording }
func (f *fakeSpan) SetAttributes(...attribute.KeyValue)           {}
func (f *fakeSpan) RecordError(err error, _ ...trace.EventOption) { f.recorded = err }
func (f *fakeSpan) SetStatus(c codes.Code, msg string)            { f.statusCode, f.statusMsg = c, msg }
func (f *fakeSpan) AddEvent(string, ...trace.EventOption)         {}
func (f *fakeSpan) SetName(string)                                {}
func (f *fakeSpan) TracerProvider() trace.TracerProvider          { return nil }

// requestWithSpan wraps r so trace.SpanFromContext(r.Context()) returns span,
// exactly like the WrapGinHandler-wrapped /serverless/v1/{ws,http} requests.
func requestWithSpan(r *http.Request, span trace.Span) *http.Request {
	return r.WithContext(trace.ContextWithSpan(r.Context(), span))
}

// setupTracingForTest installs a real in-process tracer provider (AlwaysSample)
// plus the W3C propagator and force-enables tracer.EnableCommonTracer, mirroring
// the api/agent handler_trace_test.go house pattern. Everything is restored via
// t.Cleanup.
func setupTracingForTest(t *testing.T) {
	t.Helper()
	originalProvider := otel.GetTracerProvider()
	originalPropagator := otel.GetTextMapPropagator()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(originalProvider)
		otel.SetTextMapPropagator(originalPropagator)
	})
	patches := gomonkey.ApplyFunc(tracer.EnableCommonTracer, func() bool { return true })
	t.Cleanup(patches.Reset)
}

// recordTunnelSpanError and the explicit End() in HandleWebSocket are only
// sound if tracer.WrapGinHandler writes the entry span back into c.Request's
// context before the wrapped handler runs — that is exactly how
// /serverless/v1/{ws,http} are registered (traceHTTPHandler ->
// WrapGinHandler(gin.WrapF(handler))). The tests above cover the span
// bookkeeping with a hand-injected span; this one pins the wrapper side of
// that contract against the real WrapGinHandler + gin.WrapF chain, so a future
// wrapper refactor that only injects headers fails here instead of silently
// turning every tunnel-failure span into a no-op (the "error rate stuck at
// zero" failure mode).
func TestWrapGinHandlerExposesRecordingSpanToWrappedHandler(t *testing.T) {
	setupTracingForTest(t)

	var handlerSeesRecordingSpan bool
	wrapped := tracer.WrapGinHandler(gin.WrapF(func(_ http.ResponseWriter, r *http.Request) {
		// Sampled inside the handler: the wrapper's deferred span.End() runs
		// after this returns and would flip IsRecording to false.
		handlerSeesRecordingSpan = trace.SpanFromContext(r.Context()).IsRecording()
	}))
	wrapped(&gin.Context{Request: httptest.NewRequest(http.MethodGet, "/serverless/v1/ws", nil)})

	require.True(t, handlerSeesRecordingSpan,
		"the wrapped http handler must see a recording span in r.Context()")
}

// the tunnel header carries the optional traceparent field when
// set and omits it entirely (omitempty) when empty; the proxy-side parser
// accepts both shapes.
func TestTunnelHeaderCarriesOptionalTraceParent(t *testing.T) {
	header := tunnelHeader{
		TunnelVersion: tunnelVersion,
		InstanceID:    "inst-trace",
		Protocol:      "tcp",
		TargetPort:    defaultAgentWSPort,
		RequestID:     "req-trace",
		TraceID:       "trace-ws-1",
		TraceParent:   testTraceParent,
	}
	var buf bytes.Buffer
	require.NoError(t, writeFramedJSON(&buf, header))
	require.Contains(t, buf.String(), `"traceparent":"`+testTraceParent+`"`)
	require.Contains(t, buf.String(), `"traceID":"trace-ws-1"`)

	var got tunnelHeader
	require.NoError(t, readFramedJSON(bytes.NewReader(buf.Bytes()), &got))
	require.Equal(t, header.TraceParent, got.TraceParent)
	require.Equal(t, header.TraceID, got.TraceID)
}

func TestTunnelHeaderOmitsTraceFieldsWhenEmpty(t *testing.T) {
	header := tunnelHeader{
		TunnelVersion: tunnelVersion,
		InstanceID:    "inst-legacy",
		Protocol:      "tcp",
		TargetPort:    defaultAgentWSPort,
		RequestID:     "req-legacy",
	}
	var buf bytes.Buffer
	require.NoError(t, writeFramedJSON(&buf, header))
	require.NotContains(t, buf.String(), "traceparent")
	require.NotContains(t, buf.String(), "traceID")
}

// recordTunnelSpanError marks a recording span with the failure.
func TestRecordTunnelSpanErrorMarksRecordingSpan(t *testing.T) {
	span := &fakeSpan{recording: true}
	r := requestWithSpan(httptest.NewRequest(http.MethodGet, "/serverless/v1/ws", nil), span)

	recordTunnelSpanError(r, errors.New("tunnel refused"))

	require.NotNil(t, span.recorded)
	require.Equal(t, "tunnel refused", span.recorded.Error())
	require.Equal(t, codes.Error, span.statusCode)
	require.Equal(t, "tunnel refused", span.statusMsg)
}

// non-recording spans (tracing disabled) are left untouched — the
// helper is a strict no-op, not even a panic.
func TestRecordTunnelSpanErrorSkipsNonRecordingSpan(t *testing.T) {
	span := &fakeSpan{recording: false}
	r := requestWithSpan(httptest.NewRequest(http.MethodGet, "/serverless/v1/ws", nil), span)

	recordTunnelSpanError(r, errors.New("tunnel refused"))

	require.Nil(t, span.recorded)
	require.NotEqual(t, codes.Error, span.statusCode)
}

// a request without any span in its context resolves to the noop span —
// no panic, no effect.
func TestRecordTunnelSpanErrorToleratesMissingSpan(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/serverless/v1/ws", nil)

	recordTunnelSpanError(r, errors.New("tunnel refused")) // must not panic
}

// fakeTunnelProxy impersonates the function_proxy tcp.tunnel server on a real
// local listener: it reads the framed-JSON tunnel header and answers with a
// canned response, capturing the header for assertions. Driving the real
// dialTunnel wire path (instead of gomonkey-patching dialTunnel, whose
// one-line body gets inlined into dialInstanceTunnel and silently defeats
// ApplyFunc) keeps these tests deterministic under the default compiler
// settings.
type fakeTunnelProxy struct {
	listener net.Listener
	headerCh chan tunnelHeader
}

// startFakeTunnelProxy starts a tunnel proxy that answers every dial with
// response. The listener is closed via t.Cleanup. The component mTLS config
// defaults to disabled (zero-value config), so dialInstanceTunnel dials the
// listener in plain TCP — no loadTunnelTLSConfig stubbing needed either.
func startFakeTunnelProxy(t *testing.T, response tunnelResponse) *fakeTunnelProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxy := &fakeTunnelProxy{listener: listener, headerCh: make(chan tunnelHeader, 1)}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var header tunnelHeader
		if err := readFramedJSON(conn, &header); err != nil {
			return
		}
		proxy.headerCh <- header
		if err := writeFramedJSON(conn, response); err != nil {
			return
		}
		// Hold the conn open until the client hangs up so the success case's
		// tunnel survives past dialInstanceTunnel's return.
		if _, cerr := io.Copy(io.Discard, conn); cerr != nil {
			// The drain always ends in an error (EOF or connection reset when
			// the client hangs up) — that is the expected termination, not a
			// failure, so there is nothing to handle.
		}
	}()
	t.Cleanup(func() {
		if cerr := listener.Close(); cerr != nil {
			// Listener already closed or never accepted — cleanup is
			// best-effort.
		}
	})
	return proxy
}

// addr is the dial endpoint (host:port) for dialInstanceTunnel.
func (p *fakeTunnelProxy) addr() string {
	return p.listener.Addr().String()
}

// capturedHeader returns the header the proxy read off the wire.
func (p *fakeTunnelProxy) capturedHeader(t *testing.T) tunnelHeader {
	t.Helper()
	select {
	case header := <-p.headerCh:
		return header
	case <-time.After(testWaitTimeout):
		t.Fatal("fake tunnel proxy did not observe a dial")
		return tunnelHeader{}
	}
}

// on a proxy rejection the entry span is marked with the
// error, and the tunnel header forwarded to the proxy carries both TraceID and
// the wrapper-injected TraceParent.
func TestDialInstanceTunnelRecordsFailureAndForwardsTraceContext(t *testing.T) {
	proxy := startFakeTunnelProxy(t, tunnelResponse{OK: false, Message: "tunnel refused"})

	span := &fakeSpan{recording: true}
	r := httptest.NewRequest(http.MethodGet, "/serverless/v1/ws", nil)
	r.Header.Set(constant.HeaderTraceID, "trace-dial-1")
	r.Header.Set(constant.HeaderTraceParent, testTraceParent)
	r = requestWithSpan(r, span)

	w := httptest.NewRecorder()
	_, ok := dialInstanceTunnel(w, r, "inst-dial", defaultAgentWSPort, proxy.addr())

	require.False(t, ok)
	require.Equal(t, http.StatusBadGateway, w.Code)
	require.NotNil(t, span.recorded)
	require.Contains(t, span.recorded.Error(), "tunnel refused")
	require.Equal(t, codes.Error, span.statusCode)
	// The correlation fields made it onto the wire.
	header := proxy.capturedHeader(t)
	require.Equal(t, "inst-dial", header.InstanceID)
	require.Equal(t, defaultAgentWSPort, header.TargetPort)
	require.Equal(t, "trace-dial-1", header.TraceID)
	require.Equal(t, testTraceParent, header.TraceParent)
}

// on a successful dial the
// tunnelContext carries the request's X-Trace-Id for the closeBoth log.
func TestDialInstanceTunnelPopulatesTraceIDOnSuccess(t *testing.T) {
	proxy := startFakeTunnelProxy(t, tunnelResponse{OK: true})

	r := httptest.NewRequest(http.MethodGet, "/serverless/v1/ws", nil)
	r.Header.Set(constant.HeaderTraceID, "trace-dial-2")
	r.Header.Set(constant.HeaderTraceParent, testTraceParent)

	w := httptest.NewRecorder()
	tc, ok := dialInstanceTunnel(w, r, "inst-dial-ok", defaultAgentWSPort, proxy.addr())
	require.True(t, ok)
	defer tc.tunnelConn.Close()

	require.Equal(t, "trace-dial-2", tc.traceID)
	// The correlation fields made it onto the wire.
	header := proxy.capturedHeader(t)
	require.Equal(t, "trace-dial-2", header.TraceID)
	require.Equal(t, testTraceParent, header.TraceParent)
}

// capturingLogger captures Infof lines by embedding the no-op utils.FakeLogger
// (which satisfies the FormatLogger interface) and overriding Infof.
type capturingLogger struct {
	utils.FakeLogger
	mu    sync.Mutex
	lines []string
}

func (c *capturingLogger) Infof(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *capturingLogger) hasTraceID(traceID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, line := range c.lines {
		if strings.Contains(line, "trace_id="+traceID) {
			return true
		}
	}
	return false
}

// failingConn is a net.Conn whose reads fail immediately with a non-"closed
// network" error, deterministically driving the client->tunnel copy of pipe()
// into the closeBoth log path (closing a net.Pipe end instead races the
// EOF-swallowing opposite direction for the log-once).
type failingConn struct {
	err error
}

func (c *failingConn) Read([]byte) (int, error)         { return 0, c.err }
func (c *failingConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *failingConn) Close() error                     { return nil }
func (c *failingConn) LocalAddr() net.Addr              { return nil }
func (c *failingConn) RemoteAddr() net.Addr             { return nil }
func (c *failingConn) SetDeadline(time.Time) error      { return nil }
func (c *failingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *failingConn) SetWriteDeadline(time.Time) error { return nil }

// when the session breaks mid-stream, the closeBoth interruption
// log carries the tunnel's trace_id so the break is attributable even though
// the connection-establishment span already ended.
func TestPipeCloseBothLogsTraceID(t *testing.T) {
	capture := &capturingLogger{}
	original := log.GetLogger()
	log.SetupLogger(capture)
	t.Cleanup(func() { log.SetupLogger(original) })

	_, tunnelEnd := net.Pipe()
	tc := tunnelContext{
		tunnelConn: tunnelEnd,
		instanceID: "inst-log",
		port:       defaultAgentWSPort,
		requestID:  "req-log",
		traceID:    "trace-log-1",
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipe(ctx, cancel, &failingConn{err: errors.New("simulated client disconnect")}, tc)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(testWaitTimeout):
		t.Fatalf("pipe did not observe the failing client side")
	}
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatalf("pipe goroutine did not exit")
	}
	require.True(t, capture.hasTraceID("trace-log-1"),
		"closeBoth log lines %v do not carry trace_id=trace-log-1", capture.lines)
}

// recordingSpanProcessor is a minimal in-memory sdktrace.SpanProcessor.
type recordingSpanProcessor struct {
	mu    sync.Mutex
	ended []string
}

func (p *recordingSpanProcessor) OnStart(_ context.Context, _ sdktrace.ReadWriteSpan) {}
func (p *recordingSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ended = append(p.ended, s.Name())
}
func (p *recordingSpanProcessor) Shutdown(_ context.Context) error   { return nil }
func (p *recordingSpanProcessor) ForceFlush(_ context.Context) error { return nil }

// span.End() is idempotent — a second End must not panic and must
// not export twice. HandleWebSocket calls End() explicitly after the upgrade
// request was forwarded while the wrapper's deferred End() fires later on the
// same span.
func TestSpanEndIsIdempotentSingleExport(t *testing.T) {
	processor := &recordingSpanProcessor{}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer func() { _ = provider.Shutdown(context.Background()) }()

	_, span := provider.Tracer("wsproxy-test").Start(context.Background(), "connection-established")
	span.End()
	span.End() // second End must be a no-op

	processor.mu.Lock()
	defer processor.mu.Unlock()
	require.Len(t, processor.ended, 1)
	require.Equal(t, "connection-established", processor.ended[0])
}
