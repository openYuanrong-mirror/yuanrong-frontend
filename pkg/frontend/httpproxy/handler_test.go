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

package httpproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	shortWriteCap    = 7
	failOnThirdWrite = 3
	errorPayloadLen  = 100
	pipeTimeout      = 2 * time.Second
	halfCloseProbe   = "x"
	readProbeSize    = 1
)

type shortWriteConn struct {
	maxPerWrite int
	written     int
	buf         bytes.Buffer
}

func (s *shortWriteConn) Write(b []byte) (int, error) {
	n := len(b)
	if n > s.maxPerWrite {
		n = s.maxPerWrite
	}
	s.buf.Write(b[:n])
	s.written += n
	return n, nil
}

func (s *shortWriteConn) Read(b []byte) (int, error)       { return 0, io.EOF }
func (s *shortWriteConn) Close() error                     { return nil }
func (s *shortWriteConn) LocalAddr() net.Addr              { return nil }
func (s *shortWriteConn) RemoteAddr() net.Addr             { return nil }
func (s *shortWriteConn) SetDeadline(time.Time) error      { return nil }
func (s *shortWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (s *shortWriteConn) SetWriteDeadline(time.Time) error { return nil }

func TestWriteAllRetriesOnShortWrite(t *testing.T) {
	const total = 4096
	payload := bytes.Repeat([]byte("x"), total)
	conn := &shortWriteConn{maxPerWrite: shortWriteCap}
	if err := writeAll(conn, payload); err != nil {
		t.Fatalf("writeAll: %v", err)
	}
	if conn.written != total {
		t.Fatalf("bytes written: got %d want %d (tail dropped by short write)", conn.written, total)
	}
	if !bytes.Equal(conn.buf.Bytes(), payload) {
		t.Fatal("delivered bytes do not match payload")
	}
}

func TestWriteAllPropagatesWriteError(t *testing.T) {
	w := &errWriter{maxPerWrite: shortWriteCap, failOnCall: failOnThirdWrite}
	if err := writeAll(w, bytes.Repeat([]byte("y"), errorPayloadLen)); err == nil {
		t.Fatal("expected write error, got nil")
	}
}

type errWriter struct {
	calls       int
	maxPerWrite int
	failOnCall  int
}

func (w *errWriter) Write(b []byte) (int, error) {
	w.calls++
	if w.calls >= w.failOnCall {
		return 0, errors.New("simulated write failure")
	}
	n := len(b)
	if n > w.maxPerWrite {
		n = w.maxPerWrite
	}
	return n, nil
}

func TestFlushPrefetchForwardsBufferedBytes(t *testing.T) {
	prefetched := []byte("body-started-early")
	reader := bufio.NewReader(strings.NewReader(string(prefetched) + "trailing"))
	if _, err := reader.Peek(len(prefetched)); err != nil {
		t.Fatalf("Peek to populate buffer: %v", err)
	}
	clientBuf := bufio.NewReadWriter(reader, bufio.NewWriter(new(strings.Builder)))

	var got bytes.Buffer
	tunnel := &bytesConn{w: &got}
	flushPrefetch(clientBuf, tunnel)

	if !bytes.HasPrefix(got.Bytes(), prefetched) {
		t.Fatalf("prefetched bytes lost: got %q want prefix %q", got.Bytes(), prefetched)
	}
	if got.Len() == 0 {
		t.Fatal("no bytes forwarded to tunnel")
	}
}

type bytesConn struct {
	w *bytes.Buffer
}

func (c *bytesConn) Read(b []byte) (int, error)       { return 0, io.EOF }
func (c *bytesConn) Write(b []byte) (int, error)      { return c.w.Write(b) }
func (c *bytesConn) Close() error                     { return nil }
func (c *bytesConn) LocalAddr() net.Addr              { return nil }
func (c *bytesConn) RemoteAddr() net.Addr             { return nil }
func (c *bytesConn) SetDeadline(time.Time) error      { return nil }
func (c *bytesConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bytesConn) SetWriteDeadline(time.Time) error { return nil }

func TestPipeBidirectionalPassthrough(t *testing.T) {
	requestBytes := []byte("POST /serverless/v1/http/users?instance=i&port=18092 HTTP/1.1\r\n" +
		"Host: x\r\nContent-Length: 5\r\n\r\nhello")
	responseBytes := []byte("HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nok-body")

	clientEnd, tunnelEnd := net.Pipe()
	defer clientEnd.Close()
	defer tunnelEnd.Close()

	serverErr := make(chan error, 1)
	go sandboxEchoServer(tunnelEnd, requestBytes, responseBytes, serverErr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipeDone := make(chan struct{})
	go func() {
		pipe(ctx, cancel, clientEnd, tunnelEnd)
		close(pipeDone)
	}()

	if _, err := clientEnd.Write(requestBytes); err != nil {
		t.Fatalf("client write request: %v", err)
	}
	gotResp := make([]byte, len(responseBytes))
	if _, err := io.ReadFull(clientEnd, gotResp); err != nil {
		t.Fatalf("client read response: %v", err)
	}
	if !bytes.Equal(gotResp, responseBytes) {
		t.Fatalf("response not preserved verbatim: got %q want %q", gotResp, responseBytes)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("sandbox server side: %v", err)
	}
	if err := clientEnd.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	select {
	case <-pipeDone:
	case <-time.After(pipeTimeout):
		t.Fatal("pipe did not return after client close")
	}
}

func sandboxEchoServer(tunnelEnd io.ReadWriter, wantReq, resp []byte, errCh chan<- error) {
	got := make([]byte, len(wantReq))
	if _, err := io.ReadFull(tunnelEnd, got); err != nil {
		errCh <- err
		return
	}
	if !bytes.Equal(got, wantReq) {
		errCh <- errString("request not preserved verbatim through pipe")
		return
	}
	if _, err := tunnelEnd.Write(resp); err != nil {
		errCh <- err
		return
	}
	errCh <- nil
}

func TestPipeClosesBothOnHalfClose(t *testing.T) {
	clientEnd, tunnelEnd := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipeDone := make(chan struct{})
	go func() { pipe(ctx, cancel, clientEnd, tunnelEnd); close(pipeDone) }()

	if _, err := clientEnd.Write([]byte(halfCloseProbe)); err != nil {
		t.Fatalf("write before close: %v", err)
	}
	if err := clientEnd.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	select {
	case <-pipeDone:
	case <-time.After(pipeTimeout):
		t.Fatal("pipe did not terminate after one side closed")
	}
	buf := make([]byte, readProbeSize)
	if _, err := tunnelEnd.Read(buf); err == nil {
		t.Fatal("expected tunnel read to error after client close, got nil")
	}
}

func TestRewriteRequestRestoresPath(t *testing.T) {
	rawURL := "/serverless/v1/http/api/v1/users?instance=i&port=18092&tenant_id=default&bar=baz"
	req := mustNewRequest(t, "GET", rawURL)
	req.Host = "caller.example"

	pr, ok := rewriteRequest(httptest.NewRecorder(), req)
	if !ok {
		t.Fatal("rewriteRequest returned ok=false for a valid path")
	}
	if pr.URL.Path != "/api/v1/users" {
		t.Fatalf("path: got %q want %q", pr.URL.Path, "/api/v1/users")
	}
	if got := pr.URL.RawQuery; got != "bar=baz" {
		t.Fatalf("business query: got %q want %q (route keys must be stripped)", got, "bar=baz")
	}
	if pr.Host != "caller.example" {
		t.Fatalf("Host: got %q want %q", pr.Host, "caller.example")
	}
	tunnelEnd, sandboxEnd := net.Pipe()
	defer tunnelEnd.Close()
	go func() { defer sandboxEnd.Close(); pr.Write(tunnelEnd) }()
	got, err := http.ReadRequest(bufio.NewReader(sandboxEnd))
	if err != nil {
		t.Fatalf("read forwarded request: %v", err)
	}
	if got.URL.Path != "/api/v1/users" || got.URL.RawQuery != "bar=baz" {
		t.Fatalf("forwarded request line: path=%q query=%q, want /api/v1/users bar=baz",
			got.URL.Path, got.URL.RawQuery)
	}
}

func TestRewriteRequestBareRouteRestoresRoot(t *testing.T) {
	req := mustNewRequest(t, "GET", "/serverless/v1/http?instance=i&port=18092")
	pr, ok := rewriteRequest(httptest.NewRecorder(), req)
	if !ok {
		t.Fatal("rewriteRequest returned ok=false for a bare route")
	}
	if pr.URL.Path != "/" {
		t.Fatalf(`path: got %q want "/" (bare route must map to root)`, pr.URL.Path)
	}

	tunnelEnd, sandboxEnd := net.Pipe()
	defer tunnelEnd.Close()
	go func() { defer sandboxEnd.Close(); pr.Write(tunnelEnd) }()
	got, err := http.ReadRequest(bufio.NewReader(sandboxEnd))
	if err != nil {
		t.Fatalf("read forwarded request: %v", err)
	}
	if got.URL.Path != "/" {
		t.Fatalf(`forwarded request line: path=%q, want "/"`, got.URL.Path)
	}
}

func TestRewriteRequestStripsAuthorization(t *testing.T) {
	req := mustNewRequest(t, "GET",
		"/serverless/v1/http/users?instance=i&port=18092")
	req.Header.Set("Authorization", "Basic dTpw")

	pr, ok := rewriteRequest(httptest.NewRecorder(), req)
	if !ok {
		t.Fatal("rewriteRequest returned ok=false")
	}
	if got := pr.Header.Get("Authorization"); got != "" {
		t.Fatalf("Authorization must be stripped, got %q", got)
	}
}

func mustNewRequest(t *testing.T, method, urlStr string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, urlStr, nil)
	if err != nil {
		t.Fatalf("NewRequest(%s, %q): %v", method, urlStr, err)
	}
	return req
}

type errString string

func (e errString) Error() string { return string(e) }
