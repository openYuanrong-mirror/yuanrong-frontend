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

package resolver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/proxy"
	"frontend/pkg/frontend/sandboxrouter/route"
)

const instanceKey = "/sn/instance/business/yrk/tenant/default/function/0-svc/version/$latest/defaultaz/req0/inst-abc"

const runningJSON = `{"instanceID":"inst-abc","tenantID":"tenant-a","proxyGrpcAddress":"10.0.0.1:22772",` +
	`"instanceStatus":{"code":3},"extensions":{"portForward":"[\"tcp:31080:8765\"]"}}`

const pausedJSON = `{"instanceID":"inst-abc","tenantID":"tenant-a","functionProxyID":"InstanceManagerOwner",` +
	`"instanceStatus":{"code":13,"msg":"paused"}}`

const resumedJSON = `{"instanceID":"inst-abc","tenantID":"tenant-a","function":"default/0-svc/$latest",` +
	`"functionProxyID":"target-proxy","proxyGrpcAddress":"10.0.0.9:22772","containerID":"target-sandbox",` +
	`"instanceStatus":{"code":3},"extensions":{"portForward":"[\"public+http:42080:8765\"]"}}`

type fakeAuthorityReader struct {
	key       string
	value     []byte
	err       error
	calls     atomic.Int32
	gate      <-chan struct{}
	started   chan struct{}
	startOnce sync.Once
}

func (f *fakeAuthorityReader) ReadInstance(ctx context.Context, _ string) (string, []byte, error) {
	f.calls.Add(1)
	if f.started != nil {
		f.startOnce.Do(func() { close(f.started) })
	}
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}
	return f.key, f.value, f.err
}

func resolve(r *InstanceInfoWatchResolver) (*route.Target, error) {
	return r.Resolve(context.Background(), route.Key{SafeInstanceID: "inst-abc", Port: 8765})
}

// Verifies the etcd event-type mapping end to end: PUT(running) adds, DELETE removes.
func TestApplyEventPutThenDelete(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(
		&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})

	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(runningJSON)})
	if _, err := resolve(r); err != nil {
		t.Fatalf("route should resolve after PUT(running): %v", err)
	}

	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey})
	if _, err := resolve(r); !errors.Is(err, route.ErrRouteNotFound) {
		t.Errorf("route should be gone after DELETE, got %v", err)
	}
}

// SYNCED bypasses the filter and must be a no-op (not panic).
func TestApplyEventSyncedIgnored(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(
		&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
	r.applyEvent(&etcd3.Event{Type: etcd3.SYNCED})
	if _, err := resolve(r); !errors.Is(err, route.ErrRouteNotFound) {
		t.Errorf("SYNCED should add nothing, got %v", err)
	}
}

func TestSandboxInstanceFilter(t *testing.T) {
	// Full instance key (depth 14) is kept (filter returns false).
	if sandboxInstanceFilter(&etcd3.Event{Key: instanceKey}) {
		t.Error("full instance key should be kept (filter false)")
	}
	// Shallower keys are skipped (filter returns true).
	if !sandboxInstanceFilter(&etcd3.Event{Key: "/sn/instance/business/yrk"}) {
		t.Error("shallow key should be skipped (filter true)")
	}
}

// The same watch event must also populate the exec endpoint cache so the web
// terminal exec path can resolve proxyGrpcAddress locally; DELETE clears it.
func TestApplyEventFeedsExecEndpointCache(t *testing.T) {
	r := NewInstanceInfoWatchResolver()
	execendpoint.Default().Delete("inst-abc") // isolate from other tests' singleton state

	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(runningJSON)})
	ep, ok := execendpoint.Default().Get("inst-abc")
	if !ok {
		t.Fatal("exec endpoint should be cached after PUT(running)")
	}
	if ep.ProxyGrpcAddress != "10.0.0.1:22772" {
		t.Errorf("ProxyGrpcAddress = %q, want 10.0.0.1:22772", ep.ProxyGrpcAddress)
	}

	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey})
	if _, ok := execendpoint.Default().Get("inst-abc"); ok {
		t.Error("exec endpoint should be gone after DELETE")
	}
}

func TestPausedControlRouteRejectsSandboxDataPlane(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(
		&fakeAuthorityReader{key: instanceKey, value: []byte(pausedJSON)})
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })

	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(pausedJSON)})
	_, err := resolve(r)

	if err == nil || !strings.Contains(err.Error(), "instance paused") {
		t.Fatalf("paused route error = %v, want instance paused", err)
	}
}

func TestRunningCacheMissReadThroughInstallsWinnerForFirstRequest(t *testing.T) {
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })
	reader := &fakeAuthorityReader{key: instanceKey, value: []byte(resumedJSON)}
	r := newInstanceInfoWatchResolverWithReader(reader)

	target, err := resolve(r)

	if err != nil {
		t.Fatalf("first cache-miss request should resolve from authoritative winner: %v", err)
	}
	if got := target.TargetURL.String(); got != "http://10.0.0.9:42080" {
		t.Fatalf("target = %q, want target winner host port", got)
	}
	if reader.calls.Load() != 1 {
		t.Fatalf("authoritative reads = %d, want 1", reader.calls.Load())
	}
	ep, ok := execendpoint.Default().Get("inst-abc")
	if !ok || ep.ProxyGrpcAddress != "10.0.0.9:22772" || ep.ContainerID != "target-sandbox" {
		t.Fatalf("exec endpoint = %+v, present=%v; want authoritative target", ep, ok)
	}
}

func TestStalePausedCacheReadThroughConvergesToRunningWinner(t *testing.T) {
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })
	reader := &fakeAuthorityReader{key: instanceKey, value: []byte(resumedJSON)}
	r := newInstanceInfoWatchResolverWithReader(reader)
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(runningJSON)})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(pausedJSON)})

	target, err := resolve(r)

	if err != nil {
		t.Fatalf("stale PAUSED cache should converge on first request: %v", err)
	}
	if got := target.TargetURL.String(); got != "http://10.0.0.9:42080" {
		t.Fatalf("target = %q, want winner target", got)
	}
	if execendpoint.Default().IsPaused("inst-abc") {
		t.Fatal("authoritative RUNNING read-through must clear stale PAUSED state")
	}
}

func TestFirstPublicRequestReadThroughsWinnerAndProxies(t *testing.T) {
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/first" {
			t.Fatalf("upstream path = %q, want /first", request.URL.Path)
		}
		if _, err := w.Write([]byte("winner-public-route")); err != nil {
			t.Errorf("write winner response: %v", err)
		}
	}))
	defer upstream.Close()
	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	_, hostPort, found := strings.Cut(parsed.Host, ":")
	if !found {
		t.Fatalf("upstream has no host port: %s", parsed.Host)
	}
	winner := fmt.Sprintf(
		`{"instanceID":"inst-abc","tenantID":"tenant-a","function":"default/0-svc/$latest",`+
			`"functionProxyID":"target-proxy","proxyGrpcAddress":"127.0.0.1:22772",`+
			`"containerID":"target-sandbox","instanceStatus":{"code":3},`+
			`"extensions":{"portForward":"[\"public+http:%s:8765\"]"}}`, hostPort)
	reader := &fakeAuthorityReader{key: instanceKey, value: []byte(winner)}
	router := proxy.New(newInstanceInfoWatchResolverWithReader(reader))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "http://router/inst-abc/8765/first", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || recorder.Body.String() != "winner-public-route" {
		t.Fatalf("first public response = (%d, %q), want winner response", recorder.Code, recorder.Body.String())
	}
	if reader.calls.Load() != 1 {
		t.Fatalf("authoritative reads = %d, want 1", reader.calls.Load())
	}
}

func TestReadThroughAuthoritativePausedReturnsPausedNotNotFound(t *testing.T) {
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })
	r := newInstanceInfoWatchResolverWithReader(
		&fakeAuthorityReader{key: instanceKey, value: []byte(pausedJSON)})

	_, err := resolve(r)

	if !errors.Is(err, route.ErrInstancePaused) {
		t.Fatalf("error = %v, want instance paused", err)
	}
	if errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("PAUSED must not be reported as 404: %v", err)
	}
}

func TestReadThroughOnlyMissingAuthorityReturnsNotFound(t *testing.T) {
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })
	r := newInstanceInfoWatchResolverWithReader(
		&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})

	_, err := resolve(r)

	if !errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("error = %v, want route not found", err)
	}
}

func TestReadThroughFailureIsUnavailableNotNotFound(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(
		&fakeAuthorityReader{err: errors.New("etcd unavailable")})

	_, err := resolve(r)

	if err == nil || errors.Is(err, route.ErrRouteNotFound) || errors.Is(err, route.ErrInstancePaused) {
		t.Fatalf("error = %v, want explicit authoritative-read failure", err)
	}
}

func TestConcurrentCacheMissesShareOneAuthoritativeRead(t *testing.T) {
	execendpoint.Default().Delete("inst-abc")
	t.Cleanup(func() { execendpoint.Default().Delete("inst-abc") })
	gate := make(chan struct{})
	started := make(chan struct{})
	reader := &fakeAuthorityReader{
		key: instanceKey, value: []byte(resumedJSON), gate: gate, started: started,
	}
	r := newInstanceInfoWatchResolverWithReader(reader)

	const callers = 16
	var wg sync.WaitGroup
	wg.Add(callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			_, err := resolve(r)
			errs <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("authoritative read did not start")
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent resolve failed: %v", err)
		}
	}
	if reader.calls.Load() != 1 {
		t.Fatalf("authoritative reads = %d, want one singleflight call", reader.calls.Load())
	}
}
