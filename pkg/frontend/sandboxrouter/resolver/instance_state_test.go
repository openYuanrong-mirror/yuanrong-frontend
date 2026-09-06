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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/proxy"
	"frontend/pkg/frontend/sandboxrouter/route"
)

func failureJSON(id, runtime string, state int, version int) []byte {
	return []byte(fmt.Sprintf(`{"instanceID":%q,"tenantID":"tenant-a","runtimeID":%q,"version":%d,"proxyGrpcAddress":"10.0.0.1:22772","instanceStatus":{"code":%d,"exitCode":137,"type":5,"errCode":100,"msg":"sandbox was oom-killed"}}`, id, runtime, version, state))
}

func requireFailure(t *testing.T, err error, state int) *route.InstanceFailure {
	t.Helper()
	var failure *route.InstanceFailure
	if !errors.As(err, &failure) || failure.Status.Code != int32(state) {
		t.Fatalf("failure = %v, want state %d", err, state)
	}
	return failure
}

func TestFailureReachesHTTPWithoutUpstream(t *testing.T) {
	for _, tc := range []struct {
		state, status int
		code          string
	}{{6, 410, "SANDBOX_EXITED"}, {4, 503, "SANDBOX_RECOVERING"}, {7, 409, "SANDBOX_SCHEDULE_FAILED"}} {
		t.Run(tc.code, func(t *testing.T) {
			reader := &fakeAuthorityReader{key: instanceKey, value: failureJSON("inst-abc", "runtime-old", tc.state, 4)}
			r := newInstanceInfoWatchResolverWithReader(reader)
			server := proxy.New(r)
			server.SetAuth(false, false, 8765, 0)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/inst-abc/8765/invoke", nil))
			if response.Code != tc.status {
				t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["code"] != tc.code || body["message"] != "sandbox was oom-killed" || body["exit_code"] != float64(137) {
				t.Fatalf("body = %v", body)
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("failure response must not be cached")
			}
			if _, ok := execendpoint.Default().Get("inst-abc"); ok {
				t.Fatal("failed instance kept an exec endpoint")
			}
		})
	}
}

func TestFailureRetentionExpiryAndRecovery(t *testing.T) {
	now := time.Unix(1000, 0)
	reader := &fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound}
	r := newInstanceInfoWatchResolverWithReader(reader)
	r.now = func() time.Time { return now }
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: failureJSON("inst-abc", "old", 6, 4), Rev: 10})
	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey, Rev: 11})
	_, err := resolve(r)
	requireFailure(t, err, 6)
	now = now.Add(9 * time.Minute)
	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey, Rev: 12})
	now = now.Add(2 * time.Minute)
	_, err = resolve(r)
	if !errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("expired failure: %v", err)
	}
	if _, ok := execendpoint.Default().GetSummary("inst-abc"); ok {
		t.Fatal("expired summary retained")
	}
	// A new RUNNING winner replaces a retained failure immediately.
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: failureJSON("inst-abc", "old", 4, 4), Rev: 20})
	reader.err = nil
	reader.key = instanceKey
	reader.value = []byte(strings.Replace(resumedJSON, `"instanceID"`, `"version":5,"instanceID"`, 1))
	target, err := resolve(r)
	if err != nil || target == nil {
		t.Fatalf("recovery did not restore routing: %v", err)
	}
}

func TestFailureDoesNotHideAuthorityError(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: errors.New("etcd unavailable")})
	r.applyPut(instanceKey, failureJSON("inst-abc", "old", 6, 4))
	_, err := resolve(r)
	var failure *route.InstanceFailure
	if err == nil || errors.As(err, &failure) || errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("authority error lost: %v", err)
	}
}

func TestStaleExitAndDeleteCannotReplaceNewGeneration(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
	oldKey := strings.Replace(instanceKey, "req0", "old-request", 1)
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: oldKey, Value: failureJSON("inst-abc", "old", 6, 40), Rev: 10})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: oldKey, Value: failureJSON("inst-abc", "old", 6, 40), Rev: 15})
	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: oldKey, Rev: 21})
	target, err := resolve(r)
	if err != nil || target == nil {
		t.Fatalf("stale event removed running generation: %v", err)
	}
	if r.FailureForRuntime(route.Key{SafeInstanceID: "inst-abc"}, "inst-abc", "old") != nil {
		t.Fatal("stale runtime failure returned")
	}
}

func TestConcurrentWatchWinsOverReadThrough(t *testing.T) {
	gate := make(chan struct{})
	reader := &fakeAuthorityReader{key: instanceKey, value: failureJSON("inst-abc", "old", 6, 4), gate: gate, started: make(chan struct{})}
	r := newInstanceInfoWatchResolverWithReader(reader)
	done := make(chan error, 1)
	go func() { _, err := resolve(r); done <- err }()
	<-reader.started
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20})
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("old read replaced watch: %v", err)
	}
}

func TestSanitizedFailureIdentityAndCollision(t *testing.T) {
	raw := "inst_abc"
	key := strings.Replace(instanceKey, "inst-abc", raw, 1)
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{key: key, value: failureJSON(raw, "old", 6, 4)})
	r.applyPut(key, failureJSON(raw, "old", 6, 4))
	_, err := resolve(r)
	if got := requireFailure(t, err, 6).InstanceID; got != raw {
		t.Fatalf("raw identity = %s", got)
	}
	r.applyPut(instanceKey, failureJSON("inst-abc", "other", 6, 4))
	_, err = resolve(r)
	var failure *route.InstanceFailure
	if err == nil || errors.As(err, &failure) {
		t.Fatalf("ambiguous identity leaked failure: %v", err)
	}
}

func TestDeletedFailureCacheIsBounded(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
	r.maxRetained = 2
	now := time.Unix(1000, 0)
	r.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("bounded-%d", i)
		key := strings.Replace(instanceKey, "inst-abc", id, 1)
		r.applyPut(key, failureJSON(id, "old", 6, 4))
		r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: key})
		now = now.Add(time.Second)
	}
	_, err := r.Resolve(context.Background(), route.Key{SafeInstanceID: "bounded-0", Port: 8765})
	if !errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("oldest retained failure survived capacity limit: %v", err)
	}
}

type revisionedReader struct {
	*fakeAuthorityReader
	revision int64
}

func (f revisionedReader) ReadInstanceWithRevision(ctx context.Context, id string) (string, []byte, int64, error) {
	key, value, err := f.ReadInstance(ctx, id)
	return key, value, f.revision, err
}

func TestOldAbsentReadCannotRemoveNewerRunningRoute(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(revisionedReader{&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound}, 10})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20})
	if _, err := r.refreshInstance(context.Background(), "inst-abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolve(r); err != nil {
		t.Fatalf("old absence removed new running route: %v", err)
	}
}

func TestOldReadCannotReplaceNewGeneration(t *testing.T) {
	oldKey := strings.Replace(instanceKey, "req0", "old-request", 1)
	r := newInstanceInfoWatchResolverWithReader(revisionedReader{&fakeAuthorityReader{key: oldKey, value: failureJSON("inst-abc", "old", 6, 50)}, 10})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20})
	if _, err := r.refreshInstance(context.Background(), "inst-abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolve(r); err != nil {
		t.Fatalf("old generation replaced new runtime: %v", err)
	}
}

func TestNormalExitPreservesZeroCodeAndReturnType(t *testing.T) {
	value := []byte(`{"instanceID":"inst-abc","tenantID":"tenant-a","runtimeID":"old","instanceStatus":{"code":6,"exitCode":0,"type":1}}`)
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{key: instanceKey, value: value})
	_, err := resolve(r)
	failure := requireFailure(t, err, 6)
	if failure.Status.ExitCode != 0 || failure.Status.Type != 1 {
		t.Fatalf("normal exit lost: %+v", failure.Status)
	}
}

func TestRecreationWithinOneEtcdTransaction(t *testing.T) {
	for _, deleteFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(deleteFirst), func(t *testing.T) {
			oldKey := strings.Replace(instanceKey, "req0", "old-request", 1)
			r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
			r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: oldKey, Value: failureJSON("inst-abc", "old", 6, 4), Rev: 10})
			deleted := &etcd3.Event{Type: etcd3.DELETE, Key: oldKey, Rev: 20}
			created := &etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20}
			if deleteFirst {
				r.applyEvent(deleted)
				r.applyEvent(created)
			} else {
				r.applyEvent(created)
				r.applyEvent(deleted)
			}
			if _, err := resolve(r); err != nil {
				t.Fatalf("new runtime rejected at shared revision: %v", err)
			}
		})
	}
}
