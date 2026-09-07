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
	"frontend/pkg/common/faas_common/etcd3"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/proxy"
	"frontend/pkg/frontend/sandboxrouter/route"
)

const (
	testFailureVersion        = 4
	testOldGenerationVersion  = 40
	testReadGenerationVersion = 50
	testFailureControlPort    = 8765
	testFailureExitCode       = 137
	testEpochSeconds          = 1000
	testExpiryGrace           = 2 * time.Minute
)

func failureJSON(id, runtime string, state int32, version int) []byte {
	return []byte(fmt.Sprintf(`{"instanceID":%q,"tenantID":"tenant-a","runtimeID":%q,"version":%d,`+
		`"proxyGrpcAddress":"10.0.0.1:22772","instanceStatus":{"code":%d,"exitCode":%d,"type":5,`+
		`"errCode":100,"msg":"sandbox was oom-killed"}}`, id, runtime, version, state, testFailureExitCode))
}

func requireFailure(t *testing.T, err error, state int32) *route.InstanceFailure {
	t.Helper()
	var failure *route.InstanceFailure
	if !errors.As(err, &failure) || failure.Status.Code != state {
		t.Fatalf("failure = %v, want state %d", err, state)
	}
	return failure
}

func TestFailureReachesHTTPWithoutUpstream(t *testing.T) {
	for _, tc := range []struct {
		state  int32
		status int
		code   string
	}{
		{execendpoint.StatusFatal, http.StatusGone, "SANDBOX_EXITED"},
		{execendpoint.StatusFailed, http.StatusServiceUnavailable, "SANDBOX_RECOVERING"},
		{execendpoint.StatusScheduleFailed, http.StatusConflict, "SANDBOX_SCHEDULE_FAILED"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			reader := &fakeAuthorityReader{key: instanceKey,
				value: failureJSON("inst-abc", "runtime-old", tc.state, testFailureVersion)}
			r := newInstanceInfoWatchResolverWithReader(reader)
			server := proxy.New(r)
			server.SetAuth(false, false, testFailureControlPort, 0)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/inst-abc/8765/invoke", nil))
			if response.Code != tc.status {
				t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
			}
			var body struct {
				Code     string `json:"code"`
				Message  string `json:"message"`
				ExitCode int32  `json:"exit_code"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, tc.code, body.Code)
			require.Equal(t, "sandbox was oom-killed", body.Message)
			require.EqualValues(t, testFailureExitCode, body.ExitCode)
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
	now := time.Unix(testEpochSeconds, 0)
	reader := &fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound}
	r := newInstanceInfoWatchResolverWithReader(reader)
	r.now = func() time.Time { return now }
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey,
		Value: failureJSON("inst-abc", "old", execendpoint.StatusFatal, testFailureVersion), Rev: 10})
	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey, Rev: 11})
	_, err := resolve(r)
	requireFailure(t, err, execendpoint.StatusFatal)
	now = now.Add(r.retention - time.Minute)
	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey, Rev: 12})
	now = now.Add(testExpiryGrace)
	_, err = resolve(r)
	if !errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("expired failure: %v", err)
	}
	if _, ok := execendpoint.Default().GetSummary("inst-abc"); ok {
		t.Fatal("expired summary retained")
	}
	// A new RUNNING winner replaces a retained failure immediately.
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey,
		Value: failureJSON("inst-abc", "old", execendpoint.StatusFailed, testFailureVersion), Rev: 20})
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
	r.applyPut(instanceKey, failureJSON("inst-abc", "old", execendpoint.StatusFatal, testFailureVersion))
	_, err := resolve(r)
	var failure *route.InstanceFailure
	if err == nil || errors.As(err, &failure) || errors.Is(err, route.ErrRouteNotFound) {
		t.Fatalf("authority error lost: %v", err)
	}
}

func TestStaleExitAndDeleteCannotReplaceNewGeneration(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
	oldKey := strings.Replace(instanceKey, "req0", "old-request", 1)
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: oldKey,
		Value: failureJSON("inst-abc", "old", execendpoint.StatusFatal, testOldGenerationVersion), Rev: 10})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: oldKey,
		Value: failureJSON("inst-abc", "old", execendpoint.StatusFatal, testOldGenerationVersion), Rev: 15})
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
	reader := &fakeAuthorityReader{key: instanceKey,
		value: failureJSON("inst-abc", "old", execendpoint.StatusFatal, testFailureVersion),
		gate:  gate, started: make(chan struct{})}
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
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{
		key: key, value: failureJSON(raw, "old", execendpoint.StatusFatal, testFailureVersion),
	})
	r.applyPut(key, failureJSON(raw, "old", execendpoint.StatusFatal, testFailureVersion))
	_, err := resolve(r)
	if got := requireFailure(t, err, execendpoint.StatusFatal).InstanceID; got != raw {
		t.Fatalf("raw identity = %s", got)
	}
	r.applyPut(instanceKey, failureJSON("inst-abc", "other", execendpoint.StatusFatal, testFailureVersion))
	_, err = resolve(r)
	var failure *route.InstanceFailure
	if err == nil || errors.As(err, &failure) {
		t.Fatalf("ambiguous identity leaked failure: %v", err)
	}
}

func TestDeletedFailureCacheIsBounded(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
	r.maxRetained = 2
	now := time.Unix(testEpochSeconds, 0)
	r.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("bounded-%d", i)
		key := strings.Replace(instanceKey, "inst-abc", id, 1)
		r.applyPut(key, failureJSON(id, "old", execendpoint.StatusFatal, testFailureVersion))
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

func (f revisionedReader) ReadInstanceWithRevision(ctx context.Context, id string) (instanceReadResult, error) {
	key, value, err := f.ReadInstance(ctx, id)
	return instanceReadResult{key: key, value: value, revision: f.revision}, err
}

func TestOldAbsentReadCannotRemoveNewerRunningRoute(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(revisionedReader{
		fakeAuthorityReader: &fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound},
		revision:            10,
	})
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
	r := newInstanceInfoWatchResolverWithReader(revisionedReader{
		fakeAuthorityReader: &fakeAuthorityReader{
			key: oldKey, value: failureJSON("inst-abc", "old", execendpoint.StatusFatal, testReadGenerationVersion),
		},
		revision: 10,
	})
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: instanceKey, Value: []byte(resumedJSON), Rev: 20})
	if _, err := r.refreshInstance(context.Background(), "inst-abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := resolve(r); err != nil {
		t.Fatalf("old generation replaced new runtime: %v", err)
	}
}

func TestNormalExitPreservesZeroCodeAndReturnType(t *testing.T) {
	value := []byte(`{"instanceID":"inst-abc","tenantID":"tenant-a","runtimeID":"old",` +
		`"instanceStatus":{"code":6,"exitCode":0,"type":1}}`)
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{key: instanceKey, value: value})
	_, err := resolve(r)
	failure := requireFailure(t, err, execendpoint.StatusFatal)
	if failure.Status.ExitCode != 0 || failure.Status.Type != 1 {
		t.Fatalf("normal exit lost: %+v", failure.Status)
	}
}

func TestRecreationWithinOneEtcdTransaction(t *testing.T) {
	for _, deleteFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(deleteFirst), func(t *testing.T) {
			oldKey := strings.Replace(instanceKey, "req0", "old-request", 1)
			r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
			r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: oldKey,
				Value: failureJSON("inst-abc", "old", execendpoint.StatusFatal, testFailureVersion), Rev: 10})
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

func TestDeleteRejectsPartiallyDecodedPreviousValue(t *testing.T) {
	r := newInstanceInfoWatchResolverWithReader(&fakeAuthorityReader{err: ErrAuthoritativeInstanceNotFound})
	// A type error can leave valid fields populated even though Unmarshal fails.
	previous := []byte(`{"instanceID":"inst-abc","runtimeID":"old",` +
		`"instanceStatus":{"code":6,"exitCode":137},"version":{}}`)
	r.applyEvent(&etcd3.Event{Type: etcd3.DELETE, Key: instanceKey, PrevValue: previous})
	_, err := resolve(r)
	require.True(t, errors.Is(err, route.ErrRouteNotFound), "unexpected route error: %v", err)
	require.Nil(t, r.FailureForRuntime(route.Key{SafeInstanceID: "inst-abc"}, "inst-abc", "old"))
}
