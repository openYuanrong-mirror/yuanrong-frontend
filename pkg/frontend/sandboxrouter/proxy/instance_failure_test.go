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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"frontend/pkg/frontend/sandboxrouter/route"
)

type failureResolver struct {
	failure *route.InstanceFailure
	target  *route.Target
}

func (f failureResolver) Resolve(context.Context, route.Key) (*route.Target, error) {
	if f.target != nil {
		return f.target, nil
	}
	return nil, f.failure
}
func (f failureResolver) FailureForRuntime(key route.Key, id, runtime string) *route.InstanceFailure {
	if f.failure.InstanceID == id && f.failure.RuntimeID == runtime {
		return f.failure
	}
	return nil
}
func oomFailure() *route.InstanceFailure {
	return &route.InstanceFailure{InstanceID: "inst-abc", RuntimeID: "runtime-old", Tenant: "tenant-a",
		Status: route.InstanceStatus{Code: 6, Msg: "private OOM reason", ExitCode: 137, Type: 5, ErrCode: 100}}
}
func TestFailureAuthorizationAndPublicDetails(t *testing.T) {
	for _, tc := range []struct {
		name, path, token string
		status            int
		detail            bool
	}{
		{"unauthenticated", "/inst-abc/50090/invoke", "", 401, false},
		{"other tenant", "/inst-abc/50090/invoke", mintJWT(t, "tenant-b", farFuture), 403, false},
		{"owner", "/inst-abc/50090/invoke", mintJWT(t, "tenant-a", farFuture), 410, true},
		{"public port", "/inst-abc/8080/", "", 410, false},
		{"public tunnel", "/inst-abc/8765/", "", 410, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(failureResolver{failure: oomFailure()})
			s.SetAuth(true, false, 50090, 8765)
			rec := doAuth(s, http.MethodPost, tc.path, tc.token)
			if rec.Code != tc.status {
				t.Fatalf("HTTP %d: %s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "private OOM reason") != tc.detail {
				t.Fatalf("unexpected failure detail: %s", rec.Body.String())
			}
			if !tc.detail && strings.Contains(rec.Body.String(), "exit_code") {
				t.Fatalf("public exit metadata: %s", rec.Body.String())
			}
		})
	}
}

func TestTransportFailureUsesOnlyMatchingRuntime(t *testing.T) {
	for _, runtime := range []string{"runtime-old", "runtime-new", ""} {
		t.Run("runtime-"+runtime, func(t *testing.T) {
			target := targetTo(t, "http://127.0.0.1:1")
			target.InstanceID = "inst-abc"
			target.RuntimeID = runtime
			target.Tenant = "tenant-a"
			s := New(failureResolver{failure: oomFailure(), target: target})
			s.SetAuth(true, false, 50090, 8765)
			s.proxy.Transport = roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection reset") })
			rec := doAuth(s, http.MethodPost, "/inst-abc/50090/invoke", mintJWT(t, "tenant-a", farFuture))
			want := 502
			if runtime == "runtime-old" {
				want = 410
			}
			if rec.Code != want {
				t.Fatalf("HTTP %d want %d: %s", rec.Code, want, rec.Body.String())
			}
		})
	}
}

func TestFrontendAuthenticatedFailureStillChecksTenant(t *testing.T) {
	s := New(failureResolver{failure: oomFailure()})
	s.SetAuth(true, false, 50090, 8765)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		req := httptest.NewRequest(http.MethodPost, "/inst-abc/50090/invoke", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		req.Header.Set(internalSrcHeader, internalSrcAuthenticatedValue)
		req.Header.Set(internalTenantHeader, tenant)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		want := 410
		if tenant == "tenant-b" {
			want = 403
		}
		if rec.Code != want {
			t.Fatalf("tenant %s HTTP %d: %s", tenant, rec.Code, rec.Body.String())
		}
	}
}
