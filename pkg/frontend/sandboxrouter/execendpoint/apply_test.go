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

package execendpoint

import (
	"strconv"
	"testing"
)

const instanceKey = "/sn/instance/business/yrk/tenant/default/function/0-svc/version/$latest/" +
	"defaultaz/req0/inst-abc"

// Shape mirrors a real /sn/instance value (MessageToJsonString output) reduced
// to the fields the exec path reads.
const runningJSON = `{"instanceID":"inst-abc","proxyGrpcAddress":"10.0.0.1:22774",` +
	`"containerID":"sbox-4fb6aa1c","tenantID":"default","functionProxyID":"node-a",` +
	`"function":"func-a","startTime":"1700000000",` +
	`"resources":{"resources":{"CPU":{"scalar":{"value":1000}},"Memory":{"scalar":{"value":2048}}}},` +
	`"scheduleOption":{"extension":{"rootfs":"{\"runtime\":\"runsc\",\"type\":\"image\",` +
	`\"imageurl\":\"registry.example.com/ns/image:tag\"}"}},` +
	`"instanceStatus":{"code":3,"msg":"running"},` +
	`"extensions":{"portForward":"[\"tcp:31080:8765\"]"}}`

func TestApplyEventPutRunning(t *testing.T) {
	s := NewStore()
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(runningJSON))

	got, ok := s.Get("inst-abc")
	if !ok {
		t.Fatal("RUNNING instance should be cached after PUT")
	}
	if got.ProxyGrpcAddress != "10.0.0.1:22774" {
		t.Errorf("ProxyGrpcAddress = %q, want 10.0.0.1:22774", got.ProxyGrpcAddress)
	}
	if got.ContainerID != "sbox-4fb6aa1c" {
		t.Errorf("ContainerID = %q, want sbox-4fb6aa1c", got.ContainerID)
	}
	summaries := s.ListSummaries("default", "")
	if len(summaries) != 1 {
		t.Fatalf("expected one running summary, got %+v", summaries)
	}
	if summaries[0].Function != "func-a" || summaries[0].Resources["CPU"].Scalar.Value != 1000 {
		t.Fatalf("unexpected summary: %+v", summaries[0])
	}
	if summaries[0].NodeID != "node-a" {
		t.Fatalf("summary NodeID = %q, want node-a", summaries[0].NodeID)
	}
	if summaries[0].Image != "registry.example.com/ns/image:tag" {
		t.Fatalf("summary image = %q, want registry.example.com/ns/image:tag", summaries[0].Image)
	}
}

func TestApplyEventFormatsS3RootfsAsImage(t *testing.T) {
	s := NewStore()
	const s3RootfsJSON = `{"instanceID":"inst-abc","tenantID":"default","instanceStatus":{"code":3},` +
		`"scheduleOption":{"extension":{"rootfs":"{\"runtime\":\"runsc\",\"type\":\"s3\",` +
		`\"imageurl\":\"registry.example.com\",\"storageInfo\":{\"endpoint\":\"cn-hangzhou.example.com\",` +
		`\"bucket\":\"crfs-dev\",\"object\":\"rootfs.img\",\"accessKey\":\"secret-ak\",\"secretKey\":\"secret-sk\"}}"}}}`

	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(s3RootfsJSON))

	summaries := s.ListSummaries("default", "")
	if len(summaries) != 1 {
		t.Fatalf("expected one running summary, got %+v", summaries)
	}
	if summaries[0].Image != "s3://crfs-dev/rootfs.img" {
		t.Fatalf("summary image = %q, want s3://crfs-dev/rootfs.img", summaries[0].Image)
	}
	if summaries[0].ImageEndpoint != "cn-hangzhou.example.com" {
		t.Fatalf("summary image endpoint = %q, want cn-hangzhou.example.com", summaries[0].ImageEndpoint)
	}
}

func TestApplyEventFormatsOSSRootfsAsS3Image(t *testing.T) {
	s := NewStore()
	const ossRootfsJSON = `{"instanceID":"inst-abc","tenantID":"default","instanceStatus":{"code":3},` +
		`"scheduleOption":{"extension":{"rootfs":"{\"runtime\":\"runsc\",\"type\":\"s3\",` +
		`\"imageurl\":\"oss-cn-hangzhou.aliyuncs.com\",\"storageInfo\":{` +
		`\"endpoint\":\"https://oss-cn-hangzhou.aliyuncs.com\",\"bucket\":\"yr-rootfs-prod\",` +
		`\"object\":\"/images/python310/rootfs.img\",\"accessKey\":\"secret-ak\",` +
		`\"secretKey\":\"secret-sk\"}}"}}}`

	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(ossRootfsJSON))

	summaries := s.ListSummaries("default", "")
	if len(summaries) != 1 {
		t.Fatalf("expected one running summary, got %+v", summaries)
	}
	if summaries[0].Image != "s3://yr-rootfs-prod/images/python310/rootfs.img" {
		t.Fatalf("summary image = %q, want s3://yr-rootfs-prod/images/python310/rootfs.img", summaries[0].Image)
	}
	if summaries[0].ImageEndpoint != "https://oss-cn-hangzhou.aliyuncs.com" {
		t.Fatalf("summary image endpoint = %q, want https://oss-cn-hangzhou.aliyuncs.com", summaries[0].ImageEndpoint)
	}
}

func TestApplyEventUsesPlainRootfsAsImage(t *testing.T) {
	s := NewStore()
	const plainRootfsJSON = `{"instanceID":"inst-abc","tenantID":"default","instanceStatus":{"code":3},` +
		`"createOptions":{"rootfs":"python:3.12-slim"}}`

	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(plainRootfsJSON))

	summaries := s.ListSummaries("default", "")
	if len(summaries) != 1 {
		t.Fatalf("expected one running summary, got %+v", summaries)
	}
	if summaries[0].Image != "python:3.12-slim" {
		t.Fatalf("summary image = %q, want python:3.12-slim", summaries[0].Image)
	}
}

func TestStoreGetSummary(t *testing.T) {
	s := NewStore()
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(runningJSON))

	summary, ok := s.GetSummary("inst-abc")
	if !ok {
		t.Fatal("GetSummary should find a cached running instance")
	}
	if summary.InstanceID != "inst-abc" {
		t.Fatalf("InstanceID = %q, want inst-abc", summary.InstanceID)
	}
	if summary.TenantID != "default" {
		t.Fatalf("TenantID = %q, want default", summary.TenantID)
	}

	if _, ok := s.GetSummary("missing"); ok {
		t.Fatal("GetSummary should not find a missing instance")
	}
}

func TestApplyEventPutThenDelete(t *testing.T) {
	s := NewStore()
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(runningJSON))
	if _, ok := s.Get("inst-abc"); !ok {
		t.Fatal("should be cached after PUT")
	}
	ApplyInstanceEvent(s, EventDelete, instanceKey, nil)
	if _, ok := s.Get("inst-abc"); ok {
		t.Error("should be gone after DELETE")
	}
	if summaries := s.ListSummaries("default", ""); len(summaries) != 0 {
		t.Errorf("summary should be gone after DELETE, got %+v", summaries)
	}
}

func TestApplyEventNonRunningRemoved(t *testing.T) {
	s := NewStore()
	// Pre-populate, then a non-cacheable update (code 5 = EXITING) must evict it.
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(runningJSON))
	const exitedJSON = `{"instanceID":"inst-abc","proxyGrpcAddress":"10.0.0.1:22774","instanceStatus":{"code":5}}`
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(exitedJSON))
	if _, ok := s.Get("inst-abc"); ok {
		t.Error("non-cacheable instance must not stay cached")
	}
	if summaries := s.ListSummaries("default", ""); len(summaries) != 0 {
		t.Errorf("non-cacheable summary must not stay cached, got %+v", summaries)
	}
}

// TestApplyEventCacheableStates asserts the non-RUNNING cacheable states are
// cached, not just RUNNING.
func TestApplyEventCacheableStates(t *testing.T) {
	cacheable := map[int32]string{
		2: "CREATING", 11: "SUB_HEALTH", 4: "FAILED", 6: "FATAL", 7: "SCHEDULE_FAILED", 1: "SCHEDULING",
	}
	for code, name := range cacheable {
		s := NewStore()
		json := `{"instanceID":"inst-abc","proxyGrpcAddress":"10.0.0.1:22774",` +
			`"instanceStatus":{"code":` + strconv.Itoa(int(code)) + `,"msg":"` + name + `"}}`
		ApplyInstanceEvent(s, EventPut, instanceKey, []byte(json))
		if _, ok := s.Get("inst-abc"); !ok {
			t.Errorf("code %d (%s) should be cached as exec endpoint", code, name)
		}
		summaries := s.ListSummaries("default", "")
		if len(summaries) != 1 {
			t.Fatalf("code %d (%s): expected one summary, got %+v", code, name, summaries)
		}
		if summaries[0].StatusCode != code {
			t.Errorf("code %d (%s): StatusCode = %d", code, name, summaries[0].StatusCode)
		}
		if summaries[0].StatusMsg != name {
			t.Errorf("code %d (%s): StatusMsg = %q", code, name, summaries[0].StatusMsg)
		}
	}
}

func TestApplyEventEmptyProxyStillCachedForList(t *testing.T) {
	s := NewStore()
	const noProxyJSON = `{"instanceID":"inst-abc","tenantID":"default","proxyGrpcAddress":"","instanceStatus":{"code":3}}`
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(noProxyJSON))
	if _, ok := s.Get("inst-abc"); ok {
		t.Error("RUNNING instance with empty proxyGrpcAddress must not be cached as exec endpoint")
	}
	if summaries := s.ListSummaries("default", ""); len(summaries) != 1 {
		t.Errorf("RUNNING instance with empty proxyGrpcAddress should still be cached for list, got %+v", summaries)
	}
}

func TestApplyEventMalformedJSONRemoved(t *testing.T) {
	s := NewStore()
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(runningJSON))
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte("{not json"))
	if _, ok := s.Get("inst-abc"); ok {
		t.Error("malformed PUT must evict by key, not keep stale entry")
	}
}

func TestApplyEventInstanceIDFromKeyFallback(t *testing.T) {
	s := NewStore()
	// Value omits instanceID; it must be recovered from the key's last segment.
	const noIDJSON = `{"proxyGrpcAddress":"10.0.0.1:22774","containerID":"sbox-x","instanceStatus":{"code":3}}`
	ApplyInstanceEvent(s, EventPut, instanceKey, []byte(noIDJSON))
	if _, ok := s.Get("inst-abc"); !ok {
		t.Error("instanceID should fall back to the key's last segment")
	}
}
