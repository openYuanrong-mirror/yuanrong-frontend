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
	"encoding/json"
	"strings"

	"frontend/pkg/frontend/sandboxrouter/rootfs"
)

// cacheableInstanceStates are the yr InstanceState codes (instance_state.h) this
// package caches. "Instance gone" states (NEW/EXITING/EXITED/EVICTING/EVICTED/
// SUSPEND) are not cached: they remove the summary so GET returns 404. PAUSED is
// cacheable too, but it never carries a routable exec endpoint (it has no live
// runtime), so it retains its summary yet drops the exec endpoint below. Kept
// local so this package stays dependency-free (stdlib only) and unit-testable,
// mirroring route/apply.go.
var cacheableInstanceStates = map[int32]struct{}{
	1:  {}, // SCHEDULING
	2:  {}, // CREATING
	3:  {}, // RUNNING
	4:  {}, // FAILED
	6:  {}, // FATAL
	7:  {}, // SCHEDULE_FAILED
	11: {}, // SUB_HEALTH
	13: {}, // PAUSED
}

// EventKind is the kind of instance-info change, mirroring the etcd watch event
// types while keeping this package free of the etcd client dependency.
type EventKind int

const (
	// EventPut is an instance-info create/update.
	EventPut EventKind = iota
	// EventDelete is an instance-info removal.
	EventDelete
)

// instanceExecInfo is the minimal view of the /sn/instance JSON the exec path
// needs. The full kernel InstanceInfo proto (serialized via MessageToJsonString)
// carries far more; we only read these fields, matching the approach in
// route/instanceinfo.go. proxyGrpcAddress and containerID are NOT present in
// frontend's existing InstanceSpecification struct, which is why we parse our own.
type instanceExecInfo struct {
	InstanceID       string            `json:"instanceID"`
	TenantID         string            `json:"tenantID"`
	FunctionProxyID  string            `json:"functionProxyID"`
	ProxyGrpcAddress string            `json:"proxyGrpcAddress"`
	ContainerID      string            `json:"containerID"`
	ContainerIP      string            `json:"containerIP"`
	Function         string            `json:"function"`
	StartTime        string            `json:"startTime"`
	CreateOptions    map[string]string `json:"createOptions"`
	ScheduleOption   struct {
		Extension map[string]string `json:"extension"`
	} `json:"scheduleOption"`
	Resources struct {
		Resources map[string]Resource `json:"resources"`
	} `json:"resources"`
	InstanceStatus struct {
		Code     int32  `json:"code"`
		ExitCode int32  `json:"exitCode"`
		Msg      string `json:"msg"`
		Type     int32  `json:"type"`
		ErrCode  int32  `json:"errCode"`
	} `json:"instanceStatus"`
}

// ApplyInstanceEvent updates the cache for one /sn/instance watch event:
//   - DELETE removes the instance's endpoint and summary (instanceID recovered from the key).
//   - PUT of a cacheable state adds/replaces its summary. If it also has a
//     non-empty proxyGrpcAddress, it adds/replaces its exec endpoint. PAUSED is
//     cacheable but never carries a routable endpoint, so it keeps its summary
//     while its exec endpoint is dropped.
//   - PUT of a non-cacheable state or unparseable value removes cached data.
//
// It never panics on bad input; malformed data results in the instance having
// no cached data.
func ApplyInstanceEvent(s *Store, kind EventKind, key string, value []byte) {
	if kind == EventDelete {
		id := instanceIDFromKey(key)
		// FATAL/FAILED: keep the summary so GET returns the failure state,
		// only drop the exec endpoint to avoid routing to a dead instance.
		if summary, ok := s.GetSummary(id); ok {
			if summary.StatusCode == StatusFatal || summary.StatusCode == StatusFailed {
				s.DeleteEndpoint(id)
				return
			}
		}
		s.Delete(id)
		return
	}

	var info instanceExecInfo
	if err := json.Unmarshal(value, &info); err != nil {
		// Can't identify the instance reliably; remove by key to be safe.
		s.Delete(instanceIDFromKey(key))
		return
	}
	id := info.InstanceID
	if id == "" {
		id = instanceIDFromKey(key)
	}

	if _, ok := cacheableInstanceStates[info.InstanceStatus.Code]; !ok {
		s.Delete(id)
		return
	}

	tenantID := info.TenantID
	if tenantID == "" {
		tenantID = tenantIDFromKey(key)
	}
	image := rootfsDisplay(info.ScheduleOption.Extension, info.CreateOptions)
	s.PutSummary(Summary{
		InstanceID:     id,
		TenantID:       tenantID,
		NodeID:         info.FunctionProxyID,
		Function:       info.Function,
		Image:          image.Image,
		ImageEndpoint:  image.Endpoint,
		StatusCode:     info.InstanceStatus.Code,
		StatusMsg:      info.InstanceStatus.Msg,
		StatusType:     info.InstanceStatus.Type,
		StatusExitCode: info.InstanceStatus.ExitCode,
		StatusErrCode:  info.InstanceStatus.ErrCode,
		StartTime:      info.StartTime,
		Resources:      info.Resources.Resources,
		ContainerID:    info.ContainerID,
		ContainerIP:    info.ContainerIP,
		SandboxType:    info.CreateOptions["sandbox_type"],
		CreateOptions:  copyStringMap(info.CreateOptions),
	})

	if info.ProxyGrpcAddress == "" {
		s.DeleteEndpoint(id)
		return
	}

	s.Put(Endpoint{
		InstanceID:       id,
		ProxyGrpcAddress: info.ProxyGrpcAddress,
		ContainerID:      info.ContainerID,
	})
}

func rootfsDisplay(optionMaps ...map[string]string) rootfs.Display {
	for _, options := range optionMaps {
		display := rootfs.DisplayInfo(options["rootfs"])
		if display.Image != "" || display.Endpoint != "" {
			return display
		}
	}
	return rootfs.Display{}
}

// instanceIDFromKey returns the last '/'-separated segment of an instance key,
// which is the raw instanceID. A key with no '/' is returned unchanged. Mirrors
// route/apply.go:instanceIDFromKey.
func instanceIDFromKey(key string) string {
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return key
}

func tenantIDFromKey(key string) string {
	parts := strings.Split(key, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "tenant" {
			return parts[i+1]
		}
	}
	return ""
}

// copyStringMap returns a shallow copy of m, or nil when empty/nil.
func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
