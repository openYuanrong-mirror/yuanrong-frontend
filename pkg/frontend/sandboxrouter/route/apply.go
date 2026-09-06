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

package route

import (
	"encoding/json"
	"strings"
)

// kernelStatusRunning is constant.KernelInstanceStatusRunning (3). Only RUNNING
// instances contribute routes; any other state removes them. Kept as a local
// const so this package stays dependency-free (stdlib only) and unit-testable.
const kernelStatusRunning int32 = 3

// EventKind is the kind of instance-info change. It mirrors the etcd watch
// event types but keeps this package free of the etcd client dependency so the
// route-update logic stays unit-testable on its own.
type EventKind int

const (
	// EventPut is an instance-info create/update.
	EventPut EventKind = iota
	// EventDelete is an instance-info removal.
	EventDelete
)

// ApplyInstanceEvent updates the cache for one /sn/instance watch event:
//   - DELETE removes the instance's routes (instanceID recovered from the key).
//   - PUT of a RUNNING instance with port forwardings adds/replaces its routes;
//     any other state, missing portForward, or unparseable value removes them.
//
// It never panics on bad input; malformed data results in the instance having
// no routes; the resolver chooses a lifecycle rejection or a missing-route result.
func ApplyInstanceEvent(c *Cache, kind EventKind, key string, value []byte) {
	if kind == EventDelete {
		c.DeleteInstance(instanceIDFromKey(key))
		return
	}

	var info InstanceInfo
	if err := json.Unmarshal(value, &info); err != nil {
		// Can't identify the instance reliably; remove by key to be safe.
		c.DeleteInstance(instanceIDFromKey(key))
		return
	}
	id := info.InstanceID
	if id == "" {
		id = instanceIDFromKey(key)
	}

	if info.InstanceStatus.Code != kernelStatusRunning {
		c.DeleteInstance(id)
		return
	}

	targets, err := ComputeRoutes(&info)
	if err != nil || len(targets) == 0 {
		c.DeleteInstance(id)
		return
	}
	// tenantID identifies the runtime instance owner. The tenant embedded in the
	// key belongs to the function and differs for shared system functions, so it
	// must never be used as an authorization fallback.
	if info.TenantID == "" {
		c.DeleteInstance(id)
		return
	}
	for _, t := range targets {
		t.Tenant = info.TenantID
	}
	c.PutInstance(id, targets)
}

// instanceIDFromKey returns the last '/'-separated segment of an instance key,
// which is the raw instanceID (see GenInstanceKey key format). A key with no
// '/' is returned unchanged.
func instanceIDFromKey(key string) string {
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return key
}
