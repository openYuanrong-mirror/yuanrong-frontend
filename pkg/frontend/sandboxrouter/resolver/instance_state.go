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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/route"
)

// Each record owns a raw instance identity. The outer index uses the URL's
// sanitized ID; ambiguous sanitized identities never expose a lifecycle result.
// Deleted records also fence late watch events during the retention window.
type instanceObservation struct {
	info      route.InstanceInfo
	key       string
	revision  int64
	deletedAt time.Time
}

func copyObservations(src map[string]*instanceObservation) map[string]*instanceObservation {
	dst := make(map[string]*instanceObservation, len(src))
	for id, v := range src {
		dst[id] = v
	}
	return dst
}

func sameObservations(a, b map[string]*instanceObservation) bool {
	if len(a) != len(b) {
		return false
	}
	for id, v := range a {
		if b[id] != v {
			return false
		}
	}
	return true
}

func rawInstanceID(key string) string {
	return key[strings.LastIndexByte(key, '/')+1:]
}

func (r *InstanceInfoWatchResolver) observationLocked(safeID string) (*instanceObservation, error) {
	var found *instanceObservation
	for _, observed := range r.observations[safeID] {
		if !observed.deletedAt.IsZero() && !r.now().Before(observed.deletedAt.Add(r.retention)) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("ambiguous sandbox route identity")
		}
		found = observed
	}
	return found, nil
}

func (r *InstanceInfoWatchResolver) cachedTarget(key route.Key) (*route.Target, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	observed, err := r.observationLocked(key.SafeInstanceID)
	if err != nil {
		return nil, err
	}
	if observed != nil && (observed.info.InstanceStatus.Code != execendpoint.StatusRunning || !observed.deletedAt.IsZero()) {
		return nil, nil // Refresh a non-running observation before rejecting a request.
	}
	target, err := r.cache.Get(key)
	if err == route.ErrRouteNotFound {
		return nil, nil
	}
	return target, err
}

func (r *InstanceInfoWatchResolver) resultLocked(key route.Key) (*route.Target, error) {
	observed, err := r.observationLocked(key.SafeInstanceID)
	if err != nil {
		return nil, err
	}
	if observed != nil {
		if failure := route.FailureFor(observed.info); failure != nil {
			return nil, failure
		}
		if !observed.deletedAt.IsZero() {
			return nil, route.ErrRouteNotFound
		}
		if observed.info.InstanceStatus.Code == execendpoint.StatusPaused {
			return nil, route.ErrInstancePaused
		}
	}
	return r.cache.Get(key)
}

// FailureForRuntime is a read-only transport-error fallback. An exit from a
// replacement runtime cannot be used to explain the failed upstream request.
func (r *InstanceInfoWatchResolver) FailureForRuntime(key route.Key, instanceID, runtimeID string) *route.InstanceFailure {
	if runtimeID == "" || instanceID == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	observed, err := r.observationLocked(key.SafeInstanceID)
	if err != nil || observed == nil || observed.info.InstanceID != instanceID || observed.info.RuntimeID != runtimeID {
		return nil
	}
	return route.FailureFor(observed.info)
}

func (r *InstanceInfoWatchResolver) putLocked(key string, value []byte, revision int64) {
	var info route.InstanceInfo
	if err := json.Unmarshal(value, &info); err != nil {
		r.deleteLocked(key, nil, revision)
		return
	}
	if info.InstanceID == "" {
		info.InstanceID = rawInstanceID(key)
	}
	id, safeID := info.InstanceID, route.SanitizeID(info.InstanceID)
	entries := r.observations[safeID]
	if entries == nil {
		entries = make(map[string]*instanceObservation)
		r.observations[safeID] = entries
	}
	old := entries[id]
	if old != nil {
		if revision != 0 && (revision < old.revision ||
			(revision == old.revision && (old.key == key || old.deletedAt.IsZero()))) {
			return
		}
		// Within one instance key, version only increases. The authoritative reader
		// may return an older serializable snapshot even without a concurrent watch.
		if old.key == key && info.Version < old.info.Version {
			return
		}
	}
	record := &instanceObservation{info: info, key: key, revision: revision}
	if old != nil && old.key == key && revision == 0 {
		record.revision = old.revision
	}
	entries[id] = record
	execendpoint.ApplyInstanceEvent(execendpoint.Default(), execendpoint.EventPut, key, value)
	route.ApplyInstanceEvent(r.cache, route.EventPut, key, value)
}

func (r *InstanceInfoWatchResolver) deleteLocked(key string, previous []byte, revision int64) {
	id := rawInstanceID(key)
	safeID := route.SanitizeID(id)
	entries := r.observations[safeID]
	var old *instanceObservation
	if entries != nil {
		old = entries[id]
	}
	if old != nil && (old.key != key || (revision != 0 && revision <= old.revision)) {
		return
	}
	if old == nil {
		var info route.InstanceInfo
		_ = json.Unmarshal(previous, &info)
		// PrevValue is only trusted when it belongs to this exact key identity.
		if info.InstanceID != id {
			info = route.InstanceInfo{InstanceID: id}
		}
		old = &instanceObservation{info: info, key: key}
		if entries == nil {
			entries = make(map[string]*instanceObservation)
			r.observations[safeID] = entries
		}
	}
	firstDelete := old.deletedAt.IsZero()
	record := *old
	if revision > record.revision {
		record.revision = revision
	}
	if record.deletedAt.IsZero() {
		record.deletedAt = r.now()
	}
	entries[id] = &record
	r.cache.DeleteInstance(id)
	execendpoint.Default().DeleteEndpoint(id)
	if route.FailureFor(record.info) == nil {
		execendpoint.Default().Delete(id)
	}
	if firstDelete {
		r.enforceRetentionLimitLocked()
	}
}

func (r *InstanceInfoWatchResolver) removeObservationLocked(safeID, id string) {
	delete(r.observations[safeID], id)
	if len(r.observations[safeID]) == 0 {
		delete(r.observations, safeID)
	}
	r.cache.DeleteInstance(id)
	execendpoint.Default().Delete(id)
}

func (r *InstanceInfoWatchResolver) pruneLocked() {
	now := r.now()
	if now.Before(r.nextPrune) {
		return
	}
	r.nextPrune = now.Add(time.Minute)
	for safeID, entries := range r.observations {
		for id, observed := range entries {
			if !observed.deletedAt.IsZero() && !now.Before(observed.deletedAt.Add(r.retention)) {
				r.removeObservationLocked(safeID, id)
			}
		}
	}
}

func (r *InstanceInfoWatchResolver) enforceRetentionLimitLocked() {
	var retained []*instanceObservation
	for _, entries := range r.observations {
		for _, v := range entries {
			if !v.deletedAt.IsZero() {
				retained = append(retained, v)
			}
		}
	}
	if len(retained) <= r.maxRetained {
		return
	}
	sort.Slice(retained, func(i, j int) bool { return retained[i].deletedAt.Before(retained[j].deletedAt) })
	for _, v := range retained[:len(retained)-r.maxRetained] {
		r.removeObservationLocked(route.SanitizeID(v.info.InstanceID), v.info.InstanceID)
	}
}
