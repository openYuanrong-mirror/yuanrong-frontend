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

// Package execendpoint maintains an in-memory instanceID -> exec endpoint cache
// fed from the same /sn/instance watch the sandbox router already runs. It lets
// the web terminal / file-copy exec path resolve an instance's backend
// (proxyGrpcAddress + containerID) locally instead of issuing a full
// query-tenant-instances HTTP call to the master on every exec.
//
// Unlike route.Cache (keyed by {SafeInstanceID, port} and limited to
// instances that publish a portForward extension), this cache keys on the raw
// instanceID and holds every RUNNING instance, because exec only needs the
// proxy gRPC endpoint and does not depend on port forwarding.
package execendpoint

import (
	"sort"
	"sync"
	"time"
)

const (
	// StatusRunning mirrors FunctionSystem's RUNNING InstanceState.
	StatusRunning int32 = 3
	// StatusFailed mirrors FunctionSystem's FAILED InstanceState.
	StatusFailed int32 = 4
	// StatusFatal mirrors FunctionSystem's FATAL InstanceState.
	StatusFatal int32 = 6
)

// Endpoint is the minimal backend coordinate the exec path needs for one
// instance: where to dial (ProxyGrpcAddress) and which container to exec into
// (ContainerID).
type Endpoint struct {
	InstanceID       string
	ProxyGrpcAddress string
	ContainerID      string
}

// Resource is the minimal scalar resource shape carried by InstanceInfo JSON.
type Resource struct {
	Scalar struct {
		Value float64 `json:"value"`
		Limit float64 `json:"limit"`
	} `json:"scalar"`
}

// Summary is the minimal RUNNING instance view needed by frontend list APIs.
type Summary struct {
	InstanceID     string
	TenantID       string
	NodeID         string
	Function       string
	Image          string
	ImageEndpoint  string
	StatusCode     int32
	StatusMsg      string
	StatusType     int32
	StatusExitCode int32
	StatusErrCode  int32
	StartTime      string
	// ObservedRunningAt is the frontend-local fallback start timestamp. It is
	// set when the watcher first observes this instance in RUNNING state and is
	// used only when the service-side startTime is missing or unparsable.
	ObservedRunningAt time.Time
	Resources         map[string]Resource
	// ContainerID is InstanceInfo.containerID (docker containerID or supervisor/runsc sandboxID).
	ContainerID string
	// ContainerIP is the sandbox internal IP (docker inspect or supervisor create response).
	ContainerIP string
	// SandboxType is the executor kind, from createOptions["sandbox_type"].
	SandboxType string
	// CreateOptions is a copy of the instance createOptions map, for the agent Get handler to parse.
	CreateOptions map[string]string
}

// Store is a concurrency-safe instanceID -> Endpoint map. The zero value is not
// usable; obtain one via NewStore or the package-level Default singleton.
type Store struct {
	mu        sync.RWMutex
	m         map[string]Endpoint
	summaries map[string]Summary
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		m:         make(map[string]Endpoint),
		summaries: make(map[string]Summary),
	}
}

// Get returns the endpoint for instanceID and whether it was present.
func (s *Store) Get(instanceID string) (Endpoint, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.m[instanceID]
	return e, ok
}

// Put inserts or replaces the endpoint for ep.InstanceID. A Put with an empty
// InstanceID is ignored (there would be no key to look it up by).
func (s *Store) Put(ep Endpoint) {
	if ep.InstanceID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[ep.InstanceID] = ep
}

// PutSummary inserts or replaces the list summary for summary.InstanceID.
func (s *Store) PutSummary(summary Summary) {
	if summary.InstanceID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if summary.ObservedRunningAt.IsZero() {
		if existing, ok := s.summaries[summary.InstanceID]; ok && !existing.ObservedRunningAt.IsZero() {
			summary.ObservedRunningAt = existing.ObservedRunningAt
		} else {
			summary.ObservedRunningAt = time.Now()
		}
	}
	s.summaries[summary.InstanceID] = summary
}

// GetSummary returns the cached RUNNING instance summary for instanceID.
func (s *Store) GetSummary(instanceID string) (Summary, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	summary, ok := s.summaries[instanceID]
	return summary, ok
}

// Delete removes the endpoint for instanceID (no-op if absent).
func (s *Store) Delete(instanceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, instanceID)
	delete(s.summaries, instanceID)
}

// DeleteEndpoint removes only the exec endpoint, keeping the list summary intact.
func (s *Store) DeleteEndpoint(instanceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, instanceID)
}

// Size returns the number of cached endpoints.
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// ListSummaries returns RUNNING instance summaries matching the optional tenant and instance filters.
func (s *Store) ListSummaries(tenantID, instanceID string) []Summary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Summary, 0, len(s.summaries))
	for _, summary := range s.summaries {
		if tenantID != "" && summary.TenantID != tenantID {
			continue
		}
		if instanceID != "" && summary.InstanceID != instanceID {
			continue
		}
		out = append(out, summary)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstanceID < out[j].InstanceID
	})
	return out
}

// defaultStore is the process-wide exec endpoint cache, fed by the sandbox
// router's instance-info watch and read by the web terminal exec path.
var defaultStore = NewStore()

// Default returns the package-level Store singleton.
func Default() *Store {
	return defaultStore
}
