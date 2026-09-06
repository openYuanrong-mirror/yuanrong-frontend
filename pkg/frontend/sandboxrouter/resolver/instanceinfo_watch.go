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
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/route"
)

// instanceEtcdKeyLen is the '/'-separated depth of a full instance key
// (/sn/instance/business/yrk/tenant/{t}/function/{f}/version/{v}/defaultaz/{r}/{id}),
// mirroring the filter in frontend's existing instanceinfo watcher.
const instanceEtcdKeyLen = 14

// errNoEtcdClient is returned by Start when the shared router etcd client is
// not initialised.
var errNoEtcdClient = errors.New("sandboxrouter: router etcd client not initialised")

// ErrAuthoritativeInstanceNotFound is returned only when the retained
// instance route no longer exists in ETCD. Callers map it to a genuine 404.
var ErrAuthoritativeInstanceNotFound = errors.New("authoritative instance not found")

const (
	instanceRoutePathPrefix    = "/yr/route/business/yrk"
	defaultReadThroughTimeout  = 500 * time.Millisecond
	defaultFailureRetention    = 10 * time.Minute
	defaultMaxRetainedFailures = 4096
	functionKeyParts           = 3
)

type instanceAuthorityReader interface {
	ReadInstance(ctx context.Context, instanceID string) (key string, value []byte, err error)
}

type revisionedInstanceAuthorityReader interface {
	ReadInstanceWithRevision(context.Context, string) (instanceReadResult, error)
}

type etcdInstanceAuthorityReader struct{}

type instanceReadResult struct {
	key      string
	value    []byte
	revision int64
}

type readThroughResult struct {
	Val interface{}
	Err error
}

type readThroughCall struct {
	done chan struct{}
	val  interface{}
	err  error
}

// readThroughGroup coalesces concurrent authority reads for one safe instance
// ID. It deliberately remains a rebuildable process-local optimisation; ETCD
// is still the logical authority.
type readThroughGroup struct {
	mu    sync.Mutex
	calls map[string]*readThroughCall
}

func (g *readThroughGroup) DoChan(
	key string, operation func() (interface{}, error),
) <-chan readThroughResult {
	result := make(chan readThroughResult, 1)
	g.mu.Lock()
	call, exists := g.calls[key]
	if !exists {
		if g.calls == nil {
			g.calls = make(map[string]*readThroughCall)
		}
		call = &readThroughCall{done: make(chan struct{})}
		g.calls[key] = call
	}
	g.mu.Unlock()

	if !exists {
		go func() {
			call.val, call.err = operation()
			close(call.done)
			g.mu.Lock()
			if g.calls[key] == call {
				delete(g.calls, key)
			}
			g.mu.Unlock()
		}()
	}
	go func() {
		<-call.done
		result <- readThroughResult{Val: call.val, Err: call.err}
	}()
	return result
}

type authoritativeRouteInfo struct {
	InstanceID       string               `json:"instanceID"`
	Function         string               `json:"function"`
	FunctionProxyID  string               `json:"functionProxyID"`
	ProxyGrpcAddress string               `json:"proxyGrpcAddress"`
	RequestID        string               `json:"requestID"`
	TenantID         string               `json:"tenantID"`
	Version          route.JSONInt64      `json:"version"`
	InstanceStatus   route.InstanceStatus `json:"instanceStatus"`
}

func (e etcdInstanceAuthorityReader) ReadInstance(ctx context.Context, requestedID string) (string, []byte, error) {
	return e.readInstance(ctx, requestedID, nil)
}

func (e etcdInstanceAuthorityReader) ReadInstanceWithRevision(
	ctx context.Context, id string) (instanceReadResult, error) {
	var result instanceReadResult
	var err error
	result.key, result.value, err = e.readInstance(ctx, id, &result.revision)
	return result, err
}

func (etcdInstanceAuthorityReader) readInstance(
	ctx context.Context, requestedID string, revision *int64) (string, []byte, error) {
	client := etcd3.GetRouterEtcdClient()
	if client == nil {
		return "", nil, errNoEtcdClient
	}
	routeKey := instanceRoutePathPrefix + "/" + requestedID
	routeResponse, err := client.GetResponse(
		etcd3.CreateEtcdCtxInfoWithTimeout(ctx, etcd3.DurationContextTimeout),
		routeKey, clientv3.WithSerializable())
	if err != nil {
		return "", nil, fmt.Errorf("read authoritative route %s: %w", requestedID, err)
	}
	if revision != nil && routeResponse != nil && routeResponse.Header != nil {
		*revision = routeResponse.Header.Revision
	}
	if routeResponse == nil || len(routeResponse.Kvs) == 0 {
		return "", nil, ErrAuthoritativeInstanceNotFound
	}
	var routeInfo authoritativeRouteInfo
	if err := json.Unmarshal(routeResponse.Kvs[0].Value, &routeInfo); err != nil {
		return "", nil, fmt.Errorf("decode authoritative route %s: %w", requestedID, err)
	}
	if routeInfo.InstanceID == "" || route.SanitizeID(routeInfo.InstanceID) != requestedID {
		return "", nil, fmt.Errorf("authoritative route identity mismatch for %s", requestedID)
	}
	// PAUSED RouteInfo is itself sufficient to reject the data plane and clear
	// stale caches. It deliberately has no runtime physical identity.
	if routeInfo.InstanceStatus.Code == execendpoint.StatusPaused {
		return routeKey, append([]byte(nil), routeResponse.Kvs[0].Value...), nil
	}
	parts := strings.Split(routeInfo.Function, "/")
	if len(parts) != functionKeyParts || routeInfo.RequestID == "" {
		return "", nil, fmt.Errorf("authoritative route %s cannot identify InstanceInfo", requestedID)
	}
	instanceKey := constant.InstancePathPrefix + "/business/yrk/tenant/" + parts[0] +
		"/function/" + parts[1] + "/version/" + parts[2] + "/defaultaz/" +
		routeInfo.RequestID + "/" + routeInfo.InstanceID
	opts := []clientv3.OpOption{clientv3.WithSerializable()}
	if routeResponse.Header != nil && routeResponse.Header.Revision > 0 {
		opts = append(opts, clientv3.WithRev(routeResponse.Header.Revision))
	}
	instanceResponse, err := client.GetResponse(
		etcd3.CreateEtcdCtxInfoWithTimeout(ctx, etcd3.DurationContextTimeout), instanceKey, opts...)
	if err != nil {
		return "", nil, fmt.Errorf("read authoritative instance %s: %w", requestedID, err)
	}
	if instanceResponse == nil || len(instanceResponse.Kvs) == 0 {
		return "", nil, fmt.Errorf("authoritative route %s has no matching InstanceInfo", requestedID)
	}
	value := append([]byte(nil), instanceResponse.Kvs[0].Value...)
	var instance route.InstanceInfo
	if err := json.Unmarshal(value, &instance); err != nil {
		return "", nil, fmt.Errorf("decode authoritative instance %s: %w", requestedID, err)
	}
	if route.SanitizeID(instance.InstanceID) != requestedID || instance.RequestID != routeInfo.RequestID ||
		instance.Version != routeInfo.Version || instance.FunctionProxyID != routeInfo.FunctionProxyID ||
		instance.ProxyGrpcAddress != routeInfo.ProxyGrpcAddress ||
		instance.InstanceStatus.Code != routeInfo.InstanceStatus.Code {
		return "", nil, fmt.Errorf("instance info and route info disagree for %s", requestedID)
	}
	if instance.InstanceStatus.Code == execendpoint.StatusRunning &&
		(instance.FunctionProxyID == "" || instance.FunctionProxyID == execendpoint.InstanceManagerOwner ||
			route.ExtractIP(instance.ProxyGrpcAddress) == "") {
		return "", nil, fmt.Errorf("authoritative RUNNING route is incomplete for %s", requestedID)
	}
	return instanceKey, value, nil
}

// ReadAuthoritativeInstance returns the retained logical instance view from
// ETCD without publishing it into any frontend cache. Lifecycle authorization
// uses this bounded read when its rebuildable summary cache has not converged.
func ReadAuthoritativeInstance(ctx context.Context, instanceID string) (*route.InstanceInfo, error) {
	safeID := route.SanitizeID(strings.TrimSpace(instanceID))
	if safeID == "" {
		return nil, errors.New("sandboxrouter: instance ID is required")
	}
	readCtx, cancel := context.WithTimeout(ctx, defaultReadThroughTimeout)
	defer cancel()
	_, value, err := (etcdInstanceAuthorityReader{}).ReadInstance(readCtx, safeID)
	if err != nil {
		return nil, err
	}
	var info route.InstanceInfo
	if err := json.Unmarshal(value, &info); err != nil {
		return nil, fmt.Errorf("decode authoritative instance %s: %w", safeID, err)
	}
	if info.InstanceID == "" || route.SanitizeID(info.InstanceID) != safeID || info.TenantID == "" {
		return nil, fmt.Errorf("authoritative instance identity is incomplete for %s", safeID)
	}
	return &info, nil
}

// InstanceInfoWatchResolver maintains a route cache by watching /sn/instance
// and replicating the FunctionMaster route computation. It reuses frontend's
// existing etcd watch infrastructure and is fully decoupled from the Traefik
// etcd-KV registry / HTTP provider.
type InstanceInfoWatchResolver struct {
	cache        *route.Cache
	reader       instanceAuthorityReader
	readTimeout  time.Duration
	reads        readThroughGroup
	mu           sync.RWMutex
	observations map[string]map[string]*instanceObservation
	now          func() time.Time
	retention    time.Duration
	maxRetained  int
	nextPrune    time.Time
}

// NewInstanceInfoWatchResolver returns a resolver with an empty cache.
func NewInstanceInfoWatchResolver() *InstanceInfoWatchResolver {
	return NewInstanceInfoWatchResolverWithTimeout(defaultReadThroughTimeout)
}

// NewInstanceInfoWatchResolverWithTimeout returns a resolver whose ETCD
// read-through operations are bounded by timeout. Non-positive values retain
// the safe default.
func NewInstanceInfoWatchResolverWithTimeout(timeout time.Duration) *InstanceInfoWatchResolver {
	if timeout <= 0 {
		timeout = defaultReadThroughTimeout
	}
	resolver := newInstanceInfoWatchResolverWithReader(etcdInstanceAuthorityReader{})
	resolver.readTimeout = timeout
	return resolver
}

func newInstanceInfoWatchResolverWithReader(reader instanceAuthorityReader) *InstanceInfoWatchResolver {
	return &InstanceInfoWatchResolver{
		cache: route.NewCache(), reader: reader, readTimeout: defaultReadThroughTimeout,
		observations: make(map[string]map[string]*instanceObservation),
		now:          time.Now, retention: defaultFailureRetention, maxRetained: defaultMaxRetainedFailures,
	}
}

// Start lists existing instances and watches for changes until stopCh closes.
// The watcher emits a PUT per existing key on startup, so the initial cache is
// populated without a separate full-list step.
func (r *InstanceInfoWatchResolver) Start(stopCh <-chan struct{}) error {
	client := etcd3.GetRouterEtcdClient()
	if client == nil {
		return errNoEtcdClient
	}
	watcher := etcd3.NewEtcdWatcher(constant.InstancePathPrefix, sandboxInstanceFilter, r.applyEvent, stopCh, client)
	watcher.StartWatch()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				r.mu.Lock()
				r.pruneLocked()
				r.mu.Unlock()
			}
		}
	}()
	log.GetLogger().Infof("sandboxrouter: watching instance info under %s", constant.InstancePathPrefix)
	return nil
}

// Resolve uses the watch-fed cache as the fast path. A miss, including a
// locally cached PAUSED state, performs one bounded authoritative ETCD
// read-through. Concurrent misses for the same instance are coalesced.
func (r *InstanceInfoWatchResolver) Resolve(ctx context.Context, key route.Key) (*route.Target, error) {
	if target, err := r.cachedTarget(key); target != nil || err != nil {
		return target, err
	}
	if r.reader == nil {
		return nil, fmt.Errorf("sandboxrouter authority reader is unavailable")
	}
	readCtx, cancel := context.WithTimeout(ctx, r.readTimeout)
	defer cancel()
	result := r.reads.DoChan(key.SafeInstanceID, func() (interface{}, error) {
		if target, err := r.cachedTarget(key); target != nil || err != nil {
			return nil, err
		}
		return r.refreshInstance(readCtx, key.SafeInstanceID)
	})
	select {
	case <-readCtx.Done():
		return nil, fmt.Errorf("authoritative route read timed out: %w", readCtx.Err())
	case outcome := <-result:
		if outcome.Err != nil {
			return nil, outcome.Err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked()
	return r.resultLocked(key)
}

func (r *InstanceInfoWatchResolver) refreshInstance(
	ctx context.Context, safeID string,
) (*route.InstanceInfo, error) {
	r.mu.Lock()
	before := r.observations[safeID]
	// Copy the observation pointers: every update replaces its pointer. A
	// concurrent watch update must win over this potentially older etcd read.
	snapshot := copyObservations(before)
	r.mu.Unlock()
	var key string
	var value []byte
	var revision int64
	var err error
	if reader, ok := r.reader.(revisionedInstanceAuthorityReader); ok {
		result, readErr := reader.ReadInstanceWithRevision(ctx, safeID)
		if readErr != nil {
			if !errors.Is(readErr, ErrAuthoritativeInstanceNotFound) {
				return nil, fmt.Errorf("authoritative route read failed: %w", readErr)
			}
		}
		key, value, revision = result.key, result.value, result.revision
		err = readErr
	} else {
		key, value, err = r.reader.ReadInstance(ctx, safeID)
		if err != nil {
			if !errors.Is(err, ErrAuthoritativeInstanceNotFound) {
				return nil, fmt.Errorf("authoritative route read failed: %w", err)
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !sameObservations(snapshot, r.observations[safeID]) {
		return nil, nil
	}
	if errors.Is(err, ErrAuthoritativeInstanceNotFound) {
		for _, observed := range r.observations[safeID] {
			r.deleteLocked(observed.key, nil, revision)
		}
		// Clear an unobserved legacy cache entry, but do not remove a newer
		// RUNNING route when deleteLocked rejected an older etcd snapshot.
		if len(r.observations[safeID]) == 0 {
			r.cache.DeleteInstance(safeID)
			execendpoint.Default().Delete(safeID)
		}
		return nil, nil
	}
	var info route.InstanceInfo
	if err := json.Unmarshal(value, &info); err != nil {
		return nil, fmt.Errorf("decode read-through InstanceInfo: %w", err)
	}
	if route.SanitizeID(info.InstanceID) != safeID {
		return nil, fmt.Errorf("authoritative instance identity mismatch for %s", safeID)
	}
	r.putLocked(key, value, revision)
	return &info, nil
}

// applyEvent maps an etcd watch event onto the route cache. PUT/DELETE are
// translated to the etcd-free route.ApplyInstanceEvent; SYNCED/ERROR are ignored.
// The same event also feeds the exec endpoint cache (execendpoint), so the web
// terminal / file-copy exec path can resolve an instance's proxyGrpcAddress
// locally instead of querying the master, reusing this one watch.
func (r *InstanceInfoWatchResolver) applyEvent(event *etcd3.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked()
	switch event.Type {
	case etcd3.PUT:
		r.putLocked(event.Key, event.Value, event.Rev)
	case etcd3.DELETE:
		r.deleteLocked(event.Key, event.PrevValue, event.Rev)
	}
}

func (r *InstanceInfoWatchResolver) applyPut(key string, value []byte) {
	r.applyEvent(&etcd3.Event{Type: etcd3.PUT, Key: key, Value: value})
}

// sandboxInstanceFilter returns true to SKIP an event. It keeps only full
// instance keys (depth instanceEtcdKeyLen); sub-keys and other records are
// skipped. Non-sandbox instances pass through but yield no routes.
func sandboxInstanceFilter(event *etcd3.Event) bool {
	return len(strings.Split(event.Key, constant.ETCDEventKeySeparator)) != instanceEtcdKeyLen
}
