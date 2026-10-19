/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

package proxyrouting

import (
	"context"
	"fmt"
	"sync"
	"time"

	"frontend/pkg/common/faas_common/etcd3"
)

const defaultOwnershipResolverName = "etcd"

// OwnershipResolver queries the owning node IP for a given user/workspace owner.
// This interface abstracts the ownership storage backend, allowing future
// replacement of etcd with other storage systems (e.g., Ceph, local API).
type OwnershipResolver interface {
	// Resolve returns the owning node IP for the given owner.
	// Returns ("", nil) if no ownership record exists.
	Resolve(ctx context.Context, owner string) (owningIP string, err error)
}

// EtcdOwnershipResolver is the default implementation using etcd.
type EtcdOwnershipResolver struct {
	client    *etcd3.EtcdClient
	keyPrefix string
	timeout   time.Duration
}

// NewEtcdOwnershipResolver creates an EtcdOwnershipResolver using the process-wide
// meta etcd client and the given key prefix.
func NewEtcdOwnershipResolver(client *etcd3.EtcdClient, keyPrefix string) *EtcdOwnershipResolver {
	return &EtcdOwnershipResolver{
		client:    client,
		keyPrefix: keyPrefix,
		timeout:   5 * time.Second,
	}
}

// WithTimeout sets the etcd query timeout for this resolver.
func (r *EtcdOwnershipResolver) WithTimeout(d time.Duration) *EtcdOwnershipResolver {
	r.timeout = d
	return r
}

// Resolve queries etcd for keyPrefix + "/" + owner and returns the owning node IP.
func (r *EtcdOwnershipResolver) Resolve(ctx context.Context, owner string) (string, error) {
	if r == nil {
		return "", fmt.Errorf("etcd ownership resolver is nil")
	}
	// Always fetch the latest meta etcd client to avoid stale connections
	client := etcd3.GetMetaEtcdClient()
	if client == nil {
		return "", fmt.Errorf("meta etcd client is not initialized")
	}
	ctxInfo := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, r.timeout)
	key := r.keyPrefix + "/" + owner
	resp, err := client.Get(ctxInfo, key)
	if err != nil {
		return "", fmt.Errorf("failed to read workspace ownership for owner %q: %w", owner, err)
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// --- Registry ---

var (
	ownershipResolverMu       sync.RWMutex
	ownershipResolverRegistry = make(map[string]OwnershipResolver)
	globalOwnershipResolver   OwnershipResolver
)

// RegisterOwnershipResolver registers an OwnershipResolver under the given name.
func RegisterOwnershipResolver(name string, resolver OwnershipResolver) {
	ownershipResolverMu.Lock()
	defer ownershipResolverMu.Unlock()
	ownershipResolverRegistry[name] = resolver
}

// GetOwnershipResolver returns the resolver registered under name.
func GetOwnershipResolver(name string) (OwnershipResolver, bool) {
	ownershipResolverMu.RLock()
	defer ownershipResolverMu.RUnlock()
	resolver, ok := ownershipResolverRegistry[name]
	return resolver, ok
}

// SetGlobalOwnershipResolver sets the process-wide OwnershipResolver.
func SetGlobalOwnershipResolver(resolver OwnershipResolver) {
	ownershipResolverMu.Lock()
	defer ownershipResolverMu.Unlock()
	globalOwnershipResolver = resolver
}

// GetGlobalOwnershipResolver returns the process-wide OwnershipResolver.
func GetGlobalOwnershipResolver() OwnershipResolver {
	ownershipResolverMu.RLock()
	defer ownershipResolverMu.RUnlock()
	return globalOwnershipResolver
}

// SetOwnershipResolverForTest replaces the global resolver and returns a
// restore closure.
func SetOwnershipResolverForTest(resolver OwnershipResolver) func() {
	ownershipResolverMu.Lock()
	previous := globalOwnershipResolver
	globalOwnershipResolver = resolver
	ownershipResolverMu.Unlock()
	return func() {
		ownershipResolverMu.Lock()
		globalOwnershipResolver = previous
		ownershipResolverMu.Unlock()
	}
}

// InitGlobalOwnershipResolver configures the process-wide OwnershipResolver from
// the resolver name. When name is empty or "etcd", it registers and activates an
// EtcdOwnershipResolver backed by the meta etcd client.
func InitGlobalOwnershipResolver(name string, etcdClient *etcd3.EtcdClient) {
	if name == "" {
		name = defaultOwnershipResolverName
	}
	resolver, ok := GetOwnershipResolver(name)
	if !ok {
		resolver = NewEtcdOwnershipResolver(etcdClient, defaultWorkspaceOwnershipKeyPrefix)
		RegisterOwnershipResolver(name, resolver)
	}
	if etcdRes, ok := resolver.(*EtcdOwnershipResolver); ok && etcdClient != nil {
		etcdRes.client = etcdClient
	}
	SetGlobalOwnershipResolver(resolver)
}

const defaultWorkspaceOwnershipKeyPrefix = "/agent-runtime/workspace"

func init() {
	RegisterOwnershipResolver(defaultOwnershipResolverName,
		&EtcdOwnershipResolver{client: nil, keyPrefix: defaultWorkspaceOwnershipKeyPrefix, timeout: 5 * time.Second})
}
