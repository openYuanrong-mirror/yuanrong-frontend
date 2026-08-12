/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

// Package proxyrouting owns function_proxy discovery, create selection, and
// strict instance-owner routing for frontend direct calls.
package proxyrouting

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// Capability identifies an operation advertised by a FunctionProxy endpoint.
type Capability string

const (
	// CapabilityInvoke routes function invocation requests.
	CapabilityInvoke Capability = "faas.invoke"
	// CapabilityCreate routes instance creation requests.
	CapabilityCreate Capability = "faas.create"
	// CapabilityKill routes instance termination requests.
	CapabilityKill Capability = "faas.kill"
	// CapabilityFileTransfer routes file upload and download requests.
	CapabilityFileTransfer Capability = "file.transfer"
	// CapabilityTCPTunnel routes SSH and WebSocket tunnel requests.
	CapabilityTCPTunnel Capability = "tcp.tunnel"

	defaultSuspectTTL             = 30 * time.Second
	endpointAddressCapacityFactor = 2
)

// Endpoint describes one discovered FunctionProxy and its supported operations.
type Endpoint struct {
	NodeID           string
	GRPCAddress      string
	TCPTunnelAddress string
	Version          string
	Capabilities     map[Capability]bool
	Health           string
	Revision         int64
}

// Discovery resolves healthy FunctionProxy endpoints.
type Discovery interface {
	GetByNode(nodeID string, capability Capability) (Endpoint, bool)
	GetNext(capability Capability, excluded map[string]struct{}) (Endpoint, bool)
}

// ProxySelector selects an endpoint for a new operation.
type ProxySelector interface {
	Select(ctx context.Context, capability Capability, excluded map[string]struct{}) (Endpoint, error)
}

// DefaultProxySelector selects from the process-wide discovery cache.
type DefaultProxySelector struct{}

// Select delegates endpoint selection to the process-wide discovery cache.
func (DefaultProxySelector) Select(
	ctx context.Context, capability Capability, excluded map[string]struct{},
) (Endpoint, error) {
	return Select(ctx, capability, excluded)
}

type suspectMarker interface {
	MarkSuspectAddress(address string, ttl time.Duration)
}

type suspectChecker interface {
	IsSuspectAddress(address string) bool
}

var defaultDiscovery = NewMemoryDiscovery()

var discoveryState = struct {
	sync.RWMutex
	discovery Discovery
}{discovery: defaultDiscovery}

func currentDiscovery() Discovery {
	discoveryState.RLock()
	defer discoveryState.RUnlock()
	return discoveryState.discovery
}

// UseCache restores the process-wide watcher-backed discovery cache.
func UseCache() {
	discoveryState.Lock()
	discoveryState.discovery = defaultDiscovery
	discoveryState.Unlock()
}

// ReplaceSnapshot atomically replaces the process-wide endpoint snapshot.
func ReplaceSnapshot(endpoints []Endpoint) {
	defaultDiscovery.ReplaceSnapshot(endpoints)
	UseCache()
}

// Upsert adds or updates an endpoint in the process-wide cache.
func Upsert(endpoint Endpoint) {
	defaultDiscovery.Upsert(endpoint)
}

// DeleteAtRevision removes an endpoint unless a newer revision is cached.
func DeleteAtRevision(nodeID string, revision int64) {
	defaultDiscovery.Delete(nodeID, revision)
}

// Lookup returns a healthy endpoint by proxy node ID and capability.
func Lookup(nodeID string, capability Capability) (Endpoint, bool) {
	discovery := currentDiscovery()
	if discovery == nil {
		return Endpoint{}, false
	}
	return discovery.GetByNode(nodeID, capability)
}

// Select chooses a healthy create endpoint while excluding endpoints already
// attempted by the current pre-dispatch retry loop.
func Select(ctx context.Context, capability Capability, excluded map[string]struct{}) (Endpoint, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return Endpoint{}, ctx.Err()
		default:
		}
	}
	discovery := currentDiscovery()
	if discovery == nil {
		return Endpoint{}, fmt.Errorf("frontend proxy discovery is not initialized")
	}
	endpoint, ok := discovery.GetNext(capability, excluded)
	if !ok {
		return Endpoint{}, fmt.Errorf("no frontend proxy endpoint supports capability %s", capability)
	}
	return endpoint, nil
}

// MarkSuspect temporarily excludes an endpoint address from routing.
func MarkSuspect(address string) {
	if address == "" {
		return
	}
	marker, ok := currentDiscovery().(suspectMarker)
	if ok {
		marker.MarkSuspectAddress(address, defaultSuspectTTL)
	}
}

// IsSuspect reports whether an endpoint address is temporarily excluded.
func IsSuspect(address string) bool {
	if address == "" {
		return false
	}
	checker, ok := currentDiscovery().(suspectChecker)
	return ok && checker.IsSuspectAddress(address)
}

// SetDiscoveryForTest replaces global discovery and returns a restore closure.
func SetDiscoveryForTest(discovery Discovery) func() {
	discoveryState.Lock()
	previous := discoveryState.discovery
	discoveryState.discovery = discovery
	discoveryState.Unlock()
	return func() {
		discoveryState.Lock()
		discoveryState.discovery = previous
		discoveryState.Unlock()
	}
}

// CurrentDiscoveryForTest returns the active discovery implementation for tests.
func CurrentDiscoveryForTest() Discovery {
	return currentDiscovery()
}

// MemoryDiscovery stores the current FunctionProxy snapshot in memory.
type MemoryDiscovery struct {
	mu           sync.RWMutex
	endpoints    map[string]Endpoint
	order        []string
	next         uint64
	suspectUntil map[string]time.Time
	revisions    map[string]int64
}

// NewMemoryDiscovery creates an empty in-memory discovery cache.
func NewMemoryDiscovery() *MemoryDiscovery {
	return &MemoryDiscovery{
		endpoints:    make(map[string]Endpoint),
		suspectUntil: make(map[string]time.Time),
		revisions:    make(map[string]int64),
	}
}

func endpointAddress(endpoint Endpoint, capability Capability) string {
	if capability == CapabilityTCPTunnel {
		return endpoint.TCPTunnelAddress
	}
	return endpoint.GRPCAddress
}

// IsRoutableAddress reports whether address has a usable host and port.
func IsRoutableAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsUnspecified()
}

// IsHostPort reports whether address is a non-empty host:port pair.
func IsHostPort(address string) bool {
	host, port, err := net.SplitHostPort(address)
	return err == nil && strings.TrimSpace(host) != "" && strings.TrimSpace(port) != ""
}

// ReplaceSnapshot atomically replaces all cached endpoints.
func (d *MemoryDiscovery) ReplaceSnapshot(endpoints []Endpoint) {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := make(map[string]Endpoint, len(endpoints))
	addresses := make(map[string]struct{}, len(endpoints)*endpointAddressCapacityFactor)
	order := make([]string, 0, len(endpoints))
	revisions := make(map[string]int64, len(endpoints))
	for _, endpoint := range endpoints {
		if !isHealthyEndpoint(endpoint) {
			continue
		}
		if _, exists := next[endpoint.NodeID]; !exists {
			order = append(order, endpoint.NodeID)
		}
		next[endpoint.NodeID] = endpoint
		addresses[endpoint.GRPCAddress] = struct{}{}
		addresses[endpoint.TCPTunnelAddress] = struct{}{}
		revisions[endpoint.NodeID] = endpoint.Revision
	}
	d.endpoints, d.order, d.revisions, d.next = next, order, revisions, 0
	for address := range d.suspectUntil {
		if _, ok := addresses[address]; !ok {
			delete(d.suspectUntil, address)
		}
	}
}

// Upsert adds or updates one endpoint unless its revision is stale.
func (d *MemoryDiscovery) Upsert(endpoint Endpoint) {
	if d == nil || !isHealthyEndpoint(endpoint) {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if current := d.revisions[endpoint.NodeID]; endpoint.Revision > 0 && current > endpoint.Revision {
		return
	}
	if _, exists := d.endpoints[endpoint.NodeID]; !exists {
		d.order = append(d.order, endpoint.NodeID)
	}
	d.endpoints[endpoint.NodeID] = endpoint
	if endpoint.Revision > 0 {
		d.revisions[endpoint.NodeID] = endpoint.Revision
	}
}

func isHealthyEndpoint(endpoint Endpoint) bool {
	return endpoint.NodeID != "" && (endpoint.GRPCAddress != "" || endpoint.TCPTunnelAddress != "") &&
		strings.EqualFold(endpoint.Health, "healthy") && len(endpoint.Capabilities) != 0
}

// Delete removes one endpoint unless the delete revision is stale.
func (d *MemoryDiscovery) Delete(nodeID string, revision int64) {
	if d == nil || nodeID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if current := d.revisions[nodeID]; revision > 0 && current > revision {
		return
	}
	if revision > 0 {
		d.revisions[nodeID] = revision
	}
	endpoint, exists := d.endpoints[nodeID]
	if !exists {
		return
	}
	delete(d.endpoints, nodeID)
	delete(d.suspectUntil, endpoint.GRPCAddress)
	delete(d.suspectUntil, endpoint.TCPTunnelAddress)
	for i, current := range d.order {
		if current == nodeID {
			d.order = append(d.order[:i], d.order[i+1:]...)
			break
		}
	}
	if len(d.order) == 0 {
		d.next = 0
	} else {
		d.next %= uint64(len(d.order))
	}
}

// GetNext returns the next healthy endpoint supporting capability.
func (d *MemoryDiscovery) GetNext(capability Capability, excluded map[string]struct{}) (Endpoint, bool) {
	if d == nil {
		return Endpoint{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.order) == 0 {
		return Endpoint{}, false
	}
	now := time.Now()
	for i := 0; i < len(d.order); i++ {
		idx := int(d.next % uint64(len(d.order)))
		d.next++
		endpoint, ok := d.endpoints[d.order[idx]]
		if !ok || !endpoint.Capabilities[capability] {
			continue
		}
		address := endpointAddress(endpoint, capability)
		if _, skip := excluded[address]; skip || d.isSuspectAddressLocked(address, now) || !IsRoutableAddress(address) {
			continue
		}
		return endpoint, true
	}
	return Endpoint{}, false
}

// GetByNode returns a healthy endpoint for nodeID and capability.
func (d *MemoryDiscovery) GetByNode(nodeID string, capability Capability) (Endpoint, bool) {
	if d == nil || nodeID == "" {
		return Endpoint{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	endpoint, ok := d.endpoints[nodeID]
	if !ok || (capability != "" && !endpoint.Capabilities[capability]) {
		return Endpoint{}, false
	}
	address := endpointAddress(endpoint, capability)
	if d.isSuspectAddressLocked(address, time.Now()) || !IsRoutableAddress(address) {
		return Endpoint{}, false
	}
	return endpoint, true
}

// MarkSuspectAddress excludes an address for ttl.
func (d *MemoryDiscovery) MarkSuspectAddress(address string, ttl time.Duration) {
	if d == nil || address == "" || ttl <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.suspectUntil[address] = time.Now().Add(ttl)
}

// IsSuspectAddress reports whether an address is currently excluded.
func (d *MemoryDiscovery) IsSuspectAddress(address string) bool {
	if d == nil || address == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.isSuspectAddressLocked(address, time.Now())
}

func (d *MemoryDiscovery) isSuspectAddressLocked(address string, now time.Time) bool {
	until, ok := d.suspectUntil[address]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(d.suspectUntil, address)
	return false
}
