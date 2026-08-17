/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package util

import (
	"context"
	"time"

	"frontend/pkg/frontend/proxyrouting"
)

// These aliases keep the existing transport-focused tests concise while the
// production routing implementation lives exclusively in proxyrouting.
const (
	FrontendProxyCapabilityInvoke    = "faas.invoke"
	FrontendProxyCapabilityCreate    = "faas.create"
	FrontendProxyCapabilityKill      = "faas.kill"
	FrontendProxyCapabilityTCPTunnel = "tcp.tunnel"
	frontendProxyCapabilityInvoke    = FrontendProxyCapabilityInvoke
	frontendProxyCapabilityCreate    = FrontendProxyCapabilityCreate
	frontendProxyCapabilityKill      = FrontendProxyCapabilityKill
	proxyCapabilityTCPTunnel         = FrontendProxyCapabilityTCPTunnel
)

type frontendProxyEndpoint struct {
	NodeID           string
	Address          string
	TCPTunnelAddress string
	Version          string
	Capabilities     map[string]bool
	Health           string
	Revision         int64
}

type FrontendProxyEndpoint = frontendProxyEndpoint

func toRoutingEndpoint(endpoint frontendProxyEndpoint) proxyrouting.Endpoint {
	capabilities := make(map[proxyrouting.Capability]bool, len(endpoint.Capabilities))
	for capability, enabled := range endpoint.Capabilities {
		capabilities[proxyrouting.Capability(capability)] = enabled
	}
	health := endpoint.Health
	if health == "" {
		health = "healthy"
	}
	return proxyrouting.Endpoint{
		NodeID: endpoint.NodeID, GRPCAddress: endpoint.Address,
		TCPTunnelAddress: endpoint.TCPTunnelAddress, Version: endpoint.Version,
		Capabilities: capabilities, Health: health, Revision: endpoint.Revision,
	}
}

func fromRoutingEndpoint(endpoint proxyrouting.Endpoint) frontendProxyEndpoint {
	capabilities := make(map[string]bool, len(endpoint.Capabilities))
	for capability, enabled := range endpoint.Capabilities {
		capabilities[string(capability)] = enabled
	}
	return frontendProxyEndpoint{
		NodeID: endpoint.NodeID, Address: endpoint.GRPCAddress,
		TCPTunnelAddress: endpoint.TCPTunnelAddress, Version: endpoint.Version,
		Capabilities: capabilities, Health: endpoint.Health, Revision: endpoint.Revision,
	}
}

type memoryFrontendProxyDiscovery struct{ inner *proxyrouting.MemoryDiscovery }

func newMemoryFrontendProxyDiscovery() *memoryFrontendProxyDiscovery {
	return &memoryFrontendProxyDiscovery{inner: proxyrouting.NewMemoryDiscovery()}
}

func (d *memoryFrontendProxyDiscovery) ReplaceSnapshot(endpoints []frontendProxyEndpoint) {
	converted := make([]proxyrouting.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		converted = append(converted, toRoutingEndpoint(endpoint))
	}
	d.inner.ReplaceSnapshot(converted)
}

func (d *memoryFrontendProxyDiscovery) Upsert(endpoint frontendProxyEndpoint) {
	d.inner.Upsert(toRoutingEndpoint(endpoint))
}

func (d *memoryFrontendProxyDiscovery) GetNextEndpoint(capability string) (frontendProxyEndpoint, bool) {
	endpoint, ok := d.inner.GetNext(proxyrouting.Capability(capability), nil)
	return fromRoutingEndpoint(endpoint), ok
}

func (d *memoryFrontendProxyDiscovery) GetNext(
	capability proxyrouting.Capability, excluded map[string]struct{},
) (proxyrouting.Endpoint, bool) {
	return d.inner.GetNext(capability, excluded)
}

func (d *memoryFrontendProxyDiscovery) GetByNode(
	nodeID string, capability proxyrouting.Capability,
) (proxyrouting.Endpoint, bool) {
	return d.inner.GetByNode(nodeID, capability)
}

func (d *memoryFrontendProxyDiscovery) MarkSuspectAddress(address string, ttl time.Duration) {
	d.inner.MarkSuspectAddress(address, ttl)
}

func (d *memoryFrontendProxyDiscovery) IsSuspectAddress(address string) bool {
	return d.inner.IsSuspectAddress(address)
}

func setFrontendProxyDiscoveryForTest(discovery proxyrouting.Discovery) func() {
	return proxyrouting.SetDiscoveryForTest(discovery)
}

func currentFrontendProxyDiscovery() proxyrouting.Discovery {
	return proxyrouting.CurrentDiscoveryForTest()
}

func setFrontendProxyDiscovery(discovery proxyrouting.Discovery) {
	_ = proxyrouting.SetDiscoveryForTest(discovery)
}

func ReplaceFrontendProxyDiscoverySnapshot(endpoints []FrontendProxyEndpoint) {
	converted := make([]proxyrouting.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		converted = append(converted, toRoutingEndpoint(endpoint))
	}
	proxyrouting.ReplaceSnapshot(converted)
}

func LookupProxyTCPTunnelAddress(nodeID string) (string, bool) {
	endpoint, ok := proxyrouting.Lookup(nodeID, proxyrouting.CapabilityTCPTunnel)
	return endpoint.TCPTunnelAddress, ok
}

type ProxyTransport = proxyrouting.Transport

const (
	ProxyTransportGRPC      = proxyrouting.TransportGRPC
	ProxyTransportTCPTunnel = proxyrouting.TransportTCPTunnel
)

type InstanceOwnerRoute = proxyrouting.OwnerRoute

func ResolveInstanceOwner(instanceID, capability string, transport ProxyTransport) (InstanceOwnerRoute, error) {
	return proxyrouting.Resolve(instanceID, proxyrouting.Capability(capability), transport)
}

func WaitInstanceOwner(ctx context.Context, instanceID, capability string,
	transport ProxyTransport,
) (InstanceOwnerRoute, error) {
	return proxyrouting.Wait(ctx, instanceID, proxyrouting.Capability(capability), transport)
}
