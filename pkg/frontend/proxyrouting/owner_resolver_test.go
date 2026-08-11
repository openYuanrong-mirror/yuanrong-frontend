/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package proxyrouting

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"frontend/pkg/frontend/instancemanager"
)

func TestResolveUsesExactOwnerNodeAndCapability(t *testing.T) {
	const instanceID = "proxyrouting-owner-instance"
	instancemanager.RecordRouteOnlyInstance("tenant/function/$latest", instanceID, "owner")
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "owner", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "non-owner", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})
	restore := SetDiscoveryForTest(discovery)
	defer restore()

	route, err := Resolve(instanceID, CapabilityInvoke, TransportGRPC)
	require.NoError(t, err)
	require.Equal(t, "owner", route.OwnerProxyID)
	require.Equal(t, "10.0.0.1:22769", route.Address)
}

func TestResolveRejectsAddressOwnerAndNeverUsesSoleProxy(t *testing.T) {
	const instanceID = "proxyrouting-address-instance"
	instancemanager.RecordRouteOnlyInstance("tenant/function/$latest", instanceID, "10.0.0.1:22769")
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{{NodeID: "sole", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true}}})
	restore := SetDiscoveryForTest(discovery)
	defer restore()

	_, err := Resolve(instanceID, CapabilityInvoke, TransportGRPC)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a proxy node id")
}

func TestWaitObservesDelayedTunnelCapability(t *testing.T) {
	const instanceID = "proxyrouting-wait-instance"
	instancemanager.RecordRouteOnlyInstance("tenant/function/$latest", instanceID, "owner")
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)
	discovery := NewMemoryDiscovery()
	restore := SetDiscoveryForTest(discovery)
	defer restore()
	go func() {
		time.Sleep(20 * time.Millisecond)
		discovery.Upsert(Endpoint{NodeID: "owner", TCPTunnelAddress: "10.0.0.1:22775", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityTCPTunnel: true}})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	route, err := Wait(ctx, instanceID, CapabilityTCPTunnel, TransportTCPTunnel)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:22775", route.Address)
}
