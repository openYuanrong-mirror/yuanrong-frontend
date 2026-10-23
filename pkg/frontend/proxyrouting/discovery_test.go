/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package proxyrouting

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryDiscoveryFiltersHealthCapabilityAndSuspectAddress(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "invoke", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "unhealthy", GRPCAddress: "10.0.0.2:22769", Health: "unhealthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "create", GRPCAddress: "10.0.0.3:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityCreate: true}},
	})

	_, ok := discovery.GetByNode("unhealthy", CapabilityInvoke)
	require.False(t, ok)
	_, ok = discovery.GetByNode("create", CapabilityInvoke)
	require.False(t, ok)
	discovery.MarkSuspectAddress("10.0.0.1:22769", time.Minute)
	_, ok = discovery.GetByNode("invoke", CapabilityInvoke)
	require.False(t, ok)
}

func TestSelectExcludesAttemptedCreateEndpoint(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityCreate: true}},
		{NodeID: "b", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityCreate: true}},
	})
	restore := SetDiscoveryForTest(discovery)
	defer restore()

	first, err := Select(context.Background(), CapabilityCreate, nil)
	require.NoError(t, err)
	second, err := Select(context.Background(), CapabilityCreate,
		map[string]struct{}{first.GRPCAddress: {}})
	require.NoError(t, err)
	require.NotEqual(t, first.NodeID, second.NodeID)
}

func TestSelectHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Select(ctx, CapabilityCreate, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestUpsertDropsStaleIPIndexWhenAddressChanges(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.Upsert(Endpoint{NodeID: "a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityCreate: true}})

	_, ok := discovery.GetByIP("10.0.0.1", CapabilityCreate)
	require.True(t, ok)

	// Same node re-registers with a new address: the old ipIndex entry must go.
	discovery.Upsert(Endpoint{NodeID: "a", GRPCAddress: "10.0.0.9:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityCreate: true}})

	_, ok = discovery.GetByIP("10.0.0.1", CapabilityCreate)
	require.False(t, ok, "stale ipIndex entry should be removed on address change")
	endpoint, ok := discovery.GetByIP("10.0.0.9", CapabilityCreate)
	require.True(t, ok)
	require.Equal(t, "a", endpoint.NodeID)
}

func TestUpsertKeepsIPIndexWhenOldHostOwnedByOtherNode(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.Upsert(Endpoint{NodeID: "a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityCreate: true}})
	// Node b takes over the address before node a re-registers elsewhere.
	discovery.Upsert(Endpoint{NodeID: "b", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityCreate: true}})
	discovery.Upsert(Endpoint{NodeID: "a", GRPCAddress: "10.0.0.8:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityCreate: true}})

	// The rollback of a's previous host must not evict b's mapping.
	endpoint, ok := discovery.GetByIP("10.0.0.1", CapabilityCreate)
	require.True(t, ok)
	require.Equal(t, "b", endpoint.NodeID)
}
