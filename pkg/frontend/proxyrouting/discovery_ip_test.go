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

package proxyrouting

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeIPsReturnsSortedUniqueIPsForHealthyEndpoints(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.3:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "node-b", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "node-c", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityCreate: true}},
		{NodeID: "node-d", GRPCAddress: "10.0.0.1:22770", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	ips := discovery.NodeIPs(CapabilityInvoke)
	require.Equal(t, []string{"10.0.0.1", "10.0.0.3"}, ips)
}

func TestNodeIPsExcludesUnhealthyEndpoints(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "node-b", GRPCAddress: "10.0.0.2:22769", Health: "unhealthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	ips := discovery.NodeIPs(CapabilityInvoke)
	require.Equal(t, []string{"10.0.0.1"}, ips)
}

func TestGetByIPHit(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	endpoint, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-a", endpoint.NodeID)
}

func TestGetByIPMissNotFound(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	_, ok := discovery.GetByIP("10.0.0.99", CapabilityInvoke)
	require.False(t, ok)
}

func TestGetByIPMissCapabilityMismatch(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	_, ok := discovery.GetByIP("10.0.0.1", CapabilityCreate)
	require.False(t, ok)
}

func TestGetByIPMissWhenEndpointDeleted(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	// Upsert skips unhealthy endpoints, so deleting the node simulates
	// the endpoint going away (becoming unhealthy / removed).
	discovery.Delete("node-a", 0)

	_, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.False(t, ok)
}

func TestIPIndexSyncOnUpsert(t *testing.T) {
	discovery := NewMemoryDiscovery()

	discovery.Upsert(Endpoint{
		NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})
	endpoint, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-a", endpoint.NodeID)

	discovery.Upsert(Endpoint{
		NodeID: "node-b", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})
	endpoint, ok = discovery.GetByIP("10.0.0.2", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-b", endpoint.NodeID)
}

func TestIPIndexClearedWhenAddressChangesOnUpsert(t *testing.T) {
	discovery := NewMemoryDiscovery()

	discovery.Upsert(Endpoint{
		NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})
	_, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.True(t, ok)

	// node-a 地址变更为 10.0.0.2:旧 IP 索引应被清理,新 IP 生效
	discovery.Upsert(Endpoint{
		NodeID: "node-a", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})

	_, ok = discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.False(t, ok, "stale ipIndex entry must be removed after address change")

	endpoint, ok := discovery.GetByIP("10.0.0.2", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-a", endpoint.NodeID)
}

func TestIPIndexKeptWhenOldHostTakenOverByOtherNode(t *testing.T) {
	discovery := NewMemoryDiscovery()

	discovery.Upsert(Endpoint{
		NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})
	// node-b 接管旧 IP(IP 复用场景)
	discovery.Upsert(Endpoint{
		NodeID: "node-b", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})
	// node-a 迁移到 10.0.0.2;旧 IP 已归属 node-b,不应被 node-a 的迁移清理
	discovery.Upsert(Endpoint{
		NodeID: "node-a", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
		Capabilities: map[Capability]bool{CapabilityInvoke: true},
	})

	endpoint, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-b", endpoint.NodeID)

	endpoint, ok = discovery.GetByIP("10.0.0.2", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-a", endpoint.NodeID)
}

func TestIPIndexSyncOnDelete(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "node-b", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	discovery.Delete("node-a", 0)

	_, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.False(t, ok)

	endpoint, ok := discovery.GetByIP("10.0.0.2", CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "node-b", endpoint.NodeID)
}

func TestIPIndexSyncOnReplaceSnapshot(t *testing.T) {
	discovery := NewMemoryDiscovery()
	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-a", GRPCAddress: "10.0.0.1:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	discovery.ReplaceSnapshot([]Endpoint{
		{NodeID: "node-b", GRPCAddress: "10.0.0.2:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
		{NodeID: "node-c", GRPCAddress: "10.0.0.3:22769", Health: "healthy",
			Capabilities: map[Capability]bool{CapabilityInvoke: true}},
	})

	_, ok := discovery.GetByIP("10.0.0.1", CapabilityInvoke)
	require.False(t, ok)

	_, ok = discovery.GetByIP("10.0.0.2", CapabilityInvoke)
	require.True(t, ok)

	_, ok = discovery.GetByIP("10.0.0.3", CapabilityInvoke)
	require.True(t, ok)
}
