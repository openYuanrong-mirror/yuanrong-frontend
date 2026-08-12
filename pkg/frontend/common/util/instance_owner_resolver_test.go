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

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"frontend/pkg/frontend/instancemanager"
)

func TestResolveInstanceOwnerUsesExactOwnerAndCapability(t *testing.T) {
	const (
		instanceID = "owner-resolver-instance"
		ownerID    = "owner-resolver-proxy"
	)
	instancemanager.RecordRouteOnlyInstance("tenant/function/$latest", instanceID, ownerID)
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)

	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID:       ownerID,
			Address:      "10.0.0.11:22769",
			Capabilities: map[string]bool{FrontendProxyCapabilityInvoke: true},
			Health:       "healthy",
		},
		{
			NodeID:       "non-owner-proxy",
			Address:      "10.0.0.12:22769",
			Capabilities: map[string]bool{FrontendProxyCapabilityInvoke: true},
			Health:       "healthy",
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	route, err := ResolveInstanceOwner(
		instanceID, FrontendProxyCapabilityInvoke, ProxyTransportGRPC)

	require.NoError(t, err)
	require.Equal(t, ownerID, route.OwnerProxyID)
	require.Equal(t, "10.0.0.11:22769", route.Address)
	require.Equal(t, instanceID, route.Instance.InstanceID)
}

func TestResolveInstanceOwnerRejectsAddressAsFunctionProxyID(t *testing.T) {
	const instanceID = "owner-resolver-address-instance"
	instancemanager.RecordRouteOnlyInstance(
		"tenant/function/$latest", instanceID, "10.0.0.11:22769")
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)

	_, err := ResolveInstanceOwner(
		instanceID, FrontendProxyCapabilityInvoke, ProxyTransportGRPC)

	require.Error(t, err)
	require.Contains(t, err.Error(), "must be a proxy node id")
}

func TestResolveInstanceOwnerDoesNotFallBackToSoleProxy(t *testing.T) {
	const (
		instanceID = "owner-resolver-no-fallback-instance"
		ownerID    = "missing-owner-proxy"
	)
	instancemanager.RecordRouteOnlyInstance("tenant/function/$latest", instanceID, ownerID)
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)

	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "sole-non-owner-proxy",
		Address:      "10.0.0.12:22769",
		Capabilities: map[string]bool{FrontendProxyCapabilityInvoke: true},
		Health:       "healthy",
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	_, err := ResolveInstanceOwner(
		instanceID, FrontendProxyCapabilityInvoke, ProxyTransportGRPC)

	require.Error(t, err)
	require.Contains(t, err.Error(), ownerID)
}

func TestWaitInstanceOwnerWaitsForTunnelCapability(t *testing.T) {
	const (
		instanceID = "owner-resolver-wait-instance"
		ownerID    = "owner-resolver-wait-proxy"
	)
	instancemanager.RecordRouteOnlyInstance("tenant/function/$latest", instanceID, ownerID)
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)

	discovery := newMemoryFrontendProxyDiscovery()
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	go func() {
		time.Sleep(20 * time.Millisecond)
		discovery.Upsert(frontendProxyEndpoint{
			NodeID:           ownerID,
			TCPTunnelAddress: "10.0.0.13:22800",
			Capabilities:     map[string]bool{FrontendProxyCapabilityTCPTunnel: true},
			Health:           "healthy",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	route, err := WaitInstanceOwner(
		ctx, instanceID, FrontendProxyCapabilityTCPTunnel, ProxyTransportTCPTunnel)

	require.NoError(t, err)
	require.Equal(t, ownerID, route.OwnerProxyID)
	require.Equal(t, "10.0.0.13:22800", route.Address)
}
