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

package watcher

import (
	"testing"

	"github.com/stretchr/testify/require"

	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/frontend/proxyrouting"
)

func TestParseFrontendProxyEndpoint(t *testing.T) {
	endpoint, ok := parseFrontendProxyEndpoint("node-from-key", []byte(`{
		"node":"node-a",
		"proxyService":{
			"grpcAddress":"10.0.0.11:22769",
			"tcpTunnelAddress":"10.0.0.11:22775",
			"capabilities":["faas.create","faas.invoke","tcp.tunnel"],
			"version":"v1",
			"health":"healthy"
		}
	}`))

	require.True(t, ok)
	require.Equal(t, "node-a", endpoint.NodeID)
	require.Equal(t, "10.0.0.11:22769", endpoint.GRPCAddress)
	require.Equal(t, "10.0.0.11:22775", endpoint.TCPTunnelAddress)
	require.True(t, endpoint.Capabilities[proxyrouting.CapabilityCreate])
	require.True(t, endpoint.Capabilities[proxyrouting.CapabilityTCPTunnel])
	require.Equal(t, "v1", endpoint.Version)
}

func TestParseFrontendProxyEndpointRejectsUnhealthyRegistration(t *testing.T) {
	_, ok := parseFrontendProxyEndpoint("node-a", []byte(`{
		"proxyService":{"grpcAddress":"10.0.0.11:22769","capabilities":["faas.invoke"],"health":"unhealthy"}
	}`))
	require.False(t, ok)
}

func TestFrontendProxyEndpointHandlerAppliesPutAndDelete(t *testing.T) {
	proxyrouting.ReplaceSnapshot(nil)
	t.Cleanup(func() { proxyrouting.ReplaceSnapshot(nil) })
	key := frontendProxyEndpointPrefix + "node-a"
	frontendProxyEndpointHandler(&etcd3.Event{Type: etcd3.PUT, Key: key, Rev: 10, Value: []byte(`{
		"proxyService":{"grpcAddress":"10.0.0.11:22769","tcpTunnelAddress":"10.0.0.11:22775",
		"capabilities":["faas.invoke","tcp.tunnel"],"health":"healthy"}
	}`)})

	endpoint, ok := proxyrouting.Lookup("node-a", proxyrouting.CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "10.0.0.11:22769", endpoint.GRPCAddress)
	tunnelEndpoint, ok := proxyrouting.Lookup("node-a", proxyrouting.CapabilityTCPTunnel)
	require.True(t, ok)
	require.Equal(t, "10.0.0.11:22775", tunnelEndpoint.TCPTunnelAddress)

	frontendProxyEndpointHandler(&etcd3.Event{Type: etcd3.DELETE, Key: key, Rev: 11})
	_, ok = proxyrouting.Lookup("node-a", proxyrouting.CapabilityInvoke)
	require.False(t, ok)
	_, ok = proxyrouting.Lookup("node-a", proxyrouting.CapabilityTCPTunnel)
	require.False(t, ok)
}

func TestFrontendProxyEndpointHandlerIgnoresOlderRevision(t *testing.T) {
	proxyrouting.ReplaceSnapshot(nil)
	t.Cleanup(func() { proxyrouting.ReplaceSnapshot(nil) })
	key := frontendProxyEndpointPrefix + "node-a"
	registration := func(address string) []byte {
		return []byte(`{"proxyService":{"grpcAddress":"` + address +
			`","capabilities":["faas.invoke"],"health":"healthy"}}`)
	}
	frontendProxyEndpointHandler(&etcd3.Event{Type: etcd3.PUT, Key: key, Rev: 20,
		Value: registration("10.0.0.20:22769")})
	frontendProxyEndpointHandler(&etcd3.Event{Type: etcd3.PUT, Key: key, Rev: 19,
		Value: registration("10.0.0.19:22769")})

	endpoint, ok := proxyrouting.Lookup("node-a", proxyrouting.CapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, "10.0.0.20:22769", endpoint.GRPCAddress)
}

func TestFrontendProxyEndpointFilter(t *testing.T) {
	require.False(t, frontendProxyEndpointFilter(&etcd3.Event{Type: etcd3.SYNCED}))
	require.False(t, frontendProxyEndpointFilter(&etcd3.Event{Type: etcd3.PUT,
		Key: frontendProxyEndpointPrefix + "node-a"}))
	require.True(t, frontendProxyEndpointFilter(&etcd3.Event{Type: etcd3.PUT,
		Key: frontendProxyEndpointPrefix + "node-a/child"}))
}
