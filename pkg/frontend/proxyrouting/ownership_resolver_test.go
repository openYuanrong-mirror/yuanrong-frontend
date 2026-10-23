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
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"

	"frontend/pkg/common/faas_common/etcd3"
)

// --- Mock OwnershipResolver for testing the registry / global swap ---

type mockOwnershipResolver struct {
	owningIP string
	err      error
}

func (m *mockOwnershipResolver) Resolve(_ context.Context, _ string) (string, error) {
	return m.owningIP, m.err
}

func TestOwnershipResolverResolveReturnsOwningIP(t *testing.T) {
	// Use gomonkey to mock the etcd client Get method so the EtcdOwnershipResolver
	// returns a known owning IP.
	etcdClient := &etcd3.EtcdClient{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Get",
		func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, key string,
			_ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{{Value: []byte("10.0.0.1")}},
			}, nil
		})
	defer patches.Reset()

	resolver := NewEtcdOwnershipResolver(etcdClient, "/agent-runtime/workspace")
	ip, err := resolver.Resolve(context.Background(), "user-alice")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", ip)
}

func TestOwnershipResolverPrefersInjectedClientOverGlobal(t *testing.T) {
	// Even when the process-wide meta etcd client is unavailable, a resolver
	// built with an injected client must keep working through that client.
	patches := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return nil
	})
	patches.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Get",
		func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, key string,
			_ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{{Value: []byte("10.0.0.7")}},
			}, nil
		})
	defer patches.Reset()

	resolver := NewEtcdOwnershipResolver(&etcd3.EtcdClient{}, "/agent-runtime/workspace")
	ip, err := resolver.Resolve(context.Background(), "user-frank")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.7", ip)
}

func TestOwnershipResolverResolveReturnsEmptyWhenNoRecord(t *testing.T) {
	etcdClient := &etcd3.EtcdClient{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Get",
		func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, key string,
			_ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
		})
	defer patches.Reset()

	resolver := NewEtcdOwnershipResolver(etcdClient, "/agent-runtime/workspace")
	ip, err := resolver.Resolve(context.Background(), "user-bob")
	require.NoError(t, err)
	require.Equal(t, "", ip)
}

func TestOwnershipResolverResolveReturnsErrorOnEtcdFailure(t *testing.T) {
	etcdClient := &etcd3.EtcdClient{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Get",
		func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, key string,
			_ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return nil, errors.New("etcd connection refused")
		})
	defer patches.Reset()

	resolver := NewEtcdOwnershipResolver(etcdClient, "/agent-runtime/workspace")
	_, err := resolver.Resolve(context.Background(), "user-carol")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read workspace ownership")
}

func TestOwnershipResolverReturnsErrorWhenClientNil(t *testing.T) {
	resolver := NewEtcdOwnershipResolver(nil, "/agent-runtime/workspace")
	_, err := resolver.Resolve(context.Background(), "user-dave")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not initialized")
}

func TestOwnershipResolverCanBeReplacedWithMock(t *testing.T) {
	mock := &mockOwnershipResolver{owningIP: "10.0.0.99"}
	restore := SetOwnershipResolverForTest(mock)
	defer restore()

	resolver := GetGlobalOwnershipResolver()
	require.NotNil(t, resolver)

	ip, err := resolver.Resolve(context.Background(), "user-eve")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.99", ip)
}

func TestOwnershipResolverRegistry(t *testing.T) {
	mock := &mockOwnershipResolver{owningIP: "10.0.0.50"}
	RegisterOwnershipResolver("test-mock", mock)

	got, ok := GetOwnershipResolver("test-mock")
	require.True(t, ok)
	require.NotNil(t, got)

	ip, err := got.Resolve(context.Background(), "any-owner")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.50", ip)

	_, ok = GetOwnershipResolver("nonexistent")
	require.False(t, ok)
}

func TestInitGlobalOwnershipResolverSetsEtcdResolver(t *testing.T) {
	etcdClient := &etcd3.EtcdClient{}
	originalGlobal := GetGlobalOwnershipResolver()
	defer SetGlobalOwnershipResolver(originalGlobal)

	InitGlobalOwnershipResolver("", etcdClient)

	resolver := GetGlobalOwnershipResolver()
	require.NotNil(t, resolver)

	etcdRes, ok := resolver.(*EtcdOwnershipResolver)
	require.True(t, ok)
	require.NotNil(t, etcdRes.client)
}
