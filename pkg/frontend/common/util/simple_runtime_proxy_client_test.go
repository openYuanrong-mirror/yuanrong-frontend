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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/common/httpconstant"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/proxyrouting"
)

const (
	testFunctionSegmentCount   = 3
	testConvertedArgumentCount = 3
	testBenchmarkPayloadBytes  = 64 << 10
	testLargePayloadBytes      = 1 << 20
	testCorrelationCount       = 2
	testTypedKillSignal        = 15
	testRouteResolutionTimeout = 20 * time.Millisecond
)

type fakeFrontendProxyServiceClient struct {
	req        *frontend_proxy.InvokeInstanceRequest
	resp       *frontend_proxy.InvokeInstanceResponse
	createReq  *frontend_proxy.CreateInstanceRequest
	createResp *frontend_proxy.CreateInstanceResponse
	killReq    *frontend_proxy.KillInstanceRequest
	killResp   *frontend_proxy.KillInstanceResponse
	invokeCtx  context.Context
	createCtx  context.Context
	killCtx    context.Context
	invokeFn   func(context.Context, *frontend_proxy.InvokeInstanceRequest) (*frontend_proxy.InvokeInstanceResponse, error)
	createFn   func(context.Context, *frontend_proxy.CreateInstanceRequest) (*frontend_proxy.CreateInstanceResponse, error)
	killFn     func(context.Context, *frontend_proxy.KillInstanceRequest) (*frontend_proxy.KillInstanceResponse, error)
	err        error
	calls      int
	stream     frontend_proxy.FrontendProxyService_InvokeInstanceStreamClient
}

type fakeInvokeInstanceStreamClient struct {
	grpc.ClientStream
	frames []*frontend_proxy.InvokeInstanceStreamResponse
}

func (f *fakeInvokeInstanceStreamClient) Recv() (*frontend_proxy.InvokeInstanceStreamResponse, error) {
	if len(f.frames) == 0 {
		return nil, io.EOF
	}
	frame := f.frames[0]
	f.frames = f.frames[1:]
	return frame, nil
}

type recordingSSEWriter struct {
	events     [][]byte
	disconnect chan struct{}
}

func (w *recordingSSEWriter) SSEWrite(data []byte) (int, error) {
	w.events = append(w.events, append([]byte(nil), data...))
	return len(data), nil
}

func (w *recordingSSEWriter) ClientDisconnectChan() <-chan struct{} {
	return w.disconnect
}

type fakeFrontendProxyClientFactory struct {
	address      string
	addresses    []string
	client       frontend_proxy.FrontendProxyServiceClient
	clientsByURL map[string]frontend_proxy.FrontendProxyServiceClient
	err          error
	errByAddress map[string]error
	evicted      []string
}

func (f *fakeFrontendProxyClientFactory) ClientForAddress(
	address string,
) (frontend_proxy.FrontendProxyServiceClient, error) {
	f.address = address
	f.addresses = append(f.addresses, address)
	if err := f.errByAddress[address]; err != nil {
		return nil, err
	}
	if client := f.clientsByURL[address]; client != nil {
		return client, f.err
	}
	return f.client, f.err
}

func (f *fakeFrontendProxyClientFactory) EvictAddress(address string) {
	f.evicted = append(f.evicted, address)
}

func recordRouteOnlyForTest(t *testing.T, instanceID, ownerProxyID string) {
	t.Helper()
	instancemanager.RecordRouteOnlyInstance("tenant/test-function/$latest", instanceID, ownerProxyID)
	t.Cleanup(func() {
		instancemanager.RemoveRouteOnlyInstance(instanceID)
	})
}

func (f *fakeFrontendProxyServiceClient) InvokeInstance(ctx context.Context, in *frontend_proxy.InvokeInstanceRequest,
	opts ...grpc.CallOption,
) (*frontend_proxy.InvokeInstanceResponse, error) {
	f.calls++
	f.req = in
	f.invokeCtx = ctx
	if f.invokeFn != nil {
		return f.invokeFn(ctx, in)
	}
	return f.resp, f.err
}

func (f *fakeFrontendProxyServiceClient) InvokeInstanceStream(
	ctx context.Context,
	in *frontend_proxy.InvokeInstanceRequest,
	_ ...grpc.CallOption,
) (frontend_proxy.FrontendProxyService_InvokeInstanceStreamClient, error) {
	f.calls++
	f.req = in
	f.invokeCtx = ctx
	if f.err != nil {
		return nil, f.err
	}
	if f.stream == nil {
		return nil, errors.New("invoke stream is not configured")
	}
	return f.stream, nil
}

func (f *fakeFrontendProxyServiceClient) CreateInstance(ctx context.Context, in *frontend_proxy.CreateInstanceRequest,
	opts ...grpc.CallOption,
) (*frontend_proxy.CreateInstanceResponse, error) {
	f.calls++
	f.createReq = in
	f.createCtx = ctx
	if f.createFn != nil {
		return f.createFn(ctx, in)
	}
	return f.createResp, f.err
}

func (f *fakeFrontendProxyServiceClient) KillInstance(ctx context.Context, in *frontend_proxy.KillInstanceRequest,
	opts ...grpc.CallOption,
) (*frontend_proxy.KillInstanceResponse, error) {
	f.calls++
	f.killReq = in
	f.killCtx = ctx
	if f.killFn != nil {
		return f.killFn(ctx, in)
	}
	return f.killResp, f.err
}

func createResponseWithUnknownReadyCallResult(
	t *testing.T,
	callResult *core.CallResult,
) *frontend_proxy.CreateInstanceResponse {
	t.Helper()
	payload, err := proto.Marshal(callResult)
	require.NoError(t, err)
	resp := &frontend_proxy.CreateInstanceResponse{
		Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
		Create: &core.CreateResponse{
			Code:       common.ErrorCode_ERR_NONE,
			InstanceID: callResult.GetInstanceID(),
		},
		RouteAddress: "proxy-node-ready",
	}
	var unknown []byte
	unknown = protowire.AppendTag(unknown, createReadyCallResultFieldNumber, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, payload)
	resp.ProtoReflect().SetUnknown(unknown)
	return resp
}

func consumeProtoFieldCounts(t *testing.T, payload []byte) map[protowire.Number]int {
	t.Helper()
	counts := map[protowire.Number]int{}
	for len(payload) > 0 {
		number, wireType, n := protowire.ConsumeTag(payload)
		require.GreaterOrEqual(t, n, 0, "consume proto tag failed: %v", protowire.ParseError(n))
		payload = payload[n:]
		counts[number]++
		n = protowire.ConsumeFieldValue(number, wireType, payload)
		require.GreaterOrEqual(t, n, 0, "consume proto field %d failed: %v", number, protowire.ParseError(n))
		payload = payload[n:]
	}
	return counts
}

func consumeProtoBytesField(t *testing.T, payload []byte, target protowire.Number) []byte {
	t.Helper()
	for len(payload) > 0 {
		number, wireType, n := protowire.ConsumeTag(payload)
		require.GreaterOrEqual(t, n, 0, "consume proto tag failed: %v", protowire.ParseError(n))
		payload = payload[n:]
		if number == target {
			require.Equal(t, protowire.BytesType, wireType)
			value, n := protowire.ConsumeBytes(payload)
			require.GreaterOrEqual(t, n, 0, "consume proto bytes field %d failed: %v", number, protowire.ParseError(n))
			return value
		}
		n = protowire.ConsumeFieldValue(number, wireType, payload)
		require.GreaterOrEqual(t, n, 0, "consume proto field %d failed: %v", number, protowire.ParseError(n))
		payload = payload[n:]
	}
	return nil
}

func consumeProtoVarintField(t *testing.T, payload []byte, target protowire.Number) uint64 {
	t.Helper()
	for len(payload) > 0 {
		number, wireType, n := protowire.ConsumeTag(payload)
		require.GreaterOrEqual(t, n, 0, "consume proto tag failed: %v", protowire.ParseError(n))
		payload = payload[n:]
		if number == target {
			require.Equal(t, protowire.VarintType, wireType)
			value, n := protowire.ConsumeVarint(payload)
			require.GreaterOrEqual(t, n, 0, "consume proto varint field %d failed: %v", number, protowire.ParseError(n))
			return value
		}
		n = protowire.ConsumeFieldValue(number, wireType, payload)
		require.GreaterOrEqual(t, n, 0, "consume proto field %d failed: %v", number, protowire.ParseError(n))
		payload = payload[n:]
	}
	return 0
}

func addInstanceRouteForTest(t *testing.T, functionKey, instanceID, functionProxyID string) func() {
	return addInstanceRouteForStatusTest(t, functionKey, instanceID, functionProxyID,
		constant.KernelInstanceStatusRunning)
}

func addInstanceRouteForStatusTest(t *testing.T, functionKey, instanceID, functionProxyID string,
	status constant.InstanceStatus) func() {
	t.Helper()
	function := functionKey
	if len(strings.Split(functionKey, "/")) != testFunctionSegmentCount {
		function = "tenant/" + functionKey + "/$latest"
	}
	insSpec := &types.InstanceSpecification{
		InstanceID:      instanceID,
		Function:        function,
		FunctionProxyID: functionProxyID,
		CreateOptions: map[string]string{
			constant.FunctionKeyNote:  functionKey,
			constant.ResourceSpecNote: `{"cpu":100,"memory":100}`,
		},
		InstanceStatus: types.InstanceStatus{
			Code: int32(status),
		},
	}
	value, err := json.Marshal(insSpec)
	require.NoError(t, err)
	event := &etcd3.Event{
		Key:   "/sn/instance/business/yrk/tenant/tenant/function/function/version/$latest/defaultaz/request/" + instanceID,
		Value: value,
	}
	instancemanager.ProcessInstanceUpdate(event)
	return func() {
		instancemanager.ProcessInstanceDelete(&etcd3.Event{Key: event.Key, PrevValue: value})
	}
}

func TestMemoryFrontendProxyDiscoveryGetNextEndpointRoundRobinsCandidates(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-node-a", Address: "10.0.0.8:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
		{
			NodeID: "proxy-node-b", Address: "10.0.0.9:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
	})

	first, ok := discovery.GetNextEndpoint(frontendProxyCapabilityInvoke)
	require.True(t, ok)
	second, ok := discovery.GetNextEndpoint(frontendProxyCapabilityInvoke)
	require.True(t, ok)
	third, ok := discovery.GetNextEndpoint(frontendProxyCapabilityInvoke)
	require.True(t, ok)

	require.NotEqual(t, first.NodeID, second.NodeID)
	require.Equal(t, first.NodeID, third.NodeID)
}

func TestFrontendProxyRuntimeRequestIDProcessHelper(t *testing.T) {
	if os.Getenv("FRONTEND_PROXY_CORRELATION_ID_HELPER") != "1" {
		return
	}
	fmt.Printf("RUNTIME_ID=%s\n", newFrontendProxyRuntimeRequestID())
}

func TestRequestIDsUniqueAcrossIsolatedFrontendProcesses(t *testing.T) {
	generate := func() string {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFrontendProxyRuntimeRequestIDProcessHelper$")
		cmd.Env = append(os.Environ(), "FRONTEND_PROXY_CORRELATION_ID_HELPER=1")
		output, err := cmd.Output()
		require.NoError(t, err)
		const marker = "RUNTIME_ID="
		markerAt := strings.Index(string(output), marker)
		require.NotEqual(t, -1, markerAt)
		fields := strings.Fields(string(output)[markerAt+len(marker):])
		require.NotEmpty(t, fields)
		return fields[0]
	}

	first := generate()
	second := generate()
	require.Len(t, first, runtimeRequestIDLength)
	require.Len(t, second, runtimeRequestIDLength)
	_, err := hex.DecodeString(first)
	require.NoError(t, err)
	_, err = hex.DecodeString(second)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestFrontendProxyRuntimeRequestIDMatchesLibruntimeWireFormat(t *testing.T) {
	first := newFrontendProxyRuntimeRequestID()
	second := newFrontendProxyRuntimeRequestID()

	require.Len(t, first, runtimeRequestIDLength)
	require.Len(t, second, runtimeRequestIDLength)
	_, err := hex.DecodeString(first)
	require.NoError(t, err)
	_, err = hex.DecodeString(second)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestMemoryFrontendProxyDiscoveryReplaceSnapshotPrunesRemovedSuspectAddress(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-a",
		Address:      "10.0.0.8:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	discovery.MarkSuspectAddress("10.0.0.8:22769", time.Minute)
	require.True(t, discovery.IsSuspectAddress("10.0.0.8:22769"))

	discovery.ReplaceSnapshot(nil)

	require.False(t, discovery.IsSuspectAddress("10.0.0.8:22769"))
}

func TestMemoryFrontendProxyDiscoveryGetByNodeSkipsSuspectEndpoint(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-a",
		Address:      "10.0.0.8:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	discovery.MarkSuspectAddress("10.0.0.8:22769", time.Minute)

	_, ok := discovery.GetByNode("proxy-node-a", frontendProxyCapabilityInvoke)

	require.False(t, ok)
}

func TestMemoryFrontendProxyDiscoveryKeepsTunnelWhenGrpcIsSuspect(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:           "proxy-node-a",
		Address:          "10.0.0.8:22769",
		TCPTunnelAddress: "10.0.0.8:22775",
		Capabilities: map[string]bool{
			frontendProxyCapabilityInvoke: true,
			proxyCapabilityTCPTunnel:      true,
		},
	}})
	discovery.MarkSuspectAddress("10.0.0.8:22769", time.Minute)
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	_, invokeOK := discovery.GetByNode("proxy-node-a", frontendProxyCapabilityInvoke)
	tunnelAddress, tunnelOK := LookupProxyTCPTunnelAddress("proxy-node-a")

	require.False(t, invokeOK)
	require.True(t, tunnelOK)
	require.Equal(t, "10.0.0.8:22775", tunnelAddress)
}

func TestLookupProxyTCPTunnelAddressRejectsUnspecifiedHost(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:           "proxy-node-a",
		TCPTunnelAddress: "0.0.0.0:22775",
		Capabilities:     map[string]bool{proxyCapabilityTCPTunnel: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	_, ok := LookupProxyTCPTunnelAddress("proxy-node-a")

	require.False(t, ok)
}

func requireFaaSInvokeRequest(t *testing.T, fakeService *fakeFrontendProxyServiceClient, payload, got []byte) {
	t.Helper()
	require.Equal(t, payload, got)
	require.NotNil(t, fakeService.req)
	require.Equal(t, "frontend-1", fakeService.req.Context.FrontendClientID)
	require.Equal(t, "tenant-1", fakeService.req.Context.TenantID)
	require.NotEmpty(t, fakeService.req.Context.RequestID)
	require.Equal(t, "trace-1", fakeService.req.Context.TraceID)
	require.Equal(t, "func-key", fakeService.req.Invoke.Function)
	require.Equal(t, "instance-1", fakeService.req.Invoke.InstanceID)
	require.Equal(t, "trace-1", fakeService.req.Invoke.TraceID)
	require.NotEmpty(t, fakeService.req.Invoke.RequestID)
	require.Equal(t, common.Arg_VALUE, fakeService.req.Invoke.Args[0].Type)
	require.Equal(t, uint64(1), consumeProtoVarintField(t, fakeService.req.Invoke.Args[0].Value, 1))
	functionMeta := consumeProtoBytesField(t, fakeService.req.Invoke.Args[0].Value, 2)
	require.Equal(t, "func-key", string(consumeProtoBytesField(t, functionMeta, functionMetaIDField)))
	require.Equal(t, uint64(api.FaaSApi), consumeProtoVarintField(t, functionMeta, functionMetaAPIField))
	invocationMeta := consumeProtoBytesField(t, fakeService.req.Invoke.Args[0].Value, 4)
	require.NotEmpty(t, consumeProtoBytesField(t, invocationMeta, 1))
	require.Equal(t, common.Arg_VALUE, fakeService.req.Invoke.Args[1].Type)
	require.Equal(t, []byte(simpleRuntimeFaaSMetaPrefix+"arg-value"), fakeService.req.Invoke.Args[1].Value)
	require.Equal(t, []string{"nested-1"}, fakeService.req.Invoke.Args[1].NestedRefs)
	require.Equal(t, "value-a", fakeService.req.Invoke.InvokeOptions.CustomTag["tag-a"])
	require.Equal(t, "proxy-a", fakeService.req.Invoke.InvokeOptions.CustomTag["YR_ROUTE"])
}

func TestGRPCFrontendProxyInvokeClientBuildsRequestAndReturnsMessagePayload(t *testing.T) {
	payload := []byte("proxy-message-result")
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{
				Code:    common.ErrorCode_ERR_NONE,
				Message: string(payload),
				SmallObjects: []*common.SmallObject{{
					Id:    "return-object-1",
					Value: append(make([]byte, 16), []byte("wrong-buffer-view")...),
				}},
			},
		},
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	got, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		args: []api.Arg{{
			Type:            api.Value,
			Data:            []byte("arg-value"),
			NestedObjectIDs: []string{"nested-1"},
			TenantID:        "tenant-1",
		}},
		options: api.InvokeOptions{
			TraceID:          "trace-1",
			CustomExtensions: map[string]string{"tag-a": "value-a"},
			CreateOpt:        map[string]string{"YR_ROUTE": "proxy-a"},
		},
	})

	require.NoError(t, err)
	requireFaaSInvokeRequest(t, fakeService, payload, got)
}

func TestConvertSimpleRuntimeInvokeOptionsUsesExistingRuntimeProtocol(t *testing.T) {
	got := convertSimpleRuntimeInvokeOptions(api.InvokeOptions{
		CustomExtensions: map[string]string{"tag-a": "value-a"},
		InstanceSession:  &api.InstanceSessionConfig{SessionID: "session-a"},
		IsInterrupted:    true,
		ForceInvoke:      true,
		BypassDataSystem: true,
	})

	require.Equal(t, "value-a", got.CustomTag["tag-a"])
	require.Equal(t, "session-a", got.CustomTag["YR_AGENT_SESSION_ID"])
	require.Equal(t, "true", got.CustomTag["IS_INTERRUPTED"])
	require.Contains(t, got.CustomTag, "ENABLE_FORCE_INVOKE")
	require.Empty(t, got.CustomTag["ENABLE_FORCE_INVOKE"])
	require.True(t, got.BypassDatasystem)
}

func TestGRPCFrontendProxyInvokeClientStreamsEventsAndReturnsFinalPayload(t *testing.T) {
	payload := []byte("proxy-stream-final")
	fakeService := &fakeFrontendProxyServiceClient{
		stream: &fakeInvokeInstanceStreamClient{frames: []*frontend_proxy.InvokeInstanceStreamResponse{
			{Payload: &frontend_proxy.InvokeInstanceStreamResponse_Event{Event: []byte("event-one")}},
			{Payload: &frontend_proxy.InvokeInstanceStreamResponse_Event{Event: []byte("event-two")}},
			{Payload: &frontend_proxy.InvokeInstanceStreamResponse_Final{Final: &frontend_proxy.InvokeInstanceResponse{
				Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
				CallResult: &core.CallResult{
					Code:    common.ErrorCode_ERR_NONE,
					Message: string(payload),
				},
			}}},
		}},
	}
	writer := &recordingSSEWriter{disconnect: make(chan struct{})}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	got, err := client.InvokeByInstanceIDStream(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		args:       []api.Arg{{Type: api.Value, TenantID: "tenant-1"}},
		options:    api.InvokeOptions{TraceID: "trace-1"},
	}, writer)

	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.Equal(t, [][]byte{[]byte("event-one"), []byte("event-two")}, writer.events)
	require.Equal(t, "instance-1", fakeService.req.Invoke.InstanceID)
}

func TestGRPCFrontendProxyInvokeClientCarriesSSEAcceptHeader(t *testing.T) {
	payload := []byte("proxy-stream-final")
	fakeService := &fakeFrontendProxyServiceClient{
		stream: &fakeInvokeInstanceStreamClient{frames: []*frontend_proxy.InvokeInstanceStreamResponse{
			{Payload: &frontend_proxy.InvokeInstanceStreamResponse_Final{Final: &frontend_proxy.InvokeInstanceResponse{
				Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
				CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE, Message: string(payload)},
			}}},
		}},
	}
	writer := &recordingSSEWriter{disconnect: make(chan struct{})}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	_, err := client.InvokeByInstanceIDStream(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		args:       []api.Arg{{Type: api.Value, TenantID: "tenant-1"}},
		options: api.InvokeOptions{CustomExtensions: map[string]string{
			"Accept": httpconstant.AcceptEventStream,
		}},
	}, writer)

	require.NoError(t, err)
	require.Equal(t, httpconstant.AcceptEventStream,
		fakeService.req.Invoke.InvokeOptions.CustomTag["Accept"])
}

func TestGRPCFrontendProxyInvokeClientStripsFaaSResultMetaPrefix(t *testing.T) {
	payload := []byte(simpleRuntimeFaaSMetaPrefix + `{"body":"ok","innerCode":"0"}`)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE, Message: string(payload)},
		},
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	got, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
	})

	require.NoError(t, err)
	require.Equal(t, []byte(`{"body":"ok","innerCode":"0"}`), got)
}

func TestGRPCFrontendProxyInvokeClientKeepsPosixResultMetaPrefix(t *testing.T) {
	payload := []byte(simpleRuntimeFaaSMetaPrefix + `{"body":"ok","innerCode":"0"}`)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE, Message: string(payload)},
		},
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	got, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.PosixApi},
		instanceID: "instance-1",
	})

	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestConvertSimpleRuntimeInvokeArgsPrefixesFaaSUserValuesOnly(t *testing.T) {
	args := []api.Arg{
		{Type: api.Value, Data: []byte(`{"body":{}}`), NestedObjectIDs: []string{"nested-1"}},
		{Type: api.ArgType(common.Arg_OBJECT_REF), Data: []byte("object-ref-1")},
	}

	got := convertSimpleRuntimeInvokeArgs(api.FunctionMeta{FuncID: "faas-func", Api: api.FaaSApi}, args)

	require.Len(t, got, testConvertedArgumentCount)
	require.Equal(t, common.Arg_VALUE, got[0].Type, "metadata arg must remain first")
	require.Equal(t, common.Arg_VALUE, got[1].Type)
	require.Equal(t, []byte(simpleRuntimeFaaSMetaPrefix+`{"body":{}}`), got[1].Value)
	require.Equal(t, []string{"nested-1"}, got[1].NestedRefs)
	require.Equal(t, common.Arg_OBJECT_REF, got[2].Type)
	require.Equal(t, []byte("object-ref-1"), got[2].Value)
	require.Equal(t, []byte(`{"body":{}}`), args[0].Data, "prefixing must not mutate caller-owned args")
}

func TestConvertSimpleRuntimeInvokeArgsKeepsPosixArgsUnwrapped(t *testing.T) {
	got := convertSimpleRuntimeInvokeArgs(api.FunctionMeta{FuncID: "posix-func", Api: api.PosixApi}, []api.Arg{{
		Type: api.Value,
		Data: []byte(`{"body":{}}`),
	}})

	require.Len(t, got, 1)
	require.Equal(t, common.Arg_VALUE, got[0].Type)
	require.Equal(t, []byte(`{"body":{}}`), got[0].Value)
}

func TestPrefixSimpleRuntimeFaaSInvokeArgsDoesNotCopyReadOnlyObjectRefPayload(t *testing.T) {
	payload := []byte("object-ref-1")
	arg := &common.Arg{Type: common.Arg_OBJECT_REF, Value: payload, NestedRefs: []string{"nested-1"}}

	got := prefixSimpleRuntimeFaaSInvokeArgs([]*common.Arg{arg})

	require.Len(t, got, 1)
	require.NotSame(t, arg, got[0])
	require.Same(t, &payload[0], &got[0].Value[0])
	require.Equal(t, arg.NestedRefs, got[0].NestedRefs)
}

func BenchmarkPrefixSimpleRuntimeFaaSInvokeArgs64KiB(b *testing.B) {
	arg := &common.Arg{Type: common.Arg_VALUE, Value: make([]byte, testBenchmarkPayloadBytes)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = prefixSimpleRuntimeFaaSInvokeArgs([]*common.Arg{arg})
	}
}

func BenchmarkPrefixSimpleRuntimeFaaSInvokeArgs64KiBProtoCloneBaseline(b *testing.B) {
	arg := &common.Arg{Type: common.Arg_VALUE, Value: make([]byte, testBenchmarkPayloadBytes)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		next, ok := proto.Clone(arg).(*common.Arg)
		if !ok {
			b.Fatal("cloned argument has unexpected type")
		}
		next.Value = append([]byte(simpleRuntimeFaaSMetaPrefix), next.GetValue()...)
	}
}

func TestPrefixSimpleRuntimeFaaSInvokeArgsForRPCUsesBoundedBuffer(t *testing.T) {
	arg := &common.Arg{Type: common.Arg_VALUE, Value: make([]byte, testBenchmarkPayloadBytes)}

	first, releaseFirst := prefixSimpleRuntimeFaaSInvokeArgsForRPC([]*common.Arg{arg})
	require.Equal(t, simpleRuntimeMediumValueBufferSize, cap(first[0].Value))
	releaseFirst()
	second, releaseSecond := prefixSimpleRuntimeFaaSInvokeArgsForRPC([]*common.Arg{arg})
	defer releaseSecond()

	require.Equal(t, simpleRuntimeMediumValueBufferSize, cap(second[0].Value))
	require.Equal(t, []byte(simpleRuntimeFaaSMetaPrefix), second[0].Value[:len(simpleRuntimeFaaSMetaPrefix)])
	require.Equal(t, arg.Value, second[0].Value[len(simpleRuntimeFaaSMetaPrefix):])
}

func TestAcquireSimpleRuntimeValueBufferDoesNotPoolLargePayload(t *testing.T) {
	value, pool := acquireSimpleRuntimeValueBuffer(testLargePayloadBytes)
	require.Nil(t, pool)
	require.GreaterOrEqual(t, cap(value), testLargePayloadBytes)
}

func BenchmarkPrefixSimpleRuntimeFaaSInvokeArgsForRPC64KiB(b *testing.B) {
	arg := &common.Arg{Type: common.Arg_VALUE, Value: make([]byte, testBenchmarkPayloadBytes)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, release := prefixSimpleRuntimeFaaSInvokeArgsForRPC([]*common.Arg{arg})
		release()
	}
}

func TestGRPCFrontendProxyLifecycleClientBuildsCreateRequestAndReturnsInstanceID(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create: &core.CreateResponse{
				Code:       common.ErrorCode_ERR_NONE,
				InstanceID: "instance-created",
			},
			RouteAddress: "proxy-node-a",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	got, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		args: []api.Arg{{
			Type:            api.Value,
			Data:            []byte("create-arg"),
			NestedObjectIDs: []string{"nested-1"},
			TenantID:        "tenant-1",
		}},
		options: api.InvokeOptions{
			TraceID:          "trace-create",
			CustomExtensions: map[string]string{"custom-a": "value-a"},
			CreateOpt:        map[string]string{"create-a": "value-b"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, "instance-created", got)
	require.NotNil(t, fakeService.createReq)
	require.Equal(t, "frontend-1", fakeService.createReq.Context.FrontendClientID)
	require.Equal(t, "tenant-1", fakeService.createReq.Context.TenantID)
	require.NotEmpty(t, fakeService.createReq.Context.RequestID)
	require.Len(t, fakeService.createReq.Context.RequestID, runtimeRequestIDLength)
	_, err = hex.DecodeString(fakeService.createReq.Context.RequestID)
	require.NoError(t, err)
	require.Equal(t, "trace-create", fakeService.createReq.Context.TraceID)
	require.Equal(t, "func-key", fakeService.createReq.Create.Function)
	require.NotEmpty(t, fakeService.createReq.Create.RequestID)
	require.Equal(t, fakeService.createReq.Context.RequestID, fakeService.createReq.Create.RequestID)
	require.Equal(t, "trace-create", fakeService.createReq.Create.TraceID)
	require.Len(t, fakeService.createReq.Create.Args, 2)
	require.Equal(t, common.Arg_VALUE, fakeService.createReq.Create.Args[0].Type)
	require.NotEmpty(t, fakeService.createReq.Create.Args[0].Value)
	require.Equal(t, common.Arg_VALUE, fakeService.createReq.Create.Args[1].Type)
	require.Equal(t, []byte(simpleRuntimeFaaSMetaPrefix+"create-arg"),
		fakeService.createReq.Create.Args[1].Value)
	require.Equal(t, []string{"nested-1"}, fakeService.createReq.Create.Args[1].NestedRefs)
	require.Equal(t, "value-a", fakeService.createReq.Create.CreateOptions["custom-a"])
	require.Equal(t, "value-b", fakeService.createReq.Create.CreateOptions["create-a"])
	// No CPU/Memory set → SchedulingOps is nil so the executor applies its own default.
	require.Nil(t, fakeService.createReq.Create.GetSchedulingOps())
}

func TestGRPCFrontendProxyLifecycleClientBuildsCreateRequestEmitsCpuMemory(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-node-a",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		options: api.InvokeOptions{
			Cpu:             600,
			Memory:          2048,
			CustomResources: map[string]float64{"nvidia.com/gpu": 1},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, fakeService.createReq.Create.GetSchedulingOps())
	resources := fakeService.createReq.Create.GetSchedulingOps().GetResources()
	require.Equal(t, float64(600), resources[constant.ResourceCPUName])
	require.Equal(t, float64(2048), resources[constant.ResourceMemoryName])
	require.Equal(t, float64(1), resources["nvidia.com/gpu"])
}

func TestGRPCFrontendProxyLifecycleClientBuildsCreateRequestPassesThroughInvalidValues(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-node-a",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	// Invalid values are passed through unvalidated; the docker daemon rejects them.
	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		options:  api.InvokeOptions{Cpu: -1, Memory: 0},
	})

	require.NoError(t, err)
	require.NotNil(t, fakeService.createReq.Create.GetSchedulingOps())
	resources := fakeService.createReq.Create.GetSchedulingOps().GetResources()
	require.Equal(t, float64(-1), resources[constant.ResourceCPUName])
	_, hasMemory := resources[constant.ResourceMemoryName]
	require.False(t, hasMemory, "zero memory must be omitted")
}

func TestGRPCFrontendProxyLifecycleClientBuildsCreateRequestOmitsMissingCpuMemory(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-node-a",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	// Only a custom resource set, no CPU/Memory → resources has the custom key only,
	// CPU/Memory fall through to the executor default.
	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		options:  api.InvokeOptions{CustomResources: map[string]float64{"nvidia.com/gpu": 2}},
	})

	require.NoError(t, err)
	require.NotNil(t, fakeService.createReq.Create.GetSchedulingOps())
	resources := fakeService.createReq.Create.GetSchedulingOps().GetResources()
	require.Equal(t, float64(2), resources["nvidia.com/gpu"])
	_, hasCPU := resources[constant.ResourceCPUName]
	require.False(t, hasCPU, "zero CPU must be omitted")
	_, hasMemory := resources[constant.ResourceMemoryName]
	require.False(t, hasMemory, "zero memory must be omitted")
}

func TestGRPCFrontendProxyLifecycleClientMarksCreateSourceAsFrontend(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-owner",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		options:  api.InvokeOptions{CreateOpt: map[string]string{"custom": "value"}},
	})

	require.NoError(t, err)
	require.Equal(t, "frontend", fakeService.createReq.Create.CreateOptions["source"])
	require.Equal(t, "value", fakeService.createReq.Create.CreateOptions["custom"])
}

func TestGRPCFrontendProxyLifecycleClientPreservesExplicitCreateSource(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-owner",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		options:  api.InvokeOptions{CreateOpt: map[string]string{"source": "explicit-source"}},
	})

	require.NoError(t, err)
	require.Equal(t, "explicit-source", fakeService.createReq.Create.CreateOptions["source"])
}

func TestGRPCFrontendProxyLifecycleClientBuildsCreateRequestUsesRequestTenantWhenArgsOmitTenant(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create: &core.CreateResponse{
				Code:       common.ErrorCode_ERR_NONE,
				InstanceID: "instance-created",
			},
			RouteAddress: "proxy-owner",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		tenantID: "tenant-from-runtime",
		args: []api.Arg{{
			Type: api.Value,
			Data: []byte("create-arg"),
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, fakeService.createReq)
	require.Equal(t, "tenant-from-runtime", fakeService.createReq.Context.TenantID)
}

func TestConvertSimpleRuntimeCreateArgsPreservesFaaSCodePathsAndValuePrefix(t *testing.T) {
	codePaths := []string{
		"yr.agentexecutor.handler.initialize",
		"yr.agentexecutor.handler.handle",
		"yr.agentexecutor.handler.pre_stop",
	}
	converted := convertSimpleRuntimeCreateArgs(
		api.FunctionMeta{FuncID: "default/executor/$latest", Api: api.FaaSApi},
		[]api.Arg{{Type: api.Value, Data: []byte(`{"handler":"executor"}`)}},
		codePaths,
	)

	require.Len(t, converted, 2)
	require.Equal(t, []byte(simpleRuntimeFaaSMetaPrefix+`{"handler":"executor"}`), converted[1].GetValue())

	metadata := converted[0].GetValue()
	var config []byte
	for len(metadata) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(metadata)
		require.Greater(t, tagSize, 0)
		metadata = metadata[tagSize:]
		if number == metaDataConfigField {
			var valueSize int
			config, valueSize = protowire.ConsumeBytes(metadata)
			require.GreaterOrEqual(t, valueSize, 0)
			break
		}
		fieldSize := protowire.ConsumeFieldValue(number, wireType, metadata)
		require.GreaterOrEqual(t, fieldSize, 0)
		metadata = metadata[fieldSize:]
	}
	require.NotEmpty(t, config)

	actualCodePaths := make([]string, 0, len(codePaths))
	for len(config) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(config)
		require.Greater(t, tagSize, 0)
		config = config[tagSize:]
		if number == metaConfigCodePathsField {
			value, valueSize := protowire.ConsumeBytes(config)
			require.GreaterOrEqual(t, valueSize, 0)
			actualCodePaths = append(actualCodePaths, string(value))
			config = config[valueSize:]
			continue
		}
		fieldSize := protowire.ConsumeFieldValue(number, wireType, config)
		require.GreaterOrEqual(t, fieldSize, 0)
		config = config[fieldSize:]
	}
	require.Equal(t, codePaths, actualCodePaths)
}

func TestGRPCFrontendProxyLifecycleClientCreateInstanceRawReturnsReadyNotify(t *testing.T) {
	callResult := &core.CallResult{
		Code:       common.ErrorCode_ERR_NONE,
		Message:    "ready",
		RequestID:  "internal-result-request",
		InstanceID: "instance-created-raw",
		SmallObjects: []*common.SmallObject{{
			Id:    "ready-object",
			Value: []byte("ready-payload"),
		}},
	}
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: createResponseWithUnknownReadyCallResult(t, callResult),
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-raw")
	createReq := &core.CreateRequest{
		Function:  "tenant-a/raw-function/$latest",
		RequestID: "raw-create-request",
		TraceID:   "trace-raw-create",
	}
	rawCreate, err := proto.Marshal(createReq)
	require.NoError(t, err)

	got, err := client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{
		create:  rawCreate,
		options: api.RawRequestOption{TraceParent: "traceparent-raw"},
	})

	require.NoError(t, err)
	require.Equal(t, "raw-create-request", string(consumeProtoBytesField(t, got, 1)))
	require.NotNil(t, fakeService.createReq)
	require.Equal(t, "frontend-raw", fakeService.createReq.Context.FrontendClientID)
	require.Equal(t, "tenant-a", fakeService.createReq.Context.TenantID)
	require.NotEqual(t, "raw-create-request", fakeService.createReq.Context.RequestID)
	require.Equal(t, fakeService.createReq.Context.RequestID, fakeService.createReq.Create.RequestID)
	require.Equal(t, "trace-raw-create", fakeService.createReq.Context.TraceID)
	require.Equal(t, "frontend", fakeService.createReq.Create.CreateOptions["source"])
	require.Equal(t, "traceparent-raw", fakeService.createReq.Create.CreateOptions[traceParentExtensionKey])
}

func TestExternalRawRequestIDPreservedAcrossInternalCorrelationMapping(t *testing.T) {
	const externalRequestID = "caller-business-request"
	fakeService := &fakeFrontendProxyServiceClient{}
	fakeService.createFn = func(
		_ context.Context, req *frontend_proxy.CreateInstanceRequest,
	) (*frontend_proxy.CreateInstanceResponse, error) {
		return createResponseWithUnknownReadyCallResult(t, &core.CallResult{
			Code:       common.ErrorCode_ERR_NONE,
			RequestID:  req.GetContext().GetRequestID(),
			InstanceID: "instance-external-id",
		}), nil
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-raw")
	rawCreate, err := proto.Marshal(&core.CreateRequest{
		Function:  "tenant-a/raw-function/$latest",
		RequestID: externalRequestID,
	})
	require.NoError(t, err)

	notify, err := client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{create: rawCreate})

	require.NoError(t, err)
	require.NotEqual(t, externalRequestID, fakeService.createReq.GetContext().GetRequestID())
	require.Equal(t, fakeService.createReq.GetContext().GetRequestID(), fakeService.createReq.GetCreate().GetRequestID())
	require.Equal(t, externalRequestID, string(consumeProtoBytesField(t, notify, 1)))
}

func TestRawCreateWithoutCallerRequestIDReturnsGeneratedInternalCorrelation(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{}
	fakeService.createFn = func(
		_ context.Context, req *frontend_proxy.CreateInstanceRequest,
	) (*frontend_proxy.CreateInstanceResponse, error) {
		return createResponseWithUnknownReadyCallResult(t, &core.CallResult{
			Code:       common.ErrorCode_ERR_NONE,
			RequestID:  req.GetContext().GetRequestID(),
			InstanceID: "instance-generated-id",
		}), nil
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-raw")
	rawCreate, err := proto.Marshal(&core.CreateRequest{Function: "tenant-a/raw-function/$latest"})
	require.NoError(t, err)

	notify, err := client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{create: rawCreate})

	require.NoError(t, err)
	internalRequestID := fakeService.createReq.GetContext().GetRequestID()
	require.NotEmpty(t, internalRequestID)
	require.Equal(t, internalRequestID, fakeService.createReq.GetCreate().GetRequestID())
	require.Equal(t, internalRequestID, string(consumeProtoBytesField(t, notify, 1)))
}

func TestCallerRawIDReusedAfterCancelCannotCaptureLateResult(t *testing.T) {
	const externalRequestID = "reused-caller-request"
	internalIDs := make([]string, 0, 2)
	fakeService := &fakeFrontendProxyServiceClient{}
	fakeService.createFn = func(
		_ context.Context, req *frontend_proxy.CreateInstanceRequest,
	) (*frontend_proxy.CreateInstanceResponse, error) {
		internalIDs = append(internalIDs, req.GetContext().GetRequestID())
		return createResponseWithUnknownReadyCallResult(t, &core.CallResult{
			Code:       common.ErrorCode_ERR_NONE,
			RequestID:  req.GetContext().GetRequestID(),
			InstanceID: "instance-reuse",
		}), nil
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-raw")
	rawCreate, err := proto.Marshal(&core.CreateRequest{
		Function:  "tenant-a/raw-function/$latest",
		RequestID: externalRequestID,
	})
	require.NoError(t, err)

	first, err := client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{create: rawCreate})
	require.NoError(t, err)
	second, err := client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{create: rawCreate})
	require.NoError(t, err)

	require.Len(t, internalIDs, testCorrelationCount)
	require.NotEqual(t, internalIDs[0], internalIDs[1], "reused external IDs must never reuse an internal ticket")
	require.Equal(t, externalRequestID, string(consumeProtoBytesField(t, first, 1)))
	require.Equal(t, externalRequestID, string(consumeProtoBytesField(t, second, 1)))
}

func TestRawCreateContextCancellationReachesGRPCClient(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{}
	fakeService.createFn = func(
		ctx context.Context, _ *frontend_proxy.CreateInstanceRequest,
	) (*frontend_proxy.CreateInstanceResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-raw")
	rawCreate, err := proto.Marshal(&core.CreateRequest{Function: "tenant-a/raw-function/$latest"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{ctx: ctx, create: rawCreate})

	require.Equal(t, context.Canceled, err)
	createErr := fakeService.createCtx.Err()
	require.Equal(t, context.Canceled, createErr)
}

func TestMarshalRuntimeNotifyFromCallResultPreservesExternalGwClientInstanceID(t *testing.T) {
	const instanceID = "instance-required-by-external-gwclient"
	got, err := marshalRuntimeNotifyFromCallResult(&core.CallResult{
		Code:       common.ErrorCode_ERR_NONE,
		Message:    "ready",
		RequestID:  "raw-create-request",
		InstanceID: instanceID,
		SmallObjects: []*common.SmallObject{{
			Id:    "ready-object",
			Value: []byte("ready-payload"),
		}},
		StackTraceInfos: []*common.StackTraceInfo{{
			Type:    "UserError",
			Message: "user stack",
		}},
		RuntimeInfo: &common.RuntimeInfo{
			ServerIpAddr: "10.0.0.21",
			ServerPort:   22769,
			Route:        "10.0.0.21:22769",
		},
	})

	require.NoError(t, err)
	require.Equal(t, map[protowire.Number]int{
		1: 1,
		2: 1,
		3: 1,
		4: 1,
		5: 1,
		7: 1,
		8: 1,
	}, consumeProtoFieldCounts(t, got))
	require.Equal(t, instanceID, string(consumeProtoBytesField(t, got, runtimeNotifyInstanceIDField)))
}

func TestGRPCFrontendProxyLifecycleClientRecordsCreatedInstanceRoute(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-created",
		Address:      "10.0.0.21:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()

	function := "tenant/created-function/$latest"
	instanceID := "instance-created-route-cache"
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create: &core.CreateResponse{
				Code:       common.ErrorCode_ERR_NONE,
				InstanceID: instanceID,
			},
			RouteAddress: "proxy-node-created",
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	got, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: function, Api: api.FaaSApi},
		tenantID: "tenant",
	})

	require.NoError(t, err)
	require.Equal(t, instanceID, got)
	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: function, Api: api.FaaSApi},
		instanceID: instanceID,
	})
	require.NoError(t, err)
	require.Equal(t, "10.0.0.21:22769", address)
}

func TestGRPCFrontendProxyLifecycleClientClearsRouteOnlyCacheAfterKill(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-killed",
		Address:      "10.0.0.22:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()

	function := "tenant/killed-function/$latest"
	instanceID := "instance-killed-route-cache"
	instancemanager.RecordRouteOnlyInstance(function, instanceID, "proxy-node-killed")
	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: function, Api: api.FaaSApi},
		instanceID: instanceID,
	})
	require.NoError(t, err)
	require.Equal(t, "10.0.0.22:22769", address)

	fakeService := &fakeFrontendProxyServiceClient{
		killResp: &frontend_proxy.KillInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Kill:   &core.KillResponse{Code: common.ErrorCode_ERR_NONE},
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")
	var event frontendRouteLifecycleEvent
	restoreObserver := setFrontendRouteLifecycleObserverForTest(func(got frontendRouteLifecycleEvent) {
		event = got
	})
	defer restoreObserver()

	err = client.KillInstance(simpleRuntimeKillRequest{ctx: context.Background(),
		instanceID: instanceID,
		tenantID:   "tenant",
		options:    api.InvokeOptions{TraceID: "trace-kill-cleanup"},
	})

	require.NoError(t, err)
	require.Nil(t, instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(instanceID))
	require.True(t, strings.HasPrefix(event.RequestID, "frontend-proxy-kill-"))
	requireRouteLifecycleEvent(t, event, frontendRouteLifecycleEvent{
		Outcome: "success", RequestID: event.RequestID, TraceID: "trace-kill-cleanup",
		InstanceID: instanceID, OwningProxyID: "proxy-node-killed",
	})
}

func TestGRPCFrontendProxyLifecycleClientReturnsCreateError(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create: &core.CreateResponse{
				Code:    common.ErrorCode_ERR_AUTHORIZE_FAILED,
				Message: "authorize failed",
			},
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.Error(t, err)
	require.Equal(t, "authorize failed", err.Error())
}

func TestRoutingFrontendProxyLifecycleClientSelectsCreateCapabilityEndpoint(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-invoke-only", Address: "10.0.0.10:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
		{
			NodeID: "proxy-create", Address: "10.0.0.11:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-create",
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	got, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.NoError(t, err)
	require.Equal(t, "instance-created", got)
	require.Equal(t, "10.0.0.11:22769", factory.address)
	require.NotNil(t, fakeService.createReq)
	require.Equal(t, "frontend-test", fakeService.createReq.Context.FrontendClientID)
}

func TestFrontendProxyTransportCredentialsFollowGlobalComponentMTLS(t *testing.T) {
	original := *config.GetConfig()
	t.Cleanup(func() { config.SetConfig(original) })

	plainConfig := original
	plainConfig.ComponentMTLSEnable = false
	config.SetConfig(plainConfig)
	transportCredentials, err := frontendProxyTransportCredentials()
	require.NoError(t, err)
	require.NotNil(t, transportCredentials)

	mtlsConfig := original
	mtlsConfig.ComponentMTLSEnable = true
	mtlsConfig.HTTPSConfig = nil
	config.SetConfig(mtlsConfig)
	_, err = frontendProxyTransportCredentials()
	require.EqualError(t, err, "frontend HTTPS config is required for component certificate paths")
}

func TestRoutingFrontendProxyLifecycleCreateDoesNotReplayAfterDispatchedError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-create-a", Address: "10.0.0.11:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
		{
			NodeID: "proxy-create-b", Address: "10.0.0.12:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	firstService := &fakeFrontendProxyServiceClient{
		err: status.Error(codes.Unavailable, "create dispatched but transport failed"),
	}
	secondService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create: &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "should-not-be-created"},
		},
	}
	factory := &fakeFrontendProxyClientFactory{clientsByURL: map[string]frontend_proxy.FrontendProxyServiceClient{
		"10.0.0.11:22769": firstService,
		"10.0.0.12:22769": secondService,
	}}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "create dispatched but transport failed")
	require.Equal(t, []string{"10.0.0.11:22769"}, factory.addresses)
	require.Equal(t, []string{"10.0.0.11:22769"}, factory.evicted)
	require.True(t, discovery.IsSuspectAddress("10.0.0.11:22769"))
	require.Equal(t, 1, firstService.calls)
	require.Equal(t, 0, secondService.calls)
}

func TestRoutingFrontendProxyLifecycleCreateRetriesNextCandidateOnControlPathNotWired(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-create-stale", Address: "10.0.0.11:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
		{
			NodeID: "proxy-create-ready", Address: "10.0.0.12:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	staleService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{
				Code:        common.ErrorCode_ERR_INNER_SYSTEM_ERROR,
				Message:     "frontend proxy create requires ready create dispatcher",
				RetryReason: "control-path-not-wired",
			},
		},
	}
	readyService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-create-ready",
		},
	}
	factory := &fakeFrontendProxyClientFactory{clientsByURL: map[string]frontend_proxy.FrontendProxyServiceClient{
		"10.0.0.11:22769": staleService,
		"10.0.0.12:22769": readyService,
	}}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	got, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.NoError(t, err)
	require.Equal(t, "instance-created", got)
	require.Equal(t, []string{"10.0.0.11:22769", "10.0.0.12:22769"}, factory.addresses)
	require.Empty(t, factory.evicted)
	require.Equal(t, 1, staleService.calls)
	require.Equal(t, 1, readyService.calls)
}

func TestRoutingFrontendProxyLifecycleCreateRawSelectsCreateCapabilityEndpoint(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-invoke-only", Address: "10.0.0.10:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
		{
			NodeID: "proxy-create", Address: "10.0.0.11:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	callResult := &core.CallResult{
		Code:       common.ErrorCode_ERR_NONE,
		RequestID:  "raw-create-request",
		InstanceID: "instance-created-raw",
	}
	fakeService := &fakeFrontendProxyServiceClient{
		createResp: createResponseWithUnknownReadyCallResult(t, callResult),
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}
	createReq := &core.CreateRequest{Function: "tenant/raw-function/$latest", RequestID: "raw-create-request"}
	rawCreate, err := proto.Marshal(createReq)
	require.NoError(t, err)

	got, err := client.CreateInstanceRaw(simpleRuntimeRawCreateRequest{create: rawCreate})

	require.NoError(t, err)
	expected, err := marshalRuntimeNotifyFromCallResult(callResult)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	require.Equal(t, "10.0.0.11:22769", factory.address)
	require.NotNil(t, fakeService.createReq)
	require.Equal(t, "frontend-test", fakeService.createReq.Context.FrontendClientID)
}

func TestRoutingFrontendProxyLifecycleCreateDoesNotMarkEndpointSuspectOnCreateBusinessError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-create",
		Address:      "10.0.0.11:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create: &core.CreateResponse{
				Code:    common.ErrorCode_ERR_PARAM_INVALID,
				Message: "invalid create request",
			},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid create request")
	require.Empty(t, factory.evicted)
	require.False(t, discovery.IsSuspectAddress("10.0.0.11:22769"))
	require.Equal(t, 1, fakeService.calls)
}

func TestRoutingFrontendProxyLifecycleCreateRetriesNextCandidateOnClientFactoryError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-create-a", Address: "10.0.0.11:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
		{
			NodeID: "proxy-create-b", Address: "10.0.0.12:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	fakeService := &fakeFrontendProxyServiceClient{
		createResp: &frontend_proxy.CreateInstanceResponse{
			Status:       &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Create:       &core.CreateResponse{Code: common.ErrorCode_ERR_NONE, InstanceID: "instance-created"},
			RouteAddress: "proxy-create-b",
		},
	}
	factory := &fakeFrontendProxyClientFactory{
		client: fakeService,
		errByAddress: map[string]error{
			"10.0.0.11:22769": status.Error(codes.Unavailable, "dial create proxy failed before dispatch"),
		},
	}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	got, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.NoError(t, err)
	require.Equal(t, "instance-created", got)
	require.Equal(t, []string{"10.0.0.11:22769", "10.0.0.12:22769"}, factory.addresses)
	require.Equal(t, []string{"10.0.0.11:22769"}, factory.evicted)
	require.True(t, discovery.IsSuspectAddress("10.0.0.11:22769"))
	require.Equal(t, 1, fakeService.calls)
}

func TestRoutingFrontendProxyLifecycleCreateMarksEndpointSuspectOnClientFactoryError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-create-a",
		Address:      "10.0.0.11:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityCreate: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	factory := &fakeFrontendProxyClientFactory{
		errByAddress: map[string]error{
			"10.0.0.11:22769": status.Error(codes.Unavailable, "dial create proxy failed"),
		},
	}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
	})

	require.Error(t, err)
	require.Equal(t, []string{"10.0.0.11:22769"}, factory.addresses)
	require.Equal(t, []string{"10.0.0.11:22769"}, factory.evicted)
	require.True(t, discovery.IsSuspectAddress("10.0.0.11:22769"))
}

func TestRoutingFrontendProxyLifecycleKillMarksEndpointSuspectOnClientFactoryError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-kill",
		Address:      "10.0.0.21:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityKill: true},
	}})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()
	restoreInstance := addInstanceRouteForTest(t, "func-key", "instance-kill", "proxy-kill")
	defer restoreInstance()

	factory := &fakeFrontendProxyClientFactory{err: status.Error(codes.Unavailable, "dial kill proxy failed")}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	err := client.KillInstance(simpleRuntimeKillRequest{ctx: context.Background(),
		instanceID: "instance-kill",
	})

	require.Error(t, err)
	require.Equal(t, []string{"10.0.0.21:22769"}, factory.evicted)
	require.True(t, discovery.IsSuspectAddress("10.0.0.21:22769"))
}

func TestRoutingFrontendProxyInvokeClientDoesNotMarkEndpointSuspectOnCallResultError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-invoke",
		Address:      "127.0.0.1:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-invoke")

	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{
				Code:    common.ErrorCode_ERR_PARAM_INVALID,
				Message: "user invoke failed",
			},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		resolver:         defaultFrontendProxyRouteResolver{},
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "127.0.0.1:22769"},
		},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "user invoke failed")
	metadata, ok := GetDirectProxyErrorMetadata(err)
	require.True(t, ok)
	require.Equal(t, int(common.ErrorCode_ERR_PARAM_INVALID), metadata.Code)
	require.False(t, metadata.Retryable)
	require.Empty(t, metadata.RetryReason)
	require.Empty(t, factory.evicted)
	require.False(t, discovery.IsSuspectAddress("127.0.0.1:22769"))
	require.Equal(t, 1, fakeService.calls)
}

func TestRoutingFrontendProxyInvokeClientEvictsAddressOnInvokeErrorWithoutRetry(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-invoke",
		Address:      "127.0.0.1:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-invoke")

	fakeService := &fakeFrontendProxyServiceClient{err: status.Error(codes.Unavailable, "transport unavailable")}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		resolver:         defaultFrontendProxyRouteResolver{},
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "127.0.0.1:22769"},
		},
	})

	require.Error(t, err)
	metadata, ok := GetDirectProxyErrorMetadata(err)
	require.True(t, ok)
	require.Equal(t, int(common.ErrorCode_ERR_INNER_SYSTEM_ERROR), metadata.Code)
	require.False(t, metadata.Retryable)
	require.Equal(t, directProxyPostDispatchReason, metadata.RetryReason)
	require.Equal(t, "request failed: rpc error: code = Unavailable desc = transport unavailable",
		err.Error())
	require.NotContains(t, err.Error(), "frontend")
	require.NotContains(t, err.Error(), "proxy")
	require.NotContains(t, err.Error(), "retryable")
	require.NotContains(t, err.Error(), "retryReason")
	require.Equal(t, []string{"127.0.0.1:22769"}, factory.evicted)
	require.Equal(t, 1, fakeService.calls)
}

func TestRoutingFrontendProxyInvokeClientMarksEndpointSuspectOnClientFactoryError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-invoke",
		Address:      "127.0.0.1:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-invoke")

	factory := &fakeFrontendProxyClientFactory{err: status.Error(codes.Unavailable, "dial invoke proxy failed")}
	client := &routingFrontendProxyInvokeClient{
		resolver:         defaultFrontendProxyRouteResolver{},
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "127.0.0.1:22769"},
		},
	})

	require.Error(t, err)
	metadata, ok := GetDirectProxyErrorMetadata(err)
	require.True(t, ok)
	require.Equal(t, int(common.ErrorCode_ERR_INNER_COMMUNICATION), metadata.Code)
	require.True(t, metadata.Retryable)
	require.Equal(t, directProxyPreDispatchReason, metadata.RetryReason)
	require.Equal(t, "request failed: rpc error: code = Unavailable desc = dial invoke proxy failed",
		err.Error())
	require.NotContains(t, err.Error(), "frontend")
	require.NotContains(t, err.Error(), "retryable")
	require.NotContains(t, err.Error(), "retryReason")
	require.Equal(t, []string{"127.0.0.1:22769"}, factory.evicted)
	require.True(t, discovery.IsSuspectAddress("127.0.0.1:22769"))
}

func TestRoutingFrontendProxyInvokeClientResolvesRouteAndInvokesService(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-invoke",
		Address:      "127.0.0.1:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-invoke")

	payload := []byte("route-payload")
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE, Message: string(payload)},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		resolver:         defaultFrontendProxyRouteResolver{},
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	got, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "127.0.0.1:22769"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.Equal(t, "127.0.0.1:22769", factory.address)
	require.Equal(t, "frontend-test", fakeService.req.Context.FrontendClientID)
}

func TestDefaultFrontendProxyRouteResolverResolvesNodeRouteFromDiscovery(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:  "proxy-node-1",
		Address: "10.0.0.8:22769",
		Capabilities: map[string]bool{
			frontendProxyCapabilityInvoke: true,
		},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-node-1")

	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "proxy-node-1"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.8:22769", address)
}

func TestDefaultFrontendProxyRouteResolverUsesInstanceCacheWhenRouteMissing(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{NodeID: "proxy-a", Address: "10.0.0.11:22769", Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true}},
		{NodeID: "proxy-b", Address: "10.0.0.12:22769", Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true}},
	})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()
	restoreInstance := addInstanceRouteForTest(t, "func-key", "instance-owned", "proxy-b")
	defer restoreInstance()
	require.NotNil(t, instancemanager.GetGlobalInstanceScheduler().GetInstanceByID("func-key", "instance-owned"))

	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-owned",
	})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.12:22769", address)
}

func TestDefaultFrontendProxyRouteResolverOnlyRoutesEvictingInstanceForForceInvoke(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID: "proxy-evicting", Address: "10.0.0.13:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()
	restoreInstance := addInstanceRouteForStatusTest(t, "func-key", "instance-evicting",
		"proxy-evicting", constant.KernelInstanceStatusEvicting)
	defer restoreInstance()

	require.Nil(t, instancemanager.GetGlobalInstanceScheduler().
		GetInstanceByID("func-key", "instance-evicting"))
	require.NotNil(t, instancemanager.GetEvictingInstanceByID("instance-evicting"))

	normalCtx, cancelNormal := context.WithTimeout(context.Background(), testRouteResolutionTimeout)
	defer cancelNormal()
	_, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx: normalCtx, instanceID: "instance-evicting",
	})
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx: context.Background(), instanceID: "instance-evicting",
		options: api.InvokeOptions{ForceInvoke: true},
	})
	require.NoError(t, err)
	require.Equal(t, "10.0.0.13:22769", address)
}

func TestDefaultFrontendProxyRouteResolverPrefersInstanceCacheOverRequestRoute(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{NodeID: "proxy-a", Address: "10.0.0.11:22769", Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true}},
		{NodeID: "proxy-b", Address: "10.0.0.12:22769", Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true}},
	})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()
	restoreInstance := addInstanceRouteForTest(t, "func-key", "instance-owned-stale-route", "proxy-b")
	defer restoreInstance()
	require.NotNil(t, instancemanager.GetGlobalInstanceScheduler().GetInstanceByID(
		"func-key", "instance-owned-stale-route",
	))

	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-owned-stale-route",
		options: api.InvokeOptions{CreateOpt: map[string]string{
			frontendProxyRouteKey: "proxy-a",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.12:22769", address)
}

func TestDefaultFrontendProxyRouteResolverSelectsOwningNodeFromMultipleDiscoveryEndpoints(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID:  "proxy-node-a",
			Address: "10.0.0.8:22769",
			Capabilities: map[string]bool{
				frontendProxyCapabilityInvoke: true,
			},
		},
		{
			NodeID:  "proxy-node-b",
			Address: "10.0.0.9:22769",
			Capabilities: map[string]bool{
				frontendProxyCapabilityInvoke: true,
			},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-node-b")

	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "proxy-node-b"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.9:22769", address)
}

func TestDefaultFrontendProxyRouteResolverUsesDefaultDiscoverySnapshot(t *testing.T) {
	oldDiscovery := currentFrontendProxyDiscovery()
	defer setFrontendProxyDiscovery(oldDiscovery)
	defer ReplaceFrontendProxyDiscoverySnapshot(nil)

	ReplaceFrontendProxyDiscoverySnapshot([]FrontendProxyEndpoint{{
		NodeID:  "proxy-node-snapshot",
		Address: "10.0.0.10:22769",
		Capabilities: map[string]bool{
			frontendProxyCapabilityInvoke: true,
		},
	}})
	recordRouteOnlyForTest(t, "instance-1", "proxy-node-snapshot")

	address, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{
			CreateOpt: map[string]string{frontendProxyRouteKey: "proxy-node-snapshot"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.10:22769", address)
}

func TestRoutingFrontendProxyInvokeClientRawEvictsAddressOnInvokeErrorWithoutRetry(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-raw",
		Address:      "127.0.0.1:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-raw")

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "127.0.0.1:22769",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{err: status.Error(codes.Unavailable, "transport unavailable")}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(), invoke: rawReq})

	require.Error(t, err)
	require.Equal(t, []string{"127.0.0.1:22769"}, factory.evicted)
	require.Equal(t, 1, fakeService.calls)
}

func TestRoutingFrontendProxyInvokeClientRawMarksEndpointSuspectOnClientFactoryError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-raw",
		Address:      "127.0.0.1:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-raw")
	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "127.0.0.1:22769",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	factory := &fakeFrontendProxyClientFactory{err: status.Error(codes.Unavailable, "dial raw invoke proxy failed")}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(), invoke: rawReq})

	require.Error(t, err)
	require.Equal(t, []string{"127.0.0.1:22769"}, factory.evicted)
	require.True(t, discovery.IsSuspectAddress("127.0.0.1:22769"))
}

func TestRoutingFrontendProxyInvokeClientRawResolvesNodeRouteFromDiscovery(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:  "proxy-node-raw",
		Address: "10.0.0.9:22769",
		Capabilities: map[string]bool{
			frontendProxyCapabilityInvoke: true,
		},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-node-raw")

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "proxy-node-raw",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(), invoke: rawReq})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.9:22769", factory.address)
	require.Equal(t, "frontend-test", fakeService.req.Context.FrontendClientID)
}

func TestRoutingFrontendProxyInvokeClientRawBackfillsGeneratedRequestID(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-raw",
		Address:      "10.0.0.9:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-node-raw")

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "proxy-node-raw",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(), invoke: rawReq})

	require.NoError(t, err)
	require.NotNil(t, fakeService.req)
	require.NotEmpty(t, fakeService.req.Context.RequestID)
	require.Equal(t, fakeService.req.Context.RequestID, fakeService.req.Invoke.RequestID)
}

func TestRawInvokePreservesExternalRequestIDWithUniqueInternalCorrelation(t *testing.T) {
	const externalRequestID = "external-invoke-request"
	fakeService := &fakeFrontendProxyServiceClient{}
	fakeService.invokeFn = func(
		_ context.Context, req *frontend_proxy.InvokeInstanceRequest,
	) (*frontend_proxy.InvokeInstanceResponse, error) {
		return &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{
				Code:      common.ErrorCode_ERR_NONE,
				RequestID: req.GetContext().GetRequestID(),
			},
		}, nil
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-raw")
	rawInvoke, err := proto.Marshal(&core.InvokeRequest{
		Function:   "tenant/function/$latest",
		InstanceID: "instance-raw",
		RequestID:  externalRequestID,
	})
	require.NoError(t, err)

	notify, err := client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{
		ctx: context.Background(), invoke: rawInvoke,
	})

	require.NoError(t, err)
	require.NotEqual(t, externalRequestID, fakeService.req.GetContext().GetRequestID())
	require.Equal(t, fakeService.req.GetContext().GetRequestID(), fakeService.req.GetInvoke().GetRequestID())
	require.Equal(t, "tenant", fakeService.req.GetContext().GetTenantID())
	require.Equal(t, "tenant/function/$latest", fakeService.req.GetInvoke().GetFunction())
	require.Equal(t, externalRequestID, string(consumeProtoBytesField(t, notify, 1)))
}

func TestRawInvokeWithoutCallerRequestIDReturnsGeneratedInternalCorrelation(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{}
	fakeService.invokeFn = func(
		_ context.Context, req *frontend_proxy.InvokeInstanceRequest,
	) (*frontend_proxy.InvokeInstanceResponse, error) {
		return &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{
				Code:      common.ErrorCode_ERR_NONE,
				RequestID: req.GetContext().GetRequestID(),
			},
		}, nil
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-raw")
	rawInvoke, err := proto.Marshal(&core.InvokeRequest{
		Function:   "tenant/function/$latest",
		InstanceID: "instance-raw",
	})
	require.NoError(t, err)

	notify, err := client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{
		ctx: context.Background(), invoke: rawInvoke,
	})

	require.NoError(t, err)
	internalRequestID := fakeService.req.GetContext().GetRequestID()
	require.NotEmpty(t, internalRequestID)
	require.Equal(t, internalRequestID, fakeService.req.GetInvoke().GetRequestID())
	require.Equal(t, "tenant", fakeService.req.GetContext().GetTenantID())
	require.Equal(t, "tenant/function/$latest", fakeService.req.GetInvoke().GetFunction())
	require.Equal(t, internalRequestID, string(consumeProtoBytesField(t, notify, 1)))
}

func TestRoutingFrontendProxyLifecycleClientRouteStaleRetriesWithRefreshedOwner(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID:       "proxy-stale-owner",
			Address:      "10.0.0.31:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityKill: true, frontendProxyCapabilityInvoke: true},
		},
		{
			NodeID:       "proxy-fresh-owner",
			Address:      "10.0.0.32:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityKill: true, frontendProxyCapabilityInvoke: true},
		},
	})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()

	function := "tenant/route-stale-function/$latest"
	instanceID := "instance-route-stale-kill"
	instancemanager.RecordRouteOnlyInstance(function, instanceID, "proxy-stale-owner")
	defer instancemanager.RemoveRouteOnlyInstance(instanceID)

	staleService := &fakeFrontendProxyServiceClient{
		killResp: &frontend_proxy.KillInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{
				Code:        common.ErrorCode_ERR_INSTANCE_NOT_FOUND,
				Message:     "frontend proxy is not the owning proxy for this instance",
				Retryable:   true,
				RetryReason: "route-stale",
			},
		},
	}
	freshService := &fakeFrontendProxyServiceClient{
		killResp: &frontend_proxy.KillInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Kill:   &core.KillResponse{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{clientsByURL: map[string]frontend_proxy.FrontendProxyServiceClient{
		"10.0.0.31:22769": staleService,
		"10.0.0.32:22769": freshService,
	}}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}
	var events []frontendRouteLifecycleEvent
	restoreObserver := setFrontendRouteLifecycleObserverForTest(func(got frontendRouteLifecycleEvent) {
		events = append(events, got)
		if got.Outcome == "route-stale" {
			instancemanager.RecordRouteOnlyInstance(function, instanceID, "proxy-fresh-owner")
		}
	})
	defer restoreObserver()

	err := client.KillInstance(simpleRuntimeKillRequest{ctx: context.Background(),
		instanceID: instanceID,
		tenantID:   "tenant",
		options:    api.InvokeOptions{TraceID: "trace-kill-route-stale"},
	})

	require.NoError(t, err)
	require.Equal(t, 1, staleService.calls)
	require.Equal(t, 1, freshService.calls)
	require.Equal(t, []string{"10.0.0.31:22769", "10.0.0.32:22769"}, factory.addresses)
	require.Empty(t, factory.evicted, "typed stale-route status must not evict a healthy proxy connection")
	_, resolveErr := proxyrouting.Resolve(
		instanceID, proxyrouting.CapabilityInvoke, proxyrouting.TransportGRPC,
	)
	require.Error(t, resolveErr, "stale route-only owner must be dropped for later fresh resolution")
	require.Len(t, events, 2)
	requireRouteLifecycleEvent(t, events[0], frontendRouteLifecycleEvent{
		Outcome: "route-stale", TraceID: "trace-kill-route-stale",
		InstanceID: instanceID, OwningProxyID: "proxy-stale-owner",
		ReplayAttempted: true,
	})
	requireRouteLifecycleEvent(t, events[1], frontendRouteLifecycleEvent{
		Outcome: "success", RequestID: events[1].RequestID, TraceID: "trace-kill-route-stale",
		InstanceID: instanceID, OwningProxyID: "proxy-fresh-owner",
	})
	require.True(t, strings.HasPrefix(events[1].RequestID, "frontend-proxy-kill-"))
}

func requireRouteLifecycleEvent(t *testing.T, event, expected frontendRouteLifecycleEvent) {
	t.Helper()
	require.Equal(t, "kill", event.Operation)
	require.Equal(t, expected.Outcome, event.Outcome)
	require.Equal(t, "route-hint-cleared", event.CleanupOutcome)
	require.Equal(t, expected.RequestID, event.RequestID)
	require.Equal(t, expected.TraceID, event.TraceID)
	require.Equal(t, expected.InstanceID, event.InstanceID)
	require.Equal(t, expected.OwningProxyID, event.OwningProxyID)
	require.True(t, event.RoutePresentBefore)
	require.False(t, event.RoutePresentAfter)
	require.Equal(t, expected.ReplayAttempted, event.ReplayAttempted)
	encoded, err := json.Marshal(event)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "payload")
}

func setFrontendRouteLifecycleObserverForTest(observer func(frontendRouteLifecycleEvent)) func() {
	frontendRouteLifecycleObserverMu.Lock()
	previous := frontendRouteLifecycleObserver
	frontendRouteLifecycleObserver = observer
	frontendRouteLifecycleObserverMu.Unlock()
	return func() {
		frontendRouteLifecycleObserverMu.Lock()
		frontendRouteLifecycleObserver = previous
		frontendRouteLifecycleObserverMu.Unlock()
	}
}

func TestRoutingFrontendProxyInvokeClientRawPropagatesTraceParent(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-raw",
		Address:      "10.0.0.9:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-node-raw")

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		TraceID:    "trace-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "proxy-node-raw",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(),
		invoke:  rawReq,
		options: api.RawRequestOption{TraceParent: "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01"},
	})

	require.NoError(t, err)
	require.NotNil(t, fakeService.req)
	require.Equal(t, "trace-raw", fakeService.req.Context.TraceID)
	require.Equal(t, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01",
		fakeService.req.Context.Labels[traceParentExtensionKey])
	require.Equal(t, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01",
		fakeService.req.Invoke.InvokeOptions.CustomTag[traceParentExtensionKey])
}

func TestRoutingFrontendProxyInvokeClientRawPrefersCachedOwningRouteOverRequestRoute(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-stale", Address: "10.0.0.8:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
		{
			NodeID: "proxy-owner", Address: "10.0.0.9:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
	})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()
	cleanupInstance := addInstanceRouteForTest(
		t, "tenant/func-key-route-stale/$latest", "instance-route-stale", "proxy-owner",
	)
	defer cleanupInstance()

	invokeReq := &core.InvokeRequest{
		Function:   "tenant/func-key-route-stale/$latest",
		InstanceID: "instance-route-stale",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "proxy-stale",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(), invoke: rawReq})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.9:22769", factory.address)
}

func TestRoutingFrontendProxyInvokeClientRawSelectsOwningNodeFromMultipleDiscoveryEndpoints(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID:  "proxy-node-a",
			Address: "10.0.0.8:22769",
			Capabilities: map[string]bool{
				frontendProxyCapabilityInvoke: true,
			},
		},
		{
			NodeID:  "proxy-node-b",
			Address: "10.0.0.9:22769",
			Capabilities: map[string]bool{
				frontendProxyCapabilityInvoke: true,
			},
		},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-node-b")

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "proxy-node-b",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: context.Background(), invoke: rawReq})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.9:22769", factory.address)
	require.Equal(t, "frontend-test", fakeService.req.Context.FrontendClientID)
}

func TestRoutingFrontendProxyInvokeClientRawDoesNotUseSoleEndpointWithoutOwner(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:  "proxy-node-only",
		Address: "10.0.0.11:22769",
		Capabilities: map[string]bool{
			frontendProxyCapabilityInvoke: true,
		},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: canceledContextForTest(), invoke: rawReq})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Empty(t, factory.address)
}

func TestRoutingFrontendProxyInvokeClientRawSkipsSuspectSoleDiscoveryEndpoint(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-only",
		Address:      "10.0.0.11:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	discovery.MarkSuspectAddress("10.0.0.11:22769", time.Minute)
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-node-only")

	invokeReq := &core.InvokeRequest{Function: "func-key", InstanceID: "instance-raw"}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	factory := &fakeFrontendProxyClientFactory{client: &fakeFrontendProxyServiceClient{}}
	client := &routingFrontendProxyInvokeClient{clientFactory: factory}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: canceledContextForTest(), invoke: rawReq})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Empty(t, factory.address)
}

func TestRoutingFrontendProxyInvokeClientRawDoesNotSwitchAwayFromSuspectOwner(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID:       "proxy-stale",
			Address:      "10.0.0.11:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
		{
			NodeID:       "proxy-healthy",
			Address:      "10.0.0.12:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
	})
	discovery.MarkSuspectAddress("10.0.0.11:22769", time.Minute)
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-raw", "proxy-stale")

	invokeReq := &core.InvokeRequest{
		Function:   "func-key",
		InstanceID: "instance-raw",
		InvokeOptions: &core.InvokeOptions{CustomTag: map[string]string{
			frontendProxyRouteKey: "10.0.0.11:22769",
		}},
	}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status:     &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: canceledContextForTest(), invoke: rawReq})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Empty(t, factory.address)
}

func TestDefaultFrontendProxyRouteResolverRejectsSuspectDirectRoute(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-stale",
		Address:      "10.0.0.11:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	discovery.MarkSuspectAddress("10.0.0.11:22769", time.Minute)
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-suspect-route", "proxy-stale")

	_, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        canceledContextForTest(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-suspect-route",
		options: api.InvokeOptions{CreateOpt: map[string]string{
			frontendProxyRouteKey: "10.0.0.11:22769",
		}},
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestRoutingFrontendProxyInvokeClientRawDoesNotGuessWhenMultipleDiscoveryEndpoints(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{NodeID: "proxy-a", Address: "10.0.0.11:22769", Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true}},
		{NodeID: "proxy-b", Address: "10.0.0.12:22769", Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true}},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	invokeReq := &core.InvokeRequest{Function: "func-key", InstanceID: "instance-raw"}
	rawReq, err := proto.Marshal(invokeReq)
	require.NoError(t, err)
	factory := &fakeFrontendProxyClientFactory{client: &fakeFrontendProxyServiceClient{}}
	client := &routingFrontendProxyInvokeClient{clientFactory: factory}

	_, err = client.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{ctx: canceledContextForTest(), invoke: rawReq})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Empty(t, factory.address)
}

func TestRoutingFrontendProxyInvokeClientDoesNotEvictOrMarkSuspectOnProxyStatusError(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node-only",
		Address:      "10.0.0.11:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-node-only")

	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{
				Code:        common.ErrorCode_ERR_INNER_SYSTEM_ERROR,
				Message:     "runtime failed after dispatch",
				Retryable:   true,
				RetryReason: "route-stale",
			},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyInvokeClient{
		resolver:         defaultFrontendProxyRouteResolver{},
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options: api.InvokeOptions{CreateOpt: map[string]string{
			frontendProxyRouteKey: "10.0.0.11:22769",
		}},
	})

	require.Error(t, err)
	require.Empty(t, factory.evicted)
	_, ok := discovery.GetByNode("proxy-node-only", frontendProxyCapabilityInvoke)
	require.True(t, ok)
	require.Equal(t, 1, fakeService.calls)
}

func TestFrontendProxyDeadlineExceededDoesNotMarkEndpointSuspect(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-node",
		Address:      "10.0.0.11:22769",
		Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
	}})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()

	factory := &fakeFrontendProxyClientFactory{}
	evictFrontendProxyClientOnError(factory, "10.0.0.11:22769",
		status.Error(codes.DeadlineExceeded, "function execution timed out"))

	require.Empty(t, factory.evicted)
	require.False(t, discovery.IsSuspectAddress("10.0.0.11:22769"))
}

func TestRawSimpleRuntimeContextPreservesUnboundedParent(t *testing.T) {
	ctx, cancel := rawSimpleRuntimeContext(context.Background(), api.RawRequestOption{})
	defer cancel()

	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline)
}

func TestGRPCFrontendProxyInvokeClientStatusErrorKeepsRetryHintInternal(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{
				Code:        common.ErrorCode_ERR_INNER_SYSTEM_ERROR,
				Message:     "proxy unavailable",
				Retryable:   true,
				RetryReason: "route-stale",
			},
		},
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{ctx: context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
		options:    api.InvokeOptions{CreateOpt: map[string]string{frontendProxyRouteKey: "127.0.0.1:22769"}},
	})

	require.Error(t, err)
	var statusErr *frontendProxyStatusErr
	require.True(t, errors.As(err, &statusErr))
	require.Equal(t, common.ErrorCode_ERR_INNER_SYSTEM_ERROR, statusErr.code)
	require.Equal(t, "proxy unavailable", statusErr.message)
	require.True(t, statusErr.retryable)
	require.Equal(t, "route-stale", statusErr.retryReason)
	require.Equal(t, "invoke failed, code: ERR_INNER_SYSTEM_ERROR, message: proxy unavailable",
		err.Error())
	require.NotContains(t, err.Error(), "frontend")
	require.NotContains(t, err.Error(), "retryable")
	require.NotContains(t, err.Error(), "retryReason")
	metadata, ok := GetDirectProxyErrorMetadata(err)
	require.True(t, ok)
	require.Equal(t, int(common.ErrorCode_ERR_INNER_SYSTEM_ERROR), metadata.Code)
	require.True(t, metadata.Retryable)
	require.Equal(t, "route-stale", metadata.RetryReason)
}

func TestDirectProxyTransportErrorReportsTimeoutClearly(t *testing.T) {
	tests := []struct {
		name  string
		cause error
	}{
		{name: "context deadline", cause: context.DeadlineExceeded},
		{name: "grpc deadline", cause: status.Error(codes.DeadlineExceeded, "context deadline exceeded")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newDirectProxyPostDispatchError("invoke transport", tt.cause)

			require.EqualError(t, err, "request timed out")
			metadata, ok := GetDirectProxyErrorMetadata(err)
			require.True(t, ok)
			require.False(t, metadata.Retryable)
			require.Equal(t, directProxyPostDispatchReason, metadata.RetryReason)
		})
	}
}

func TestGRPCFrontendProxyInvokeClientKeepsRuntimeFailureOutOfProxyStatus(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			CallResult: &core.CallResult{
				Code:    common.ErrorCode_ERR_INSTANCE_EXITED,
				Message: "runtime exited during invoke",
			},
		},
	}
	client := newGRPCFrontendProxyInvokeClient(fakeService, "frontend-1")

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
	})

	require.Error(t, err)
	var businessErr *frontendProxyBusinessErr
	require.True(t, errors.As(err, &businessErr))
	require.Equal(t, common.ErrorCode_ERR_INSTANCE_EXITED, businessErr.code)
	require.Equal(t, "runtime exited during invoke", err.Error())
	metadata, ok := GetDirectProxyErrorMetadata(err)
	require.True(t, ok)
	require.False(t, metadata.Retryable)
	require.Empty(t, metadata.RetryReason)
}

func TestFrontendProxyBusinessErrorPreservesUpstreamMessage(t *testing.T) {
	for _, operation := range []string{"invoke call result", "create", "kill"} {
		t.Run(operation, func(t *testing.T) {
			err := frontendProxyBusinessError(operation, common.ErrorCode_ERR_INSTANCE_EXITED,
				"upstream business failure")

			require.Equal(t, "upstream business failure", err.Error())
			metadata, ok := GetDirectProxyErrorMetadata(err)
			require.True(t, ok)
			require.Equal(t, int(common.ErrorCode_ERR_INSTANCE_EXITED), metadata.Code)
		})
	}
}

func TestGRPCFrontendProxyInvokeClientMalformedResponseIsPostDispatchFailure(t *testing.T) {
	client := newGRPCFrontendProxyInvokeClient(&fakeFrontendProxyServiceClient{
		resp: &frontend_proxy.InvokeInstanceResponse{},
	}, "frontend-1")

	_, err := client.InvokeByInstanceID(simpleRuntimeInvokeRequest{
		ctx:        context.Background(),
		funcMeta:   api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		instanceID: "instance-1",
	})

	require.Error(t, err)
	metadata, ok := GetDirectProxyErrorMetadata(err)
	require.True(t, ok)
	require.Equal(t, int(common.ErrorCode_ERR_INNER_SYSTEM_ERROR), metadata.Code)
	require.False(t, metadata.Retryable)
	require.Equal(t, directProxyPostDispatchReason, metadata.RetryReason)
}

func TestDefaultFrontendProxyRouteResolverDoesNotBypassCapabilityWithRuntimeAddressFallback(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{{
		NodeID:       "proxy-owner",
		Address:      "10.0.0.9:22773",
		Capabilities: map[string]bool{"sandbox.invoke": true},
	}})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()

	function := "tenant/func-capability/$latest"
	instanceID := "instance-capability"
	recordRouteOnlyForTest(t, instanceID, "proxy-owner")

	_, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        canceledContextForTest(),
		funcMeta:   api.FunctionMeta{FuncID: function, Api: api.FaaSApi},
		instanceID: instanceID,
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestDefaultFrontendProxyRouteResolverRequiresRoute(t *testing.T) {
	_, err := (defaultFrontendProxyRouteResolver{}).ResolveFrontendProxyAddress(simpleRuntimeInvokeRequest{
		ctx:        canceledContextForTest(),
		instanceID: "instance-1",
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestGRPCFrontendProxyLifecycleClientBuildsKillRequest(t *testing.T) {
	fakeService := &fakeFrontendProxyServiceClient{
		killResp: &frontend_proxy.KillInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Kill:   &core.KillResponse{Code: common.ErrorCode_ERR_NONE},
		},
	}
	client := newGRPCFrontendProxyLifecycleClient(fakeService, "frontend-1")

	err := client.KillInstance(simpleRuntimeKillRequest{ctx: context.Background(),
		instanceID: "instance-kill",
		tenantID:   "tenant-kill",
		signal:     testTypedKillSignal,
		payload:    []byte("kill-payload"),
		options: api.InvokeOptions{
			TraceID: "trace-kill",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, fakeService.killReq)
	require.Equal(t, "frontend-1", fakeService.killReq.Context.FrontendClientID)
	require.Equal(t, "tenant-kill", fakeService.killReq.Context.TenantID)
	require.NotEmpty(t, fakeService.killReq.Context.RequestID)
	require.Equal(t, "trace-kill", fakeService.killReq.Context.TraceID)
	require.Equal(t, "instance-kill", fakeService.killReq.Kill.InstanceID)
	require.Equal(t, int32(testTypedKillSignal), fakeService.killReq.Kill.Signal)
	require.Equal(t, []byte("kill-payload"), fakeService.killReq.Kill.Payload)
	require.Equal(t, fakeService.killReq.Context.RequestID, fakeService.killReq.Kill.RequestID)
}

func TestRoutingFrontendProxyLifecycleClientSelectsKillCapabilityEndpoint(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{
			NodeID: "proxy-invoke-only", Address: "10.0.0.10:22769",
			Capabilities: map[string]bool{frontendProxyCapabilityInvoke: true},
		},
		{NodeID: "proxy-kill", Address: "10.0.0.12:22769", Capabilities: map[string]bool{frontendProxyCapabilityKill: true}},
	})
	restore := setFrontendProxyDiscoveryForTest(discovery)
	defer restore()
	recordRouteOnlyForTest(t, "instance-1", "proxy-kill")

	fakeService := &fakeFrontendProxyServiceClient{
		killResp: &frontend_proxy.KillInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Kill:   &core.KillResponse{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	err := client.KillInstance(simpleRuntimeKillRequest{ctx: context.Background(), instanceID: "instance-1"})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.12:22769", factory.address)
	require.NotNil(t, fakeService.killReq)
	require.Equal(t, "frontend-test", fakeService.killReq.Context.FrontendClientID)
}

func TestRoutingFrontendProxyLifecycleClientKillUsesOwningRoute(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{NodeID: "proxy-a", Address: "10.0.0.10:22769", Capabilities: map[string]bool{frontendProxyCapabilityKill: true}},
		{NodeID: "proxy-b", Address: "10.0.0.12:22769", Capabilities: map[string]bool{frontendProxyCapabilityKill: true}},
	})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()
	restoreInstance := addInstanceRouteForTest(t, "func-key", "instance-owned-kill", "proxy-b")
	defer restoreInstance()

	fakeService := &fakeFrontendProxyServiceClient{
		killResp: &frontend_proxy.KillInstanceResponse{
			Status: &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE},
			Kill:   &core.KillResponse{Code: common.ErrorCode_ERR_NONE},
		},
	}
	factory := &fakeFrontendProxyClientFactory{client: fakeService}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	err := client.KillInstance(simpleRuntimeKillRequest{ctx: context.Background(), instanceID: "instance-owned-kill"})

	require.NoError(t, err)
	require.Equal(t, "10.0.0.12:22769", factory.address)
}

func TestRoutingFrontendProxyLifecycleClientKillDoesNotGuessWithoutRouteWhenMultipleEndpoints(t *testing.T) {
	discovery := newMemoryFrontendProxyDiscovery()
	discovery.ReplaceSnapshot([]frontendProxyEndpoint{
		{NodeID: "proxy-a", Address: "10.0.0.10:22769", Capabilities: map[string]bool{frontendProxyCapabilityKill: true}},
		{NodeID: "proxy-b", Address: "10.0.0.12:22769", Capabilities: map[string]bool{frontendProxyCapabilityKill: true}},
	})
	restoreDiscovery := setFrontendProxyDiscoveryForTest(discovery)
	defer restoreDiscovery()

	factory := &fakeFrontendProxyClientFactory{client: &fakeFrontendProxyServiceClient{}}
	client := &routingFrontendProxyLifecycleClient{
		clientFactory:    factory,
		frontendClientID: "frontend-test",
	}

	err := client.KillInstance(simpleRuntimeKillRequest{
		ctx: canceledContextForTest(), instanceID: "missing-instance-route",
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Empty(t, factory.address)
}

func canceledContextForTest() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
