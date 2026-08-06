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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
)

const (
	testKillSignal      = 9
	testTypedKillSignal = 15
	testGetTimeout      = 321
	testRetryTimes      = 3
	testUnknownBackend  = 99
)

type fakeInvokerLibruntime struct {
	invokerLibruntime

	invokeByInstanceIDReq struct {
		funcMeta   api.FunctionMeta
		instanceID string
		args       []api.Arg
		options    api.InvokeOptions
	}
	invokeByFunctionNameReq struct {
		funcMeta api.FunctionMeta
		args     []api.Arg
		options  api.InvokeOptions
	}
	acquireReq struct {
		state    string
		funcMeta api.FunctionMeta
		options  api.InvokeOptions
	}
	releaseReq struct {
		allocation api.InstanceAllocation
		stateID    string
		abnormal   bool
		options    api.InvokeOptions
	}
	createReq struct {
		funcMeta api.FunctionMeta
		args     []api.Arg
		options  api.InvokeOptions
	}
	killReq struct {
		instanceID string
		signal     int
		payload    []byte
		options    api.InvokeOptions
	}
	createRawReq struct {
		payload []byte
		option  api.RawRequestOption
	}
	killRawReq struct {
		payload []byte
		option  api.RawRequestOption
	}

	results       map[string][]byte
	decreasedRefs []string
	decreaseOwner []string
	getTimeout    int
	getEventID    string
	deletedEvent  string
	createRawResp []byte
	killRawResp   []byte
	allocation    api.InstanceAllocation
	health        bool
	dsHealth      bool
	activeMaster  string
}

type fakeFrontendProxyInvokeClient struct {
	req     simpleRuntimeInvokeRequest
	payload []byte
	err     error
}

type fakeFrontendProxyLifecycleClient struct {
	createReq     simpleRuntimeCreateRequest
	rawCreateReq  simpleRuntimeRawCreateRequest
	rawCreateResp []byte
	killReq       simpleRuntimeKillRequest
	instanceID    string
	err           error
}

func (f *fakeFrontendProxyInvokeClient) InvokeByInstanceID(req simpleRuntimeInvokeRequest) ([]byte, error) {
	f.req = req
	return f.payload, f.err
}

func (f *fakeFrontendProxyInvokeClient) InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest) ([]byte, error) {
	return f.payload, f.err
}

func (f *fakeFrontendProxyInvokeClient) UploadFile(_ context.Context, _, _ string,
	_ io.Reader, _ string,
) (*frontend_proxy.FileTransferResponse, error) {
	return nil, f.err
}

func (f *fakeFrontendProxyInvokeClient) DownloadFile(_ context.Context, _, _ string,
	_ int64, _ string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	return nil, f.err
}

func (f *fakeFrontendProxyLifecycleClient) CreateInstance(req simpleRuntimeCreateRequest) (string, error) {
	f.createReq = req
	if f.instanceID != "" {
		return f.instanceID, f.err
	}
	return "lifecycle-instance", f.err
}

func (f *fakeFrontendProxyLifecycleClient) CreateInstanceRaw(req simpleRuntimeRawCreateRequest) ([]byte, error) {
	f.rawCreateReq = req
	if f.rawCreateResp != nil {
		return f.rawCreateResp, f.err
	}
	return []byte("lifecycle-create-notify"), f.err
}

func (f *fakeFrontendProxyLifecycleClient) KillInstance(req simpleRuntimeKillRequest) error {
	f.killReq = req
	return f.err
}

func (f *fakeInvokerLibruntime) InvokeByInstanceId(funcMeta api.FunctionMeta, instanceID string, args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	f.invokeByInstanceIDReq.funcMeta = funcMeta
	f.invokeByInstanceIDReq.instanceID = instanceID
	f.invokeByInstanceIDReq.args = args
	f.invokeByInstanceIDReq.options = invokeOpt
	return "object-1", nil
}

func (f *fakeInvokerLibruntime) InvokeByFunctionName(funcMeta api.FunctionMeta, args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	f.invokeByFunctionNameReq.funcMeta = funcMeta
	f.invokeByFunctionNameReq.args = args
	f.invokeByFunctionNameReq.options = invokeOpt
	return "object-2", nil
}

func (f *fakeInvokerLibruntime) CreateInstance(funcMeta api.FunctionMeta, args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	f.createReq.funcMeta = funcMeta
	f.createReq.args = args
	f.createReq.options = invokeOpt
	return "instance-created", nil
}

func (f *fakeInvokerLibruntime) AcquireInstance(state string, funcMeta api.FunctionMeta,
	acquireOpt api.InvokeOptions,
) (api.InstanceAllocation, error) {
	f.acquireReq.state = state
	f.acquireReq.funcMeta = funcMeta
	f.acquireReq.options = acquireOpt
	if f.allocation.InstanceID != "" {
		return f.allocation, nil
	}
	return api.InstanceAllocation{InstanceID: "instance-acquired", LeaseID: "lease-1"}, nil
}

func (f *fakeInvokerLibruntime) ReleaseInstance(allocation api.InstanceAllocation, stateID string, abnormal bool,
	option api.InvokeOptions,
) {
	f.releaseReq.allocation = allocation
	f.releaseReq.stateID = stateID
	f.releaseReq.abnormal = abnormal
	f.releaseReq.options = option
}

func (f *fakeInvokerLibruntime) Kill(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
	f.killReq.instanceID = instanceID
	f.killReq.signal = signal
	f.killReq.payload = payload
	f.killReq.options = invokeOpt
	return nil
}

func (f *fakeInvokerLibruntime) CreateInstanceRaw(createReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	f.createRawReq.payload = createReqRaw
	f.createRawReq.option = option
	return f.createRawResp, nil
}

func (f *fakeInvokerLibruntime) KillRaw(killReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	f.killRawReq.payload = killReqRaw
	f.killRawReq.option = option
	return f.killRawResp, nil
}

func (f *fakeInvokerLibruntime) GetAsync(objectID string, cb api.GetAsyncCallback) {
	cb(f.results[objectID], nil)
}

func (f *fakeInvokerLibruntime) Get(objectIDs []string, timeoutMs int) ([][]byte, error) {
	f.getTimeout = timeoutMs
	values := make([][]byte, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		values = append(values, f.results[objectID])
	}
	return values, nil
}

func (f *fakeInvokerLibruntime) GDecreaseRef(objectIDs []string, remoteClientID ...string) ([]string, error) {
	f.decreasedRefs = append(f.decreasedRefs, objectIDs...)
	f.decreaseOwner = append(f.decreaseOwner, remoteClientID...)
	return nil, nil
}

func (f *fakeInvokerLibruntime) GetEvent(objectID string, cb api.GetEventCallback) {
	f.getEventID = objectID
	cb(f.results[objectID], nil)
}

func (f *fakeInvokerLibruntime) DeleteGetEventCallback(objectID string) {
	f.deletedEvent = objectID
}

func (f *fakeInvokerLibruntime) IsHealth() bool {
	return f.health
}

func (f *fakeInvokerLibruntime) IsDsHealth() bool {
	return f.dsHealth
}

func (f *fakeInvokerLibruntime) GetActiveMasterAddr() string {
	return f.activeMaster
}

func TestDefaultClientUsesSingleInvokerLibruntimeSeam(t *testing.T) {
	librt := &fakeInvokerLibruntime{results: map[string][]byte{
		"object-1": []byte("invoke-by-id"),
		"object-2": []byte("invoke-by-name"),
	}}
	client := newDefaultClientLibruntime(librt)

	payload, err := client.Invoke(InvokeRequest{
		Function:     "func-key",
		InstanceID:   "instance-1",
		TraceID:      "trace-1",
		TenantID:     "tenant-1",
		Args:         []*api.Arg{{Type: api.Value, Data: []byte("arg")}},
		InvokeTag:    map[string]string{"k": "v"},
		RouteAddress: "proxy-a",
		RetryTimes:   testRetryTimes,
		ForceInvoke:  true,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("invoke-by-id"), payload)
	require.Equal(t, "func-key", librt.invokeByInstanceIDReq.funcMeta.FuncID)
	require.Equal(t, api.FaaSApi, librt.invokeByInstanceIDReq.funcMeta.Api)
	require.Equal(t, "instance-1", librt.invokeByInstanceIDReq.instanceID)
	require.Equal(t, "trace-1", librt.invokeByInstanceIDReq.options.TraceID)
	require.Equal(t, "proxy-a", librt.invokeByInstanceIDReq.options.CreateOpt["YR_ROUTE"])
	require.Equal(t, testRetryTimes, librt.invokeByInstanceIDReq.options.RetryTimes)
	require.True(t, librt.invokeByInstanceIDReq.options.ForceInvoke)
	require.Len(t, librt.invokeByInstanceIDReq.args, 1)
	require.Equal(t, "tenant-1", librt.invokeByInstanceIDReq.args[0].TenantID)

	payload, err = client.InvokeByName(InvokeRequest{Function: "func-key", FuncSig: "sig", BusinessType: "faas"})
	require.NoError(t, err)
	require.Equal(t, []byte("invoke-by-name"), payload)
	require.Equal(t, "func-key", librt.invokeByFunctionNameReq.funcMeta.FuncID)
	require.Equal(t, "sig", librt.invokeByFunctionNameReq.funcMeta.Sig)

	created, err := client.CreateInstanceByLibRt(
		api.FunctionMeta{FuncID: "sandbox-func"},
		[]api.Arg{{Type: api.Value}},
		api.InvokeOptions{TraceID: "trace-create"},
	)
	require.NoError(t, err)
	require.Equal(t, "instance-created", created)
	require.Equal(t, "sandbox-func", librt.createReq.funcMeta.FuncID)
	require.Equal(t, "trace-create", librt.createReq.options.TraceID)

	err = client.KillByLibRt("instance-1", testKillSignal, []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, "instance-1", librt.killReq.instanceID)
	require.Equal(t, testKillSignal, librt.killReq.signal)
	require.Equal(t, []byte("payload"), librt.killReq.payload)
}

func TestClientSimpleRuntimeLocalResultStore(t *testing.T) {
	runtime := newClientSimpleRuntime()
	payload := []byte("payload")
	objectID := runtime.putLocalResult(payload)
	payload[0] = 'P'

	var got []byte
	runtime.GetAsync(objectID, func(result []byte, err error) {
		require.NoError(t, err)
		got = result
	})
	require.Equal(t, []byte("Payload"), got)
	require.Same(t, &payload[0], &got[0])

	runtime.GetAsync(objectID, func(result []byte, err error) {
		require.Error(t, err)
		require.Nil(t, result)
	})

	failed, err := runtime.GDecreaseRef([]string{objectID})
	require.NoError(t, err)
	require.Empty(t, failed)

	_, err = runtime.Get([]string{objectID}, 0)
	require.Error(t, err)
}

func TestClientSimpleRuntimeInvokeByInstanceIDStoresProxyPayload(t *testing.T) {
	proxyPayload := []byte("proxy-payload")
	proxyClient := &fakeFrontendProxyInvokeClient{payload: proxyPayload}
	runtime := newClientSimpleRuntimeWithProxyClient(proxyClient)

	objectID, err := runtime.InvokeByInstanceId(
		api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi},
		"instance-1",
		[]api.Arg{{Type: api.Value, Data: []byte("arg"), TenantID: "tenant-1"}},
		api.InvokeOptions{TraceID: "trace-1", CreateOpt: map[string]string{"YR_ROUTE": "proxy-a"}},
	)

	require.NoError(t, err)
	require.NotEmpty(t, objectID)
	require.Equal(t, "func-key", proxyClient.req.funcMeta.FuncID)
	require.Equal(t, "instance-1", proxyClient.req.instanceID)
	require.Equal(t, "trace-1", proxyClient.req.options.TraceID)
	require.Equal(t, "proxy-a", proxyClient.req.options.CreateOpt["YR_ROUTE"])

	proxyPayload[0] = 'P'
	var got []byte
	runtime.GetAsync(objectID, func(result []byte, err error) {
		require.NoError(t, err)
		got = result
	})
	require.Equal(t, []byte("Proxy-payload"), got)
	require.Same(t, &proxyPayload[0], &got[0])
}

func TestClientSimpleRuntimeKeepsActorAndPosixInvokeOnExplicitLegacyLane(t *testing.T) {
	for name, apiType := range map[string]api.ApiType{"actor": api.ActorApi, "posix": api.PosixApi} {
		t.Run(name, func(t *testing.T) {
			proxyClient := &fakeFrontendProxyInvokeClient{payload: []byte("proxy-payload")}
			control := &fakeInvokerLibruntime{results: map[string][]byte{"object-1": []byte("legacy-result")}}
			runtime := newClientSimpleRuntimeWithProxyClientAndControl(proxyClient, control)

			objectID, err := runtime.InvokeByInstanceId(
				api.FunctionMeta{FuncID: "legacy-func", Api: apiType},
				"instance-1",
				[]api.Arg{{Type: api.Value, Data: []byte("arg")}},
				api.InvokeOptions{TraceID: "trace-legacy"},
			)

			require.NoError(t, err)
			require.Equal(t, "object-1", objectID)
			require.Equal(t, apiType, control.invokeByInstanceIDReq.funcMeta.Api)
			require.Equal(t, "instance-1", control.invokeByInstanceIDReq.instanceID)
			require.Empty(t, proxyClient.req.instanceID)

			var result []byte
			runtime.GetAsync(objectID, func(value []byte, err error) {
				require.NoError(t, err)
				result = value
			})
			require.Equal(t, []byte("legacy-result"), result)
			failed, err := runtime.GDecreaseRef([]string{objectID})
			require.NoError(t, err)
			require.Empty(t, failed)
			require.Equal(t, []string{objectID}, control.decreasedRefs)
		})
	}
}

func TestClientSimpleRuntimeFailsFastForLegacyInvokeWithoutControl(t *testing.T) {
	proxyClient := &fakeFrontendProxyInvokeClient{payload: []byte("proxy-payload")}
	runtime := newClientSimpleRuntimeWithProxyClient(proxyClient)

	objectID, err := runtime.InvokeByInstanceId(
		api.FunctionMeta{FuncID: "actor-func", Api: api.ActorApi}, "instance-1", nil, api.InvokeOptions{})

	require.Empty(t, objectID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires legacy control")
	require.Empty(t, proxyClient.req.instanceID)
}

func TestClientSimpleRuntimeDoesNotRetainConsumedResultTombstones(t *testing.T) {
	runtime := newClientSimpleRuntime()
	for i := 0; i < 10_000; i++ {
		objectID := runtime.putLocalResult([]byte("payload"))
		runtime.GetAsync(objectID, func(_ []byte, err error) { require.NoError(t, err) })
		failed, err := runtime.GDecreaseRef([]string{objectID})
		require.NoError(t, err)
		require.Empty(t, failed)
	}

	require.Empty(t, runtime.results)
}

func TestClientSimpleRuntimePreservesLegacyObjectArgumentsAndRejectsMixedOwnership(t *testing.T) {
	control := &fakeInvokerLibruntime{results: map[string][]byte{"legacy-1": []byte("legacy")}}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	localID := runtime.putLocalResult([]byte("local"))

	values, err := runtime.Get([]string{"legacy-1"}, testGetTimeout)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("legacy")}, values)
	require.Equal(t, testGetTimeout, control.getTimeout)

	failed, err := runtime.GDecreaseRef([]string{"legacy-1"}, "owner-1")
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Equal(t, []string{"owner-1"}, control.decreaseOwner)

	_, err = runtime.Get([]string{localID, "legacy-1"}, 123)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixed local and legacy")
	failed, err = runtime.GDecreaseRef([]string{localID, "legacy-1"})
	require.Error(t, err)
	require.Equal(t, []string{localID, "legacy-1"}, failed)
}

func TestClientSimpleRuntimeRoutesEventsByObjectOwnership(t *testing.T) {
	control := &fakeInvokerLibruntime{results: map[string][]byte{"legacy-event": []byte("event")}}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	localID := runtime.putLocalResult([]byte("local"))

	runtime.GetEvent(localID, func(result []byte, err error) {
		require.Error(t, err)
		require.Nil(t, result)
	})
	runtime.DeleteGetEventCallback(localID)
	require.Empty(t, control.getEventID)
	require.Empty(t, control.deletedEvent)

	runtime.GetEvent("legacy-event", func(result []byte, err error) {
		require.NoError(t, err)
		require.Equal(t, []byte("event"), result)
	})
	runtime.DeleteGetEventCallback("legacy-event")
	require.Equal(t, "legacy-event", control.getEventID)
	require.Equal(t, "legacy-event", control.deletedEvent)
}

func TestClientSimpleRuntimeCreateInstanceRawKeepsControlFallbackBeforeProxy(t *testing.T) {
	proxyClient := &fakeFrontendProxyInvokeClient{payload: []byte("proxy-invoke-response")}
	control := &fakeInvokerLibruntime{createRawResp: []byte("control-create-notify")}
	runtime := newClientSimpleRuntimeWithProxyClientControlAndFallback(proxyClient, control, true)

	got, err := runtime.CreateInstanceRaw([]byte("create-raw"), api.RawRequestOption{TraceParent: "traceparent"})

	require.NoError(t, err)
	require.Equal(t, []byte("control-create-notify"), got)
	require.Equal(t, []byte("create-raw"), control.createRawReq.payload)
	require.Equal(t, "traceparent", control.createRawReq.option.TraceParent)
}

func TestClientSimpleRuntimeCreateInstanceRawUsesLifecycleClientWhenWired(t *testing.T) {
	lifecycle := &fakeFrontendProxyLifecycleClient{rawCreateResp: []byte("proxy-create-notify")}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, nil)
	runtime.lifecycleClient = lifecycle

	got, err := runtime.CreateInstanceRaw([]byte("raw-create-request"), api.RawRequestOption{TraceParent: "traceparent"})

	require.NoError(t, err)
	require.Equal(t, []byte("proxy-create-notify"), got)
	require.Equal(t, []byte("raw-create-request"), lifecycle.rawCreateReq.create)
	require.Equal(t, "traceparent", lifecycle.rawCreateReq.options.TraceParent)
}

func TestClientSimpleRuntimeCreateInstanceRawFailsFastWithoutLifecycleOrControl(t *testing.T) {
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, nil)
	runtime.lifecycleClient = nil

	got, err := runtime.CreateInstanceRaw([]byte("raw-create-request"), api.RawRequestOption{TraceParent: "traceparent"})

	require.Nil(t, got)
	require.Error(t, err)
	require.Contains(t, err.Error(), "raw create requires frontend-proxy lifecycle client")
}

func TestClientSimpleRuntimeCreateInstanceRawDoesNotUseControlFallbackUnlessEnabled(t *testing.T) {
	control := &fakeInvokerLibruntime{createRawResp: []byte("legacy-create-response")}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = nil

	got, err := runtime.CreateInstanceRaw([]byte("raw-create-request"), api.RawRequestOption{TraceParent: "traceparent"})

	require.Nil(t, got)
	require.Error(t, err)
	require.Contains(t, err.Error(), "explicit legacy libruntime control fallback")
	require.Nil(t, control.createRawReq.payload)
}

func TestClientSimpleRuntimeKeepsActorCreateOnExplicitLegacyLane(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	runtime := newClientSimpleRuntimeWithControl(control)

	instanceID, err := runtime.CreateInstance(
		api.FunctionMeta{FuncID: "actor-func", Api: api.ActorApi},
		[]api.Arg{{Type: api.Value, Data: []byte("arg")}},
		api.InvokeOptions{TraceID: "trace-actor-create"},
	)

	require.NoError(t, err)
	require.Equal(t, "instance-created", instanceID)
	require.Equal(t, "actor-func", control.createReq.funcMeta.FuncID)
	require.Equal(t, api.ActorApi, control.createReq.funcMeta.Api)
	require.Equal(t, "trace-actor-create", control.createReq.options.TraceID)
}

func TestClientSimpleRuntimeUsesLifecycleClientForFaaSCreateWhenWired(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	lifecycle := &fakeFrontendProxyLifecycleClient{instanceID: "new-faas-instance"}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = lifecycle

	instanceID, err := runtime.CreateInstance(
		api.FunctionMeta{FuncID: "faas-func", Api: api.FaaSApi},
		[]api.Arg{{Type: api.Value, Data: []byte("arg")}},
		api.InvokeOptions{TraceID: "trace-faas-create"},
	)

	require.NoError(t, err)
	require.Equal(t, "new-faas-instance", instanceID)
	require.Equal(t, "faas-func", lifecycle.createReq.funcMeta.FuncID)
	require.Equal(t, api.FaaSApi, lifecycle.createReq.funcMeta.Api)
	require.Equal(t, "trace-faas-create", lifecycle.createReq.options.TraceID)
	require.Len(t, lifecycle.createReq.args, 1)
	require.Empty(t, control.createReq.funcMeta.FuncID)
}

func TestClientSimpleRuntimeUsesLifecycleClientForServeCreateWhenWired(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	lifecycle := &fakeFrontendProxyLifecycleClient{instanceID: "new-serve-instance"}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = lifecycle

	instanceID, err := runtime.CreateInstance(
		api.FunctionMeta{FuncID: "serve-func", Api: api.ServeApi},
		[]api.Arg{{Type: api.Value, Data: []byte("arg")}},
		api.InvokeOptions{TraceID: "trace-serve-create"},
	)

	require.NoError(t, err)
	require.Equal(t, "new-serve-instance", instanceID)
	require.Equal(t, "serve-func", lifecycle.createReq.funcMeta.FuncID)
	require.Equal(t, api.ServeApi, lifecycle.createReq.funcMeta.Api)
	require.Equal(t, "trace-serve-create", lifecycle.createReq.options.TraceID)
	require.Empty(t, control.createReq.funcMeta.FuncID)
}

func TestClientSimpleRuntimeKillRawUsesLifecycleClientWhenFrontendProxyBackendEnabled(t *testing.T) {
	control := &fakeInvokerLibruntime{killRawResp: []byte("legacy-kill-response")}
	lifecycle := &fakeFrontendProxyLifecycleClient{}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = lifecycle
	require.NoError(t, runtime.SetTenantID("tenant-raw-kill"))
	killReq, err := proto.Marshal(&core.KillRequest{
		InstanceID: "instance-raw-kill",
		Signal:     testKillSignal,
		Payload:    []byte("kill-payload"),
		RequestID:  "raw-kill-request",
	})
	require.NoError(t, err)

	traceParent := "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01"
	got, err := runtime.KillRaw(killReq, api.RawRequestOption{TraceParent: traceParent})

	require.NoError(t, err)
	killResp := &core.KillResponse{}
	require.NoError(t, proto.Unmarshal(got, killResp))
	require.Equal(t, common.ErrorCode_ERR_NONE, killResp.GetCode())
	require.Equal(t, "instance-raw-kill", lifecycle.killReq.instanceID)
	require.Equal(t, "tenant-raw-kill", lifecycle.killReq.tenantID)
	require.Equal(t, testKillSignal, lifecycle.killReq.signal)
	require.Equal(t, []byte("kill-payload"), lifecycle.killReq.payload)
	require.NotEqual(t, "raw-kill-request", lifecycle.killReq.requestID)
	require.Contains(t, lifecycle.killReq.requestID, "frontend-proxy-kill-")
	require.Equal(t, "123e4567e89b12d3a456426614174000", lifecycle.killReq.options.TraceID)
	require.Equal(t, traceParent, lifecycle.killReq.options.CustomExtensions[traceParentExtensionKey])
	require.Nil(t, control.killRawReq.payload)
}

func TestTraceIDFromTraceParentRejectsInvalidOrZeroTrace(t *testing.T) {
	require.Empty(t, traceIDFromTraceParent("not-a-traceparent"))
	require.Empty(t, traceIDFromTraceParent("00-00000000000000000000000000000000-0123456789abcdef-01"))
	require.Empty(t, traceIDFromTraceParent("00-not-hex-not-hex-not-hex-not-hex-0123456789abcdef-01"))
}

func TestClientSimpleRuntimeKillRawContextPropagatesCancellation(t *testing.T) {
	lifecycle := &fakeFrontendProxyLifecycleClient{}
	runtime := newClientSimpleRuntimeWithProxyClient(nil)
	runtime.lifecycleClient = lifecycle
	killReq, err := proto.Marshal(&core.KillRequest{InstanceID: "instance-raw-kill", RequestID: "external-kill"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = runtime.KillRawContext(ctx, killReq, api.RawRequestOption{})

	require.NoError(t, err)
	require.Equal(t, context.Canceled, lifecycle.killReq.ctx.Err())
}

func TestClientSimpleRuntimeKillRawDoesNotUseControlFallbackUnlessEnabled(t *testing.T) {
	control := &fakeInvokerLibruntime{killRawResp: []byte("legacy-kill-response")}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = nil
	killReq, err := proto.Marshal(&core.KillRequest{
		InstanceID: "instance-raw-kill",
		Signal:     testKillSignal,
		RequestID:  "raw-kill-request",
	})
	require.NoError(t, err)

	got, err := runtime.KillRaw(killReq, api.RawRequestOption{TraceParent: "traceparent-kill"})

	require.Nil(t, got)
	require.Error(t, err)
	require.Contains(t, err.Error(), "KillRaw")
	require.Nil(t, control.killRawReq.payload)
}

func TestClientSimpleRuntimeKeepsCreateKillOnControlFallback(t *testing.T) {
	control := &fakeInvokerLibruntime{
		createRawResp: []byte("create-notify"),
		killRawResp:   []byte("kill-response"),
	}
	runtime := newClientSimpleRuntimeWithProxyClientControlAndFallback(
		nil,
		control,
		true,
	)

	instanceID, err := runtime.CreateInstance(
		api.FunctionMeta{FuncID: "func-key"},
		[]api.Arg{{Type: api.Value, Data: []byte("arg")}},
		api.InvokeOptions{TraceID: "trace-create"},
	)
	require.NoError(t, err)
	require.Equal(t, "instance-created", instanceID)
	require.Equal(t, "func-key", control.createReq.funcMeta.FuncID)
	require.Equal(t, "trace-create", control.createReq.options.TraceID)

	createResp, err := runtime.CreateInstanceRaw([]byte("create-raw"), api.RawRequestOption{TraceParent: "traceparent"})
	require.NoError(t, err)
	require.Equal(t, []byte("create-notify"), createResp)
	require.Equal(t, []byte("create-raw"), control.createRawReq.payload)
	require.Equal(t, "traceparent", control.createRawReq.option.TraceParent)

	err = runtime.Kill(
		"instance-1", testKillSignal, []byte("payload"), api.InvokeOptions{TraceID: "trace-kill"},
	)
	require.NoError(t, err)
	require.Equal(t, "instance-1", control.killReq.instanceID)
	require.Equal(t, "trace-kill", control.killReq.options.TraceID)

	killResp, err := runtime.KillRaw([]byte("kill-raw"), api.RawRequestOption{TraceParent: "kill-traceparent"})
	require.NoError(t, err)
	require.Equal(t, []byte("kill-response"), killResp)
	require.Equal(t, []byte("kill-raw"), control.killRawReq.payload)
	require.Equal(t, "kill-traceparent", control.killRawReq.option.TraceParent)
}

func TestClientSimpleRuntimeKeepsAcquireReleaseOnControlFallback(t *testing.T) {
	control := &fakeInvokerLibruntime{
		allocation: api.InstanceAllocation{
			FuncKey:       "func-key",
			FuncSig:       "sig",
			InstanceID:    "instance-acquired",
			LeaseID:       "lease-1",
			RouteAddress:  "proxy-a",
			LeaseInterval: 60,
		},
	}
	runtime := newClientSimpleRuntimeWithProxyClientControlAndFallback(nil, control, true)

	allocation, err := runtime.AcquireInstance("state-1", api.FunctionMeta{FuncID: "func-key"},
		api.InvokeOptions{TraceID: "trace-acquire"})

	require.NoError(t, err)
	require.Equal(t, "instance-acquired", allocation.InstanceID)
	require.Equal(t, "state-1", control.acquireReq.state)
	require.Equal(t, "func-key", control.acquireReq.funcMeta.FuncID)
	require.Equal(t, "trace-acquire", control.acquireReq.options.TraceID)

	runtime.ReleaseInstance(allocation, "state-1", true, api.InvokeOptions{TraceID: "trace-release"})

	require.Equal(t, "instance-acquired", control.releaseReq.allocation.InstanceID)
	require.Equal(t, "state-1", control.releaseReq.stateID)
	require.True(t, control.releaseReq.abnormal)
	require.Equal(t, "trace-release", control.releaseReq.options.TraceID)
}

func TestClientSimpleRuntimeKeepsInvokeByFunctionNameOnControlFallback(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	runtime := newClientSimpleRuntimeWithProxyClientControlAndFallback(nil, control, true)

	objectID, err := runtime.InvokeByFunctionName(api.FunctionMeta{FuncID: "func-key"},
		[]api.Arg{{Type: api.Value, Data: []byte("arg")}}, api.InvokeOptions{TraceID: "trace-by-name"})

	require.NoError(t, err)
	require.Equal(t, "object-2", objectID)
	require.Equal(t, "func-key", control.invokeByFunctionNameReq.funcMeta.FuncID)
	require.Equal(t, "trace-by-name", control.invokeByFunctionNameReq.options.TraceID)
	require.Len(t, control.invokeByFunctionNameReq.args, 1)
}

func TestClientSimpleRuntimeKeepsHealthAndMasterOnControlFallback(t *testing.T) {
	control := &fakeInvokerLibruntime{
		health:       true,
		dsHealth:     true,
		activeMaster: "127.0.0.1:26001",
	}
	runtime := newClientSimpleRuntimeWithControl(control)

	require.True(t, runtime.IsHealth())
	require.True(t, runtime.IsDsHealth())
	require.Equal(t, "127.0.0.1:26001", runtime.GetActiveMasterAddr())
}

func TestDefaultClientIsDsHealthUsesRuntimeDsHealth(t *testing.T) {
	librt := &fakeInvokerLibruntime{
		health:   false,
		dsHealth: true,
	}
	client := newDefaultClientLibruntime(librt)

	require.True(t, client.IsDsHealth())
}

func TestClientSimpleRuntimeNoReturnUnsupportedMethodsDoNotPanic(t *testing.T) {
	runtime := newClientSimpleRuntime()

	require.NotPanics(t, func() {
		runtime.ReleaseInstance(api.InstanceAllocation{InstanceID: "instance-1"}, "", true, api.InvokeOptions{})
	})
	require.NotPanics(t, func() {
		runtime.Exit(1, "unsupported")
	})
}

func TestClientSimpleRuntimeDoesNotUseControlFallbackUnlessEnabled(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	runtime := newClientSimpleRuntimeWithControl(control)

	_, err := runtime.CreateInstance(api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi}, nil, api.InvokeOptions{})
	require.Error(t, err)
	require.Empty(t, control.createReq.funcMeta.FuncID)

	_, err = runtime.AcquireInstance(
		"state-1", api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi}, api.InvokeOptions{},
	)
	require.Error(t, err)
	require.Empty(t, control.acquireReq.funcMeta.FuncID)

	_, err = runtime.InvokeByFunctionName(api.FunctionMeta{FuncID: "func-key", Api: api.FaaSApi}, nil, api.InvokeOptions{})
	require.Error(t, err)
	require.Empty(t, control.invokeByFunctionNameReq.funcMeta.FuncID)

	_, err = runtime.CreateInstance(api.FunctionMeta{FuncID: "serve-func", Api: api.ServeApi}, nil, api.InvokeOptions{})
	require.Error(t, err)
	require.Empty(t, control.createReq.funcMeta.FuncID)
}

func TestClientSimpleRuntimeKeepsUntypedKillOnLegacyLane(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	runtime := newClientSimpleRuntimeWithControl(control)

	err := runtime.Kill("instance-1", testKillSignal, nil, api.InvokeOptions{})
	require.NoError(t, err)
	require.Equal(t, "instance-1", control.killReq.instanceID)
}

func TestDefaultClientKillInstanceUsesLifecycleClientForFaaS(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	lifecycle := &fakeFrontendProxyLifecycleClient{}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = lifecycle
	require.NoError(t, runtime.SetTenantID("tenant-kill"))
	client := newDefaultClientLibruntime(runtime)

	err := client.KillInstance(
		api.FunctionMeta{FuncID: "faas-func", Api: api.FaaSApi},
		"instance-faas",
		testTypedKillSignal,
		[]byte("payload"),
		api.InvokeOptions{TraceID: "trace-kill-typed"},
	)

	require.NoError(t, err)
	require.Equal(t, "instance-faas", lifecycle.killReq.instanceID)
	require.Equal(t, testTypedKillSignal, lifecycle.killReq.signal)
	require.Equal(t, []byte("payload"), lifecycle.killReq.payload)
	require.Equal(t, "trace-kill-typed", lifecycle.killReq.options.TraceID)
	require.Equal(t, "tenant-kill", lifecycle.killReq.tenantID)
	require.Empty(t, control.killReq.instanceID)
}

func TestClientSimpleRuntimeKillInstanceFaaSDoesNotUseControlFallbackWithoutLifecycle(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = nil

	err := runtime.KillInstance(
		api.FunctionMeta{FuncID: "faas-func", Api: api.FaaSApi},
		"instance-faas",
		testTypedKillSignal,
		[]byte("payload"),
		api.InvokeOptions{TraceID: "trace-kill-typed"},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "KillInstance")
	require.Empty(t, control.killReq.instanceID)
}

func TestClientSimpleRuntimeKillInstanceServeDoesNotUseControlFallbackWithoutLifecycle(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = nil

	err := runtime.KillInstance(
		api.FunctionMeta{FuncID: "serve-func", Api: api.ServeApi},
		"instance-serve",
		testTypedKillSignal,
		[]byte("payload"),
		api.InvokeOptions{TraceID: "trace-kill-serve"},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "KillInstance")
	require.Empty(t, control.killReq.instanceID)
}

func TestDefaultClientCreateInstanceUsesLifecycleClientForFaaS(t *testing.T) {
	control := &fakeInvokerLibruntime{}
	lifecycle := &fakeFrontendProxyLifecycleClient{instanceID: "new-faas-instance"}
	runtime := newClientSimpleRuntimeWithProxyClientAndControl(nil, control)
	runtime.lifecycleClient = lifecycle
	require.NoError(t, runtime.SetTenantID("tenant-create"))
	client := newDefaultClientLibruntime(runtime)

	instanceID, err := client.CreateRuntimeInstance(
		api.FunctionMeta{FuncID: "faas-func", Api: api.FaaSApi},
		[]api.Arg{{Type: api.Value, Data: []byte("arg")}},
		api.InvokeOptions{TraceID: "trace-create-typed"},
	)

	require.NoError(t, err)
	require.Equal(t, "new-faas-instance", instanceID)
	require.Equal(t, "faas-func", lifecycle.createReq.funcMeta.FuncID)
	require.Equal(t, api.FaaSApi, lifecycle.createReq.funcMeta.Api)
	require.Equal(t, "trace-create-typed", lifecycle.createReq.options.TraceID)
	require.Equal(t, "tenant-create", lifecycle.createReq.tenantID)
	require.Len(t, lifecycle.createReq.args, 1)
	require.Empty(t, control.createReq.funcMeta.FuncID)
}

func TestSetAPIClientDirectRuntimePreservesLibruntimeControlPath(t *testing.T) {
	original := clientLibruntime
	t.Cleanup(func() {
		clientLibruntime = original
	})

	control := &fakeInvokerLibruntime{}
	SetAPIClientDirectRuntime(control)
	runtime, ok := clientLibruntime.(*clientSimpleRuntime)
	require.True(t, ok)
	require.Same(t, control, runtime.control)
	require.NotNil(t, runtime.lifecycleClient)
	require.True(t, runtime.legacyFallback)

	_, err := runtime.CreateInstance(api.FunctionMeta{FuncID: "faas-create", Api: api.FaaSApi}, nil,
		api.InvokeOptions{})
	require.NoError(t, err)
	require.Equal(t, "faas-create", control.createReq.funcMeta.FuncID)
}

func TestSetAPIClientRuntimeBackendDefaultsToLibruntimeWithoutResettingDiscovery(t *testing.T) {
	original := clientLibruntime
	t.Cleanup(func() {
		clientLibruntime = original
		resetFrontendProxyDiscovery()
	})
	ReplaceFrontendProxyDiscoverySnapshot([]FrontendProxyEndpoint{{
		NodeID: "proxy-node-ssh", TCPTunnelAddress: "10.0.0.12:22775",
		Capabilities: map[string]bool{proxyCapabilityTCPTunnel: true}, Health: "healthy",
	}})
	control := &fakeInvokerLibruntime{}

	require.NoError(t, SetAPIClientRuntimeBackend(constant.BackendTypeKernel, control))
	require.Same(t, control, clientLibruntime)
	address, ok := LookupProxyTCPTunnelAddress("proxy-node-ssh")
	require.True(t, ok)
	require.Equal(t, "10.0.0.12:22775", address)
}

func TestSetAPIClientRuntimeBackendEnablesDirectInvokeExplicitly(t *testing.T) {
	original := clientLibruntime
	t.Cleanup(func() {
		clientLibruntime = original
	})

	require.NoError(t, SetAPIClientRuntimeBackend(
		constant.BackendTypeFrontendProxy, &fakeInvokerLibruntime{}))
	_, ok := clientLibruntime.(*clientSimpleRuntime)
	require.True(t, ok)
	require.Error(t, SetAPIClientRuntimeBackend(testUnknownBackend, nil))
}

func TestSetAPIClientDirectRuntimeUsesEtcdBackedFrontendProxyDiscovery(t *testing.T) {
	original := clientLibruntime
	t.Cleanup(func() {
		clientLibruntime = original
		resetFrontendProxyDiscovery()
	})
	ReplaceFrontendProxyDiscoverySnapshot([]FrontendProxyEndpoint{{
		NodeID: "proxy-node-a", Address: "10.0.0.11:19090",
		Capabilities: map[string]bool{frontendProxyCapabilityCreate: true}, Health: "healthy",
	}})

	SetAPIClientDirectRuntime(&fakeInvokerLibruntime{})

	endpoint, ok := resolveNextFrontendProxyEndpoint(frontendProxyCapabilityCreate)

	require.True(t, ok)
	require.Equal(t, "proxy-node-a", endpoint.NodeID)
	require.Equal(t, "10.0.0.11:19090", endpoint.Address)
}
