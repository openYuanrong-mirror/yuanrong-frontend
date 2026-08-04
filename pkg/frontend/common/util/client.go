/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

// Package util -
package util

import (
	"bytes"
	"context"
	"fmt"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/frontend/common/httpconstant"
)

const (
	maxInvokeRetries        = 5
	traceParentExtensionKey = "traceparent"
)

type invokerLibruntime interface {
	CreateInstance(funcMeta api.FunctionMeta, args []api.Arg,
		invokeOpt api.InvokeOptions) (instanceID string, err error)
	InvokeByInstanceId(funcMeta api.FunctionMeta, instanceID string, args []api.Arg,
		invokeOpt api.InvokeOptions) (returnObjectID string, err error)
	InvokeByFunctionName(funcMeta api.FunctionMeta, args []api.Arg,
		invokeOpt api.InvokeOptions) (returnObjectID string, err error)
	AcquireInstance(state string, funcMeta api.FunctionMeta,
		acquireOpt api.InvokeOptions) (api.InstanceAllocation, error)

	ReleaseInstance(allocation api.InstanceAllocation, stateID string, abnormal bool, option api.InvokeOptions)
	Kill(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) (err error)

	CreateInstanceRaw(createReqRaw []byte, option api.RawRequestOption) (createRespRaw []byte, err error)
	InvokeByInstanceIdRaw(invokeReqRaw []byte, option api.RawRequestOption) (resultRaw []byte, err error)
	KillRaw(killReqRaw []byte, option api.RawRequestOption) (killRespRaw []byte, err error)

	SaveState(state []byte) (stateID string, err error)
	LoadState(checkpointID string) (state []byte, err error)

	Exit(code int, message string)

	KVSet(key string, value []byte, param api.SetParam) (err error)
	KVSetWithoutKey(value []byte, param api.SetParam) (key string, err error)
	KVGet(key string, timeoutms uint) (value []byte, err error)
	KVGetMulti(keys []string, timeoutms uint) (values [][]byte, err error)
	KVDel(key string) (err error)
	KVDelMulti(keys []string) (failedKeys []string, err error)

	SetTraceID(traceID string)

	Put(objectID string, value []byte, param api.PutParam, nestedObjectIDs ...string) (err error)
	Get(objectIDs []string, timeoutMs int) (data [][]byte, err error)
	GIncreaseRef(objectIDs []string, remoteClientID ...string) (failedIDs []string, err error)
	GDecreaseRef(objectIDs []string, remoteClientID ...string) (failedIDs []string, err error)
	GetAsync(objectID string, cb api.GetAsyncCallback)
	GetEvent(objectID string, cb api.GetEventCallback)
	DeleteGetEventCallback(objectID string)

	GetFormatLogger() api.FormatLogger
	GetCredential() api.Credential
	SetTenantID(tenantID string) error
	IsHealth() bool
	IsDsHealth() bool
	GetActiveMasterAddr() string
}

var clientLibruntime invokerLibruntime

// SetAPIClientLibruntime set the client provided by the runtime
func SetAPIClientLibruntime(rt invokerLibruntime) {
	clientLibruntime = rt
}

// SetAPIClientRuntimeBackend selects the ordinary function invocation path.
// Proxy discovery remains independent because SSH tunnel routing also consumes it.
func SetAPIClientRuntimeBackend(backendType int, rt invokerLibruntime) error {
	switch backendType {
	case constant.BackendTypeKernel:
		SetAPIClientLibruntime(rt)
	case constant.BackendTypeFrontendProxy:
		SetAPIClientDirectRuntime(rt)
	default:
		return fmt.Errorf("unsupported function invoke backend %d", backendType)
	}
	return nil
}

// SetAPIClientDirectRuntime routes ordinary FaaS invoke calls directly
// to function_proxy while preserving libruntime for lifecycle and legacy APIs.
func SetAPIClientDirectRuntime(rt invokerLibruntime) {
	clientLibruntime = newClientSimpleRuntimeWithProxyClientControlAndFallback(
		newRoutingFrontendProxyInvokeClient(), rt, true)
	UseFrontendProxyDiscoveryCache()
}

// InvokeRequest -
type InvokeRequest struct {
	Function         string
	InstanceID       string
	TraceID          string
	TraceParent      string
	Args             []*api.Arg
	SchedulerID      string
	SchedulerFuncKey string
	RequestID        string
	FuncSig          string
	PoolLabel        string
	InstanceSession  *types.InstanceSessionConfig
	InvokeTag        map[string]string
	InstanceLabel    string
	ReturnObjectIDs  []string
	ResourceSpecs    map[string]int64
	AcquireTimeout   int64
	InvokeTimeout    int64
	TrafficLimited   bool
	RetryTimes       int
	BusinessType     string
	TenantID         string
	AcceptHeader     string
	RouteAddress     string
	BypassDataSystem bool
	ForceInvoke      bool
	IsInterrupted    bool
	SessionCtxID     string
	types.ResponseWriter
}

// SSEChan -
type SSEChan struct {
	Event    chan sseEvent
	EventErr error
	// WaitEvent 用于通知sse消息处理结束，防止主流程和getEvent回调阻塞等待
	WaitEvent chan struct{}
}

type sseEvent struct {
	Data []byte
	Err  error
}

// Client is used to invoke an instance and wait for its response
type Client interface {
	AcquireInstance(functionKey string, req types.AcquireOption) (*types.InstanceAllocationInfo, error)
	ReleaseInstance(allocation *types.InstanceAllocationInfo, abnormal bool)
	Invoke(req InvokeRequest) ([]byte, error)
	InvokeByName(req InvokeRequest) ([]byte, error)
	CreateInstanceRaw(createReq []byte, option api.RawRequestOption) ([]byte, error)
	InvokeInstanceRaw(invokeReq []byte, option api.RawRequestOption) ([]byte, error)
	KillRaw(killReq []byte, option api.RawRequestOption) ([]byte, error)
	CreateRuntimeInstance(funcMeta api.FunctionMeta, args []api.Arg,
		invokeOpt api.InvokeOptions) (instanceID string, err error)
	CreateInstanceByLibRt(funcMeta api.FunctionMeta, args []api.Arg,
		invokeOpt api.InvokeOptions) (instanceID string, err error)
	InvokeInstanceByLibRtAndGet(funcMeta api.FunctionMeta, instanceID string, args []api.Arg,
		invokeOpt api.InvokeOptions) ([]byte, error)
	KillInstance(funcMeta api.FunctionMeta, instanceID string, signal int, payload []byte,
		invokeOpt api.InvokeOptions) (err error)
	KillByLibRt(instanceID string, signal int, payload []byte) (err error)
	IsHealth() bool
	IsDsHealth() bool
	GetActiveMasterAddr() string
}

// NewClient return a client used to invoke other functions
func NewClient() Client {
	return newDefaultClientLibruntime(clientLibruntime)
}

func newDefaultClientLibruntime(librtcli invokerLibruntime) *defaultClient {
	return &defaultClient{clientLibruntime: librtcli}
}

type defaultClient struct {
	clientLibruntime invokerLibruntime
}

func (c *defaultClient) AcquireInstance(functionKey string, req types.AcquireOption) (
	*types.InstanceAllocationInfo, error,
) {
	var err error
	var instanceAllocation api.InstanceAllocation
	functionMeta := api.FunctionMeta{
		FuncID: functionKey,
		Sig:    req.FuncSig,
		Name:   &req.DesignateInstanceID,
		Api:    api.FaaSApi,
	}
	option := convertAcquireOption(req)
	if instanceAllocation, err = c.clientLibruntime.AcquireInstance("", functionMeta, option); err != nil {
		return nil, err
	}
	return &types.InstanceAllocationInfo{
		FuncKey:         instanceAllocation.FuncKey,
		FuncSig:         instanceAllocation.FuncSig,
		InstanceID:      instanceAllocation.InstanceID,
		ThreadID:        instanceAllocation.LeaseID,
		FunctionProxyID: instanceAllocation.RouteAddress,
		LeaseInterval:   instanceAllocation.LeaseInterval,
	}, nil
}

func (c *defaultClient) ReleaseInstance(allocation *types.InstanceAllocationInfo, abnormal bool) {
	instanceAllocation := api.InstanceAllocation{
		FuncKey:       allocation.FuncKey,
		FuncSig:       allocation.FuncSig,
		InstanceID:    allocation.InstanceID,
		LeaseID:       allocation.ThreadID,
		LeaseInterval: allocation.LeaseInterval,
	}
	c.clientLibruntime.ReleaseInstance(instanceAllocation, "", abnormal, api.InvokeOptions{})
}

func deepCopyArgs(args []*api.Arg, tenantID string) []api.Arg {
	rtArgs := make([]api.Arg, len(args))
	for idx, val := range args {
		rtArgs[idx] = api.Arg{
			Type:     val.Type,
			Data:     val.Data,
			TenantID: tenantID,
		}
	}
	return rtArgs
}

// Invoke -
func (c *defaultClient) Invoke(req InvokeRequest) ([]byte, error) {
	log.GetLogger().Debugf("invoke by instanceId: %s", req.InstanceID)
	funcMeta := api.FunctionMeta{FuncID: req.Function, Api: api.FaaSApi}
	funcArgs := deepCopyArgs(req.Args, req.TenantID)
	invokeOpts := convertCommonInvokeOption(req)
	invokeOpts.RetryTimes = req.RetryTimes
	invokeOpts.ForceInvoke = req.ForceInvoke
	objID, err := c.clientLibruntime.InvokeByInstanceId(funcMeta, req.InstanceID, funcArgs, invokeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to invoke by instance id request, req: %#v, err: %s", req, err.Error())
	}
	return c.getRes(objID, req)
}

func (c *defaultClient) getRes(objID string, req InvokeRequest) ([]byte, error) {
	var res []byte
	var resErr error
	wait := make(chan struct{}, 1)
	c.clientLibruntime.GetAsync(objID, func(result []byte, err error) {
		res = result
		resErr = err
		wait <- struct{}{}
		if !req.BypassDataSystem {
			if _, err := c.clientLibruntime.GDecreaseRef([]string{objID}); err != nil {
				fmt.Printf("failed to decrease object ref,err: %s", err.Error())
			}
		}
	})
	log.GetLogger().Debugf("invoke AcceptHeader: %s, requestId: %s, objID: %s, instanceId: %s",
		req.AcceptHeader, req.RequestID, objID, req.InstanceID)
	if req.AcceptHeader != httpconstant.AcceptEventStream {
		<-wait
		return res, resErr
	}
	sseChan := &SSEChan{
		Event:     make(chan sseEvent, 100), // 使用100大小缓冲区，防止libruntime侧回写event消息阻塞
		WaitEvent: make(chan struct{}, 1),
	}
	c.clientLibruntime.GetEvent(objID, func(result []byte, err error) {
		log.GetLogger().Debugf("event msg: %s, size: %d, objID: %s", string(result), len(result), objID)
		select {
		case sseChan.Event <- sseEvent{Data: result, Err: err}:
		case <-sseChan.WaitEvent:
			return
		}
	})
	stopSSEHandle := make(chan struct{}) // 用于反向通知sse消息处理结束，防止协程泄露
	go c.handleEvent(objID, sseChan, req, stopSSEHandle)
	defer close(stopSSEHandle)
	select {
	case <-req.ResponseWriter.ClientDisconnectChan():
		return nil, fmt.Errorf("client disconnected during wait, stop sse request, objID: %s", objID)
	case <-wait:
		if resErr != nil {
			log.GetLogger().Errorf("notify response error, objID: %s, err: %v", objID, resErr)
			return res, resErr
		}
	case <-sseChan.WaitEvent:
		if sseChan.EventErr != nil {
			log.GetLogger().Errorf("handler sse event failed, objID: %s, err: %v", objID, sseChan.EventErr)
			return nil, sseChan.EventErr
		}
		log.GetLogger().Debugf("finish handle sse event, requestId: %s, objID: %s, instanceId: %s",
			req.RequestID, objID, req.InstanceID)
		<-wait
		return res, resErr
	}
	<-sseChan.WaitEvent
	if sseChan.EventErr != nil {
		log.GetLogger().Errorf("handler sse event failed, objID: %s, err: %v", objID, sseChan.EventErr)
		return nil, sseChan.EventErr
	}
	log.GetLogger().Debugf("finish handle sse event, requestId: %s, objID: %s, instanceId: %s",
		req.RequestID, objID, req.InstanceID)
	return res, nil
}

func (c *defaultClient) handleEvent(objID string, sseChan *SSEChan, req InvokeRequest, stopSSEHandle chan struct{}) {
	defer func() {
		if err := recover(); err != nil {
			log.GetLogger().Errorf("write response err: %v", err)
		}
		c.clientLibruntime.DeleteGetEventCallback(objID)
		close(sseChan.WaitEvent)
	}()
	for {
		select {
		case <-req.ResponseWriter.ClientDisconnectChan():
			sseChan.EventErr = fmt.Errorf("client disconnected during wait, stop sse request, objID: %s", objID)
			return
		case <-stopSSEHandle:
			return
		case event, ok := <-sseChan.Event:
			if !ok {
				log.GetLogger().Debugf("event channel closed, objID: %s", objID)
				return
			}
			if event.Err != nil {
				sseChan.EventErr = event.Err
				return
			}
			data := event.Data
			if bytes.Equal(data, []byte("yuanrong_event_EOF")) {
				log.GetLogger().Debugf("event recive EOF, objID: %s", objID)
				return
			}
			_, sseChan.EventErr = req.ResponseWriter.SSEWrite(data)
			if sseChan.EventErr != nil {
				return
			}
		}
	}
}

func (c *defaultClient) GetActiveMasterAddr() string {
	return c.clientLibruntime.GetActiveMasterAddr()
}

func convertInvokeOption(req InvokeRequest) api.InvokeOptions {
	invokeOpt := convertCommonInvokeOption(req)
	cpu, mem, customRes := LibruntimeCustomResources(req.ResourceSpecs)
	invokeOpt.Cpu = cpu
	invokeOpt.Memory = mem
	invokeOpt.CustomResources = customRes
	invokeOpt.SchedulerFunctionID = req.SchedulerFuncKey
	invokeOpt.SchedulerInstanceIDs = []string{req.SchedulerID}
	invokeOpt.AcquireTimeout = int(req.AcquireTimeout)
	if req.InstanceLabel != "" {
		invokeOpt.InvokeLabels[httpconstant.HeaderInstanceLabel] = req.InstanceLabel
	}
	return invokeOpt
}

func convertCommonInvokeOption(req InvokeRequest) api.InvokeOptions {
	customExtensions := make(map[string]string, len(req.InvokeTag)+1)
	for key, value := range req.InvokeTag {
		customExtensions[key] = value
	}
	if req.TraceParent != "" {
		customExtensions[traceParentExtensionKey] = req.TraceParent
	}
	invokeOpt := api.InvokeOptions{
		TraceID:          req.TraceID,
		Timeout:          int(req.InvokeTimeout),
		CustomExtensions: customExtensions,
		InvokeLabels:     map[string]string{},
	}
	if req.RouteAddress != "" {
		invokeOpt.CreateOpt = map[string]string{"YR_ROUTE": req.RouteAddress}
	}
	if req.AcceptHeader == httpconstant.AcceptEventStream {
		invokeOpt.InvokeLabels["accept"] = httpconstant.AcceptEventStream
	}
	if req.InstanceSession != nil {
		invokeOpt.InstanceSession = &api.InstanceSessionConfig{
			SessionID:   req.InstanceSession.SessionID,
			SessionTTL:  req.InstanceSession.SessionTTL,
			Concurrency: req.InstanceSession.Concurrency,
		}
	}
	invokeOpt.IsInterrupted = req.IsInterrupted
	invokeOpt.SessionCtxID = req.SessionCtxID
	return invokeOpt
}

func convertAcquireOption(req types.AcquireOption) api.InvokeOptions {
	cpu, mem, customRes := LibruntimeCustomResources(req.ResourceSpecs)
	customExtensions := map[string]string{}
	if req.TraceParent != "" {
		customExtensions[traceParentExtensionKey] = req.TraceParent
	}
	invokeOpt := api.InvokeOptions{
		Cpu:                  cpu,
		Memory:               mem,
		CustomResources:      customRes,
		CustomExtensions:     customExtensions,
		SchedulerFunctionID:  req.SchedulerFuncKey,
		SchedulerInstanceIDs: []string{req.SchedulerID},
		TraceID:              req.TraceID,
		RetryTimes:           maxInvokeRetries,
		Timeout:              int(req.Timeout),
		AcquireTimeout:       int(req.Timeout),
		TrafficLimited:       req.TrafficLimited,
		SessionCtxID:         req.SessionCtxID,
	}
	return invokeOpt
}

// InvokeByName -
func (c *defaultClient) InvokeByName(req InvokeRequest) ([]byte, error) {
	funcMeta := api.FunctionMeta{
		FuncID:    req.Function,
		Name:      &req.InstanceID,
		Sig:       req.FuncSig,
		Api:       utils.GetAPIType(req.BusinessType),
		PoolLabel: req.PoolLabel,
	}
	funcArgs := deepCopyArgs(req.Args, req.TenantID)
	invokeOpt := convertInvokeOption(req)
	objID, err := c.clientLibruntime.InvokeByFunctionName(funcMeta, funcArgs, invokeOpt)
	if err != nil {
		return nil, err
	}
	return c.getRes(objID, req)
}

func (c *defaultClient) CreateInstanceRaw(createReq []byte, option api.RawRequestOption) ([]byte, error) {
	return c.CreateInstanceRawContext(context.Background(), createReq, option)
}

func (c *defaultClient) CreateInstanceRawContext(ctx context.Context, createReq []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if contextual, ok := c.clientLibruntime.(interface {
		CreateInstanceRawContext(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	}); ok {
		return contextual.CreateInstanceRawContext(ctx, createReq, option)
	}
	return c.clientLibruntime.CreateInstanceRaw(createReq, option)
}

// CreateInstanceRawWithContext preserves the existing Client interface while
// allowing Go-native raw lifecycle backends to inherit the caller context.
// Legacy clients keep their established context-free rollback behavior.
func CreateInstanceRawWithContext(ctx context.Context, client Client, createReq []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if contextual, ok := client.(interface {
		CreateInstanceRawContext(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	}); ok {
		return contextual.CreateInstanceRawContext(ctx, createReq, option)
	}
	return client.CreateInstanceRaw(createReq, option)
}

func (c *defaultClient) InvokeInstanceRaw(invokeReq []byte, option api.RawRequestOption) ([]byte, error) {
	return c.InvokeInstanceRawContext(context.Background(), invokeReq, option)
}

func (c *defaultClient) InvokeInstanceRawContext(ctx context.Context, invokeReq []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if contextual, ok := c.clientLibruntime.(interface {
		InvokeByInstanceIdRawContext(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	}); ok {
		return contextual.InvokeByInstanceIdRawContext(ctx, invokeReq, option)
	}
	return c.clientLibruntime.InvokeByInstanceIdRaw(invokeReq, option)
}

// InvokeInstanceRawWithContext is the context-bearing raw invoke seam used by
// HTTP and websocket entrypoints without widening the legacy Client contract.
func InvokeInstanceRawWithContext(ctx context.Context, client Client, invokeReq []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if contextual, ok := client.(interface {
		InvokeInstanceRawContext(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	}); ok {
		return contextual.InvokeInstanceRawContext(ctx, invokeReq, option)
	}
	return client.InvokeInstanceRaw(invokeReq, option)
}

func (c *defaultClient) KillByLibRt(instanceID string, signal int, payload []byte) error {
	return c.clientLibruntime.Kill(instanceID, signal, payload, api.InvokeOptions{})
}

func (c *defaultClient) KillInstance(
	funcMeta api.FunctionMeta,
	instanceID string,
	signal int,
	payload []byte,
	invokeOpt api.InvokeOptions,
) error {
	if typedRuntime, ok := c.clientLibruntime.(interface {
		KillInstance(api.FunctionMeta, string, int, []byte, api.InvokeOptions) error
	}); ok {
		return typedRuntime.KillInstance(funcMeta, instanceID, signal, payload, invokeOpt)
	}
	return c.clientLibruntime.Kill(instanceID, signal, payload, invokeOpt)
}

func (c *defaultClient) CreateInstanceByLibRt(
	funcMeta api.FunctionMeta,
	args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	return c.CreateRuntimeInstance(funcMeta, args, invokeOpt)
}

func (c *defaultClient) CreateRuntimeInstance(
	funcMeta api.FunctionMeta,
	args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	return c.clientLibruntime.CreateInstance(funcMeta, args, invokeOpt)
}

func (c *defaultClient) InvokeInstanceByLibRtAndGet(
	funcMeta api.FunctionMeta,
	instanceID string,
	args []api.Arg,
	invokeOpt api.InvokeOptions,
) ([]byte, error) {
	objID, err := c.clientLibruntime.InvokeByInstanceId(funcMeta, instanceID, args, invokeOpt)
	if err != nil {
		return nil, err
	}
	return c.getRes(objID, InvokeRequest{BypassDataSystem: invokeOpt.BypassDataSystem})
}

func (c *defaultClient) KillRaw(killReq []byte, option api.RawRequestOption) ([]byte, error) {
	return c.KillRawContext(context.Background(), killReq, option)
}

func (c *defaultClient) KillRawContext(ctx context.Context, killReq []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if contextual, ok := c.clientLibruntime.(interface {
		KillRawContext(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	}); ok {
		return contextual.KillRawContext(ctx, killReq, option)
	}
	return c.clientLibruntime.KillRaw(killReq, option)
}

// KillRawWithContext is the context-bearing raw kill seam. The compatibility
// fallback remains explicit when the selected backend has no contextual API.
func KillRawWithContext(ctx context.Context, client Client, killReq []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if contextual, ok := client.(interface {
		KillRawContext(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	}); ok {
		return contextual.KillRawContext(ctx, killReq, option)
	}
	return client.KillRaw(killReq, option)
}

func (c *defaultClient) IsHealth() bool {
	return c.clientLibruntime.IsHealth()
}

func (c *defaultClient) IsDsHealth() bool {
	return c.clientLibruntime.IsDsHealth()
}
