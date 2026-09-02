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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/logger/log"
	fronttls "frontend/pkg/common/faas_common/tls"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/proxyrouting"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
)

var frontendProxyRequestSeq atomic.Uint64

const (
	frontendPauseSnapshotSignal  = 18
	frontendResumeSnapshotSignal = 19
)

var (
	simpleRuntimeSmallValueBufferPool  sync.Pool
	simpleRuntimeMediumValueBufferPool sync.Pool
)

var (
	frontendRouteLifecycleObserverMu sync.RWMutex
	frontendRouteLifecycleObserver   func(frontendRouteLifecycleEvent)
)

const (
	frontendProxyRouteKey                             = "YR_ROUTE"
	frontendProxyCreateSourceKey                      = "source"
	frontendProxyCreateSource                         = "frontend"
	frontendProxyControlNotWired                      = "control-path-not-wired"
	simpleRuntimeFaaSMetaPrefix                       = "0000000000000000"
	defaultFrontendProxyTimeout                       = 60 * time.Second
	frontendProxyInvokeResultBuffer                   = 10 * time.Second
	frontendProxyKeepaliveTimeout                     = 10 * time.Second
	frontendProxyKillMaxAttempts                      = 2
	runtimeRequestIDLength                            = 18
	createReadyCallResultFieldNumber protowire.Number = 4
	runtimeNotifyRequestIDField      protowire.Number = 1
	runtimeNotifyCodeField           protowire.Number = 2
	runtimeNotifyMessageField        protowire.Number = 3
	runtimeNotifySmallObjectField    protowire.Number = 4
	runtimeNotifyStackTraceField     protowire.Number = 5
	runtimeNotifyRuntimeInfoField    protowire.Number = 7
	runtimeNotifyInstanceIDField     protowire.Number = 8
	functionMetaAppNameField         protowire.Number = 1
	functionMetaModuleNameField      protowire.Number = 2
	functionMetaFunctionNameField    protowire.Number = 3
	functionMetaClassNameField       protowire.Number = 4
	functionMetaLanguageField        protowire.Number = 5
	functionMetaSignatureField       protowire.Number = 7
	functionMetaAPIField             protowire.Number = 8
	functionMetaNameField            protowire.Number = 9
	functionMetaNamespaceField       protowire.Number = 10
	functionMetaIDField              protowire.Number = 11
	metaDataConfigField              protowire.Number = 3
	metaConfigCodePathsField         protowire.Number = 2
)

type grpcFrontendProxyInvokeClient struct {
	client           frontend_proxy.FrontendProxyServiceClient
	frontendClientID string
}

type grpcFrontendProxyLifecycleClient struct {
	client           frontend_proxy.FrontendProxyServiceClient
	frontendClientID string
}

type frontendRouteLifecycleEvent struct {
	Operation          string `json:"operation"`
	Outcome            string `json:"outcome"`
	CleanupOutcome     string `json:"cleanupOutcome"`
	RequestID          string `json:"requestID"`
	TraceID            string `json:"traceID,omitempty"`
	InstanceID         string `json:"instanceID"`
	OwningProxyID      string `json:"owningProxyID,omitempty"`
	RoutePresentBefore bool   `json:"routePresentBefore"`
	RoutePresentAfter  bool   `json:"routePresentAfter"`
	ReplayAttempted    bool   `json:"replayAttempted"`
}

func observeFrontendRouteLifecycle(req simpleRuntimeKillRequest, requestID, outcome string,
	change instancemanager.RouteOnlyInstanceChange, replayAttempted bool,
) {
	event := frontendRouteLifecycleEvent{
		Operation:          "kill",
		Outcome:            outcome,
		CleanupOutcome:     "route-hint-cleared",
		RequestID:          requestID,
		TraceID:            firstNonEmpty(req.options.TraceID, req.options.CustomExtensions[traceParentExtensionKey]),
		InstanceID:         req.instanceID,
		OwningProxyID:      change.Before.FunctionProxyID,
		RoutePresentBefore: change.Before.Present,
		RoutePresentAfter:  change.After.Present,
		ReplayAttempted:    replayAttempted,
	}
	if encoded, err := json.Marshal(event); err == nil {
		log.GetLogger().Infof("frontend_route_lifecycle %s", encoded)
	} else {
		log.GetLogger().Warnf("failed to encode frontend route lifecycle event: %v", err)
	}
	frontendRouteLifecycleObserverMu.RLock()
	observer := frontendRouteLifecycleObserver
	frontendRouteLifecycleObserverMu.RUnlock()
	if observer != nil {
		observer(event)
	}
}

func newGRPCFrontendProxyInvokeClient(client frontend_proxy.FrontendProxyServiceClient,
	frontendClientID string,
) frontendProxyInvokeClient {
	return &grpcFrontendProxyInvokeClient{
		client:           client,
		frontendClientID: frontendClientID,
	}
}

func newGRPCFrontendProxyLifecycleClient(client frontend_proxy.FrontendProxyServiceClient,
	frontendClientID string,
) frontendProxyLifecycleClient {
	return &grpcFrontendProxyLifecycleClient{
		client:           client,
		frontendClientID: frontendClientID,
	}
}

func (c *grpcFrontendProxyInvokeClient) InvokeByInstanceID(req simpleRuntimeInvokeRequest) ([]byte, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	requestID := newFrontendProxyRuntimeRequestID()
	ctx, cancel := simpleRuntimeInvokeContextWithParent(req.ctx, req.options, frontendProxyInvokeResultBuffer)
	defer cancel()
	invokeArgs, releaseInvokeArgs := convertSimpleRuntimeInvokeArgsForRPC(req.funcMeta, req.args)
	defer releaseInvokeArgs()
	resp, err := c.client.InvokeInstance(ctx, &frontend_proxy.InvokeInstanceRequest{
		Context: &frontend_proxy.FrontendRequestContext{
			FrontendClientID: c.frontendClientID,
			TenantID:         firstArgTenantID(req.args),
			RequestID:        requestID,
			TraceID:          req.options.TraceID,
		},
		Invoke: &core.InvokeRequest{
			Function:      req.funcMeta.FuncID,
			Args:          invokeArgs,
			InstanceID:    req.instanceID,
			RequestID:     requestID,
			TraceID:       req.options.TraceID,
			InvokeOptions: convertSimpleRuntimeInvokeOptions(req.options),
		},
		InvokeTimeoutMs: functionInvokeTimeoutMs(req.options),
	})
	if err != nil {
		return nil, newDirectProxyPostDispatchError("invoke transport", err)
	}
	return decodeFrontendProxyInvokeResponse(req.funcMeta, resp)
}

func (c *grpcFrontendProxyInvokeClient) InvokeByInstanceIDStream(
	req simpleRuntimeInvokeRequest,
	responseWriter types.ResponseWriter,
) ([]byte, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	if responseWriter == nil {
		return nil, fmt.Errorf("frontend proxy streaming invoke response writer is nil")
	}
	requestID := newFrontendProxyRuntimeRequestID()
	baseCtx, cancel := simpleRuntimeInvokeContextWithParent(req.ctx, req.options, frontendProxyInvokeResultBuffer)
	defer cancel()
	ctx, cancelStream := context.WithCancel(baseCtx)
	defer cancelStream()
	go func() {
		select {
		case <-responseWriter.ClientDisconnectChan():
			cancelStream()
		case <-ctx.Done():
		}
	}()
	invokeArgs, releaseInvokeArgs := convertSimpleRuntimeInvokeArgsForRPC(req.funcMeta, req.args)
	defer releaseInvokeArgs()
	stream, err := c.client.InvokeInstanceStream(ctx, &frontend_proxy.InvokeInstanceRequest{
		Context: &frontend_proxy.FrontendRequestContext{
			FrontendClientID: c.frontendClientID,
			TenantID:         firstArgTenantID(req.args),
			RequestID:        requestID,
			TraceID:          req.options.TraceID,
		},
		Invoke: &core.InvokeRequest{
			Function:      req.funcMeta.FuncID,
			Args:          invokeArgs,
			InstanceID:    req.instanceID,
			RequestID:     requestID,
			TraceID:       req.options.TraceID,
			InvokeOptions: convertSimpleRuntimeInvokeOptions(req.options),
		},
		InvokeTimeoutMs: functionInvokeTimeoutMs(req.options),
	})
	if err != nil {
		return nil, newDirectProxyPostDispatchError("invoke stream transport", err)
	}
	return receiveFrontendProxyInvokeStream(stream, responseWriter, req.funcMeta, cancelStream)
}

func receiveFrontendProxyInvokeStream(
	stream grpc.ServerStreamingClient[frontend_proxy.InvokeInstanceStreamResponse],
	responseWriter types.ResponseWriter,
	funcMeta api.FunctionMeta,
	cancel context.CancelFunc,
) ([]byte, error) {
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil, newDirectProxyPostDispatchError(
				"invoke stream receive", fmt.Errorf("stream ended without final response"),
			)
		}
		if recvErr != nil {
			return nil, newDirectProxyPostDispatchError("invoke stream receive", recvErr)
		}
		switch payload := frame.GetPayload().(type) {
		case *frontend_proxy.InvokeInstanceStreamResponse_Event:
			if _, err := responseWriter.SSEWrite(payload.Event); err != nil {
				cancel()
				return nil, err
			}
		case *frontend_proxy.InvokeInstanceStreamResponse_Final:
			if payload.Final == nil {
				return nil, newDirectProxyPostDispatchError(
					"invoke stream decode", fmt.Errorf("final response is nil"),
				)
			}
			return decodeFrontendProxyInvokeResponse(funcMeta, payload.Final)
		default:
			return nil, newDirectProxyPostDispatchError(
				"invoke stream decode", fmt.Errorf("received empty frame"),
			)
		}
	}
}

func decodeFrontendProxyInvokeResponse(
	funcMeta api.FunctionMeta,
	resp *frontend_proxy.InvokeInstanceResponse,
) ([]byte, error) {
	if resp == nil {
		return nil, newDirectProxyPostDispatchError("invoke decode", fmt.Errorf("response is nil"))
	}
	if err := checkFrontendProxyStatus("invoke", resp.GetStatus()); err != nil {
		if _, classified := GetDirectProxyErrorMetadata(err); !classified {
			return nil, newDirectProxyPostDispatchError("invoke decode", err)
		}
		return nil, err
	}
	callResult := resp.GetCallResult()
	if callResult == nil {
		return nil, newDirectProxyPostDispatchError("invoke decode", fmt.Errorf("missing call result"))
	}
	if callResult.GetCode() != common.ErrorCode_ERR_NONE {
		return nil, frontendProxyBusinessError("invoke call result", callResult.GetCode(), callResult.GetMessage())
	}
	// Direct frontend invokes do not set ReturnObjectIDs, so runtime uses its
	// returnByMsg contract. SmallObjects, when present, are an implementation
	// detail whose value may be either a complete DataObject buffer or a data
	// view depending on the executor. CallResult.message is the stable data
	// region exposed by the historical libruntime path.
	return normalizeSimpleRuntimeInvokePayload(funcMeta, []byte(callResult.GetMessage())), nil
}

func (c *grpcFrontendProxyLifecycleClient) CreateInstance(req simpleRuntimeCreateRequest) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("frontend proxy grpc client is nil")
	}
	requestID := newFrontendProxyRuntimeRequestID()
	ctx, cancel := simpleRuntimeInvokeContext(req.options)
	defer cancel()
	resp, err := c.client.CreateInstance(ctx, &frontend_proxy.CreateInstanceRequest{
		Context: &frontend_proxy.FrontendRequestContext{
			FrontendClientID: c.frontendClientID,
			TenantID:         firstNonEmpty(firstArgTenantID(req.args), req.tenantID),
			RequestID:        requestID,
			TraceID:          req.options.TraceID,
		},
		Create: &core.CreateRequest{
			Function:      req.funcMeta.FuncID,
			Args:          convertSimpleRuntimeCreateArgs(req.funcMeta, req.args, req.options.CodePaths),
			RequestID:     requestID,
			TraceID:       req.options.TraceID,
			CreateOptions: convertSimpleRuntimeCreateOptions(req.options),
			SchedulingOps: convertSimpleRuntimeSchedulingOps(req.options),
		},
	})
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", fmt.Errorf("frontend proxy create response is nil")
	}
	if err := checkFrontendProxyStatus("create", resp.GetStatus()); err != nil {
		return "", err
	}
	createResp := resp.GetCreate()
	if createResp == nil {
		return "", fmt.Errorf("frontend proxy create missing create response")
	}
	if createResp.GetCode() != common.ErrorCode_ERR_NONE {
		return "", frontendProxyBusinessError("create", createResp.GetCode(), createResp.GetMessage())
	}
	instanceID := createResp.GetInstanceID()
	if instanceID == "" {
		return "", fmt.Errorf("frontend proxy create response missing instance id")
	}
	ownerProxyID := strings.TrimSpace(resp.GetRouteAddress())
	if ownerProxyID != "" {
		if proxyrouting.IsHostPort(ownerProxyID) {
			return "", fmt.Errorf("frontend proxy create owner must be a proxy node id, got address %q", ownerProxyID)
		}
		instancemanager.RecordRouteOnlyInstance(req.funcMeta.FuncID, instanceID, ownerProxyID)
	}
	return instanceID, nil
}

func (c *grpcFrontendProxyLifecycleClient) CreateInstanceRaw(req simpleRuntimeRawCreateRequest) ([]byte, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	createReq := &core.CreateRequest{}
	if err := proto.Unmarshal(req.create, createReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontend proxy create request: %w", err)
	}
	if err := validateDirectProxyCoreArgs(createReq.GetArgs()); err != nil {
		return nil, fmt.Errorf("direct proxy raw create: %w", err)
	}
	externalRequestID := createReq.GetRequestID()
	requestID := newFrontendProxyRuntimeRequestID()
	createReq.RequestID = requestID
	applyRawRequestOptionsToCreate(createReq, req.options)
	ctx, cancel := rawSimpleRuntimeContext(req.ctx, req.options)
	defer cancel()
	resp, err := c.client.CreateInstance(ctx, &frontend_proxy.CreateInstanceRequest{
		Context: rawFrontendRequestContext(
			c.frontendClientID,
			requestID,
			createReq.GetTraceID(),
			req.options,
			tenantIDFromCreateRequest(createReq),
		),
		Create:          createReq,
		CreateTimeoutMs: timeoutSecondsToMs(req.timeoutSeconds),
	})
	if err != nil {
		return nil, err
	}
	return decodeFrontendProxyRawCreateResponse(resp, createReq, externalRequestID, requestID)
}

func decodeFrontendProxyRawCreateResponse(
	resp *frontend_proxy.CreateInstanceResponse,
	createReq *core.CreateRequest,
	externalRequestID string,
	requestID string,
) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("frontend proxy create response is nil")
	}
	if err := checkFrontendProxyStatus("create", resp.GetStatus()); err != nil {
		return nil, err
	}
	callResult, err := createReadyCallResultFromResponse(resp)
	if err != nil {
		return nil, err
	}
	if callResult == nil {
		if createResp := resp.GetCreate(); createResp != nil && createResp.GetCode() != common.ErrorCode_ERR_NONE {
			return nil, frontendProxyBusinessError("create", createResp.GetCode(), createResp.GetMessage())
		}
		return nil, fmt.Errorf("frontend proxy create missing ready call result")
	}
	if createResp := resp.GetCreate(); createResp != nil {
		instanceID := firstNonEmpty(createResp.GetInstanceID(), callResult.GetInstanceID())
		ownerProxyID := strings.TrimSpace(resp.GetRouteAddress())
		if instanceID != "" && ownerProxyID == "" {
			return nil, fmt.Errorf("frontend proxy create response missing final owner proxy node id")
		}
		if proxyrouting.IsHostPort(ownerProxyID) {
			return nil, fmt.Errorf("frontend proxy create owner must be a proxy node id, got address %q", ownerProxyID)
		}
		if instanceID != "" {
			instancemanager.RecordRouteOnlyInstance(createReq.GetFunction(), instanceID, ownerProxyID)
		}
	}
	return marshalRuntimeNotifyFromCallResultWithRequestID(callResult, firstNonEmpty(externalRequestID, requestID))
}

func (c *grpcFrontendProxyLifecycleClient) KillInstance(req simpleRuntimeKillRequest) error {
	_, err := c.KillInstanceWithResponse(req)
	return err
}

func (c *grpcFrontendProxyLifecycleClient) KillInstanceWithResponse(
	req simpleRuntimeKillRequest,
) (*core.KillResponse, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	requestID := firstNonEmpty(req.requestID,
		fmt.Sprintf("frontend-proxy-kill-%d", frontendProxyRequestSeq.Add(1)))
	ctx, cancel := simpleRuntimeInvokeContextWithParent(req.ctx, req.options, 0)
	defer cancel()
	resp, err := c.client.KillInstance(ctx, &frontend_proxy.KillInstanceRequest{
		Context: frontendRequestContextFromInvokeOptions(c.frontendClientID, req.tenantID, requestID, req.options),
		Kill: &core.KillRequest{
			InstanceID: req.instanceID,
			Signal:     int32(req.signal),
			Payload:    req.payload,
			RequestID:  requestID,
		},
		LifecycleTimeoutMs: timeoutSecondsToMs(req.options.Timeout),
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("frontend proxy kill response is nil")
	}
	if err := checkFrontendProxyStatus("kill", resp.GetStatus()); err != nil {
		return nil, err
	}
	killResp := resp.GetKill()
	if killResp == nil {
		return nil, fmt.Errorf("frontend proxy kill missing kill response")
	}
	if killResp.GetCode() != common.ErrorCode_ERR_NONE {
		return nil, frontendProxyBusinessError("kill", killResp.GetCode(), killResp.GetMessage())
	}
	before := instancemanager.SnapshotRouteOnlyInstance(req.instanceID)
	change := instancemanager.RouteOnlyInstanceChange{Before: before, After: before}
	switch req.signal {
	case frontendPauseSnapshotSignal:
		// PAUSED is owned by InstanceManager. The proxy handling this request is
		// only a stateless gateway and must not remain the instance owner.
		instancemanager.RecordRouteOnlyInstance(
			before.Function, req.instanceID, execendpoint.InstanceManagerOwner)
		change.After = instancemanager.SnapshotRouteOnlyInstance(req.instanceID)
	case frontendResumeSnapshotSignal:
		var started core.SnapStartedInfo
		if err := proto.Unmarshal(killResp.GetPayload(), &started); err != nil {
			return nil, fmt.Errorf("resume kill returned invalid route payload: %w", err)
		}
		if started.GetInstanceID() != req.instanceID || started.GetFunctionProxyID() == "" ||
			started.GetRouteAddress() == "" {
			return nil, fmt.Errorf("resume kill returned a mismatched or empty function proxy route")
		}
		instancemanager.RecordRouteOnlyInstance(
			before.Function, req.instanceID, started.GetFunctionProxyID())
		change.After = instancemanager.SnapshotRouteOnlyInstance(req.instanceID)
	default:
		change = instancemanager.RemoveRouteOnlyInstanceWithSnapshot(req.instanceID)
	}
	observeFrontendRouteLifecycle(req, requestID, "success", change, false)
	return killResp, nil
}

func (c *grpcFrontendProxyInvokeClient) InvokeByInstanceIDRaw(req simpleRuntimeRawInvokeRequest) ([]byte, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	invokeReq := &core.InvokeRequest{}
	if err := proto.Unmarshal(req.invoke, invokeReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontend proxy invoke request: %w", err)
	}
	if err := validateDirectProxyCoreArgs(invokeReq.GetArgs()); err != nil {
		return nil, fmt.Errorf("direct proxy raw invoke: %w", err)
	}
	externalRequestID := invokeReq.GetRequestID()
	requestID := newFrontendProxyRuntimeRequestID()
	invokeReq.RequestID = requestID
	applyRawRequestOptionsToInvoke(invokeReq, req.options)
	ctx, cancel := rawSimpleRuntimeContext(req.ctx, req.options)
	defer cancel()
	resp, err := c.client.InvokeInstance(ctx, &frontend_proxy.InvokeInstanceRequest{
		Context: rawFrontendRequestContext(
			c.frontendClientID,
			requestID,
			invokeReq.GetTraceID(),
			req.options,
			tenantIDFromInvokeRequest(invokeReq),
		),
		Invoke: invokeReq,
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("frontend proxy invoke response is nil")
	}
	if callResult := resp.GetCallResult(); callResult != nil {
		return marshalRuntimeNotifyFromCallResultWithRequestID(callResult, firstNonEmpty(externalRequestID, requestID))
	}
	if err := checkFrontendProxyStatus("invoke", resp.GetStatus()); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("frontend proxy invoke missing call result")
}

func validateDirectProxyCoreArgs(args []*common.Arg) error {
	for i, arg := range args {
		if arg == nil || arg.GetType() != common.Arg_VALUE || len(arg.GetNestedRefs()) != 0 {
			return fmt.Errorf(
				"argument %d uses ObjectRef or nested refs, which the direct proxy inline-data API does not support", i)
		}
	}
	return nil
}

type frontendProxyRouteResolver interface {
	ResolveFrontendProxyAddress(req simpleRuntimeInvokeRequest) (string, error)
}

type frontendProxyServiceClientFactory interface {
	ClientForAddress(address string) (frontend_proxy.FrontendProxyServiceClient, error)
}

type frontendProxyServiceClientEvictor interface {
	EvictAddress(address string)
}

type routingFrontendProxyInvokeClient struct {
	resolver         frontendProxyRouteResolver
	clientFactory    frontendProxyServiceClientFactory
	frontendClientID string
}

type routingFrontendProxyLifecycleClient struct {
	clientFactory    frontendProxyServiceClientFactory
	frontendClientID string
}

func newRoutingFrontendProxyInvokeClient() frontendProxyInvokeClient {
	return &routingFrontendProxyInvokeClient{
		resolver:         defaultFrontendProxyRouteResolver{},
		clientFactory:    newFrontendProxyGRPCClientPool(),
		frontendClientID: currentFrontendClientID(),
	}
}

func newRoutingFrontendProxyLifecycleClient() frontendProxyLifecycleClient {
	return &routingFrontendProxyLifecycleClient{
		clientFactory:    newFrontendProxyGRPCClientPool(),
		frontendClientID: currentFrontendClientID(),
	}
}

func (c *routingFrontendProxyInvokeClient) InvokeByInstanceID(req simpleRuntimeInvokeRequest) ([]byte, error) {
	if c == nil || c.resolver == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy routing client is not initialized")
	}
	address, err := c.resolver.ResolveFrontendProxyAddress(req)
	if err != nil {
		return nil, newDirectProxyPreDispatchError("owner resolution", err)
	}
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, newDirectProxyPreDispatchError("client acquisition", err)
	}
	payload, err := newGRPCFrontendProxyInvokeClient(serviceClient, c.frontendClientID).InvokeByInstanceID(req)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	return payload, nil
}

func (c *routingFrontendProxyInvokeClient) InvokeByInstanceIDStream(
	req simpleRuntimeInvokeRequest,
	responseWriter types.ResponseWriter,
) ([]byte, error) {
	if c == nil || c.resolver == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy routing client is not initialized")
	}
	address, err := c.resolver.ResolveFrontendProxyAddress(req)
	if err != nil {
		return nil, newDirectProxyPreDispatchError("owner resolution", err)
	}
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, newDirectProxyPreDispatchError("client acquisition", err)
	}
	payload, err := newGRPCFrontendProxyInvokeClient(serviceClient, c.frontendClientID).
		InvokeByInstanceIDStream(req, responseWriter)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	return payload, nil
}

func (c *routingFrontendProxyLifecycleClient) CreateInstance(req simpleRuntimeCreateRequest) (string, error) {
	if c == nil || c.clientFactory == nil {
		return "", fmt.Errorf("frontend proxy lifecycle routing client is not initialized")
	}
	tried := make(map[string]struct{})
	var lastErr error
	selectCtx, cancelSelect := simpleRuntimeInvokeContext(req.options)
	defer cancelSelect()
	for {
		endpoint, selectErr := proxyrouting.Select(selectCtx, proxyrouting.CapabilityCreate, tried)
		if selectErr != nil {
			if lastErr != nil {
				return "", lastErr
			}
			return "", selectErr
		}
		tried[endpoint.GRPCAddress] = struct{}{}
		serviceClient, err := c.clientFactory.ClientForAddress(endpoint.GRPCAddress)
		if err != nil {
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.GRPCAddress, err)
			lastErr = err
			continue
		}
		instanceID, err := newGRPCFrontendProxyLifecycleClient(serviceClient, c.frontendClientID).CreateInstance(req)
		if err != nil {
			if isFrontendProxyCreatePreDispatchStatus(err) {
				lastErr = err
				continue
			}
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.GRPCAddress, err)
			return "", err
		}
		return instanceID, nil
	}
}

func (c *routingFrontendProxyLifecycleClient) CreateInstanceRaw(req simpleRuntimeRawCreateRequest) ([]byte, error) {
	if c == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy lifecycle routing client is not initialized")
	}
	tried := make(map[string]struct{})
	var lastErr error
	for {
		endpoint, selectErr := proxyrouting.Select(req.ctx, proxyrouting.CapabilityCreate, tried)
		if selectErr != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, selectErr
		}
		tried[endpoint.GRPCAddress] = struct{}{}
		serviceClient, err := c.clientFactory.ClientForAddress(endpoint.GRPCAddress)
		if err != nil {
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.GRPCAddress, err)
			lastErr = err
			continue
		}
		notify, err := newGRPCFrontendProxyLifecycleClient(serviceClient, c.frontendClientID).CreateInstanceRaw(req)
		if err != nil {
			if isFrontendProxyCreatePreDispatchStatus(err) {
				lastErr = err
				continue
			}
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.GRPCAddress, err)
			return nil, err
		}
		return notify, nil
	}
}

func (c *routingFrontendProxyLifecycleClient) KillInstance(req simpleRuntimeKillRequest) error {
	_, err := c.KillInstanceWithResponse(req)
	return err
}

func (c *routingFrontendProxyLifecycleClient) KillInstanceWithResponse(
	req simpleRuntimeKillRequest,
) (*core.KillResponse, error) {
	if c == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy lifecycle routing client is not initialized")
	}
	if req.requestID == "" {
		req.requestID = fmt.Sprintf("frontend-proxy-kill-%d", frontendProxyRequestSeq.Add(1))
	}
	tried := make(map[string]struct{})
	for attempt := 0; attempt < frontendProxyKillMaxAttempts; attempt++ {
		paused := execendpoint.Default().IsPaused(req.instanceID)
		// Resolve the owning proxy address. The routable instance table (InstanceScheduler)
		// only holds RUNNING instances; FATAL/EXITED/FAILED instances self-clear from it. For
		// those unreachable states, fall back to the execendpoint summary (populated from etcd
		// PUT for cacheable states incl. FATAL) to recover the owning node ID and route the
		// kill directly. invoke paths keep using the blocking Wait (see resolveDirectProxyOwner)
		// and are untouched here, so dead instances remain unroutable for invocation.
		address, err := c.resolveKillAddress(req, paused, tried)
		if err != nil {
			return nil, err
		}
		tried[address] = struct{}{}
		serviceClient, err := c.clientFactory.ClientForAddress(address)
		if err != nil {
			evictFrontendProxyClientOnError(c.clientFactory, address, err)
			if paused && attempt+1 < frontendProxyKillMaxAttempts {
				continue
			}
			return nil, err
		}
		response, err := newGRPCFrontendProxyLifecycleClient(serviceClient, c.frontendClientID).
			KillInstanceWithResponse(req)
		if err == nil {
			return response, nil
		}
		if paused && isPausedLifecycleGatewayRetryable(err) && attempt+1 < frontendProxyKillMaxAttempts {
			continue
		}
		if !isFrontendProxyRouteStaleStatus(err) {
			evictFrontendProxyClientOnError(c.clientFactory, address, err)
			return nil, err
		}
		change := instancemanager.RouteOnlyInstanceChange{
			Before: instancemanager.SnapshotRouteOnlyInstance(req.instanceID),
			After:  instancemanager.SnapshotRouteOnlyInstance(req.instanceID),
		}
		if !paused {
			change = instancemanager.RemoveRouteOnlyInstanceWithSnapshot(req.instanceID)
		}
		willRetry := attempt+1 < frontendProxyKillMaxAttempts
		observeFrontendRouteLifecycle(req, "", "route-stale", change, willRetry)
		if !willRetry {
			return nil, err
		}
	}
	return nil, fmt.Errorf("frontend proxy kill retry exhausted")
}

// resolveKillAddress resolves the owning frontend proxy gRPC address for a kill.
// For a PAUSED instance it selects a control gateway (the instance has no live
// runtime of its own). Otherwise it prefers the non-blocking Resolve; when the
// instance is absent from the routable table (FATAL/EXITED/FAILED after
// self-clear), it falls back to the execendpoint summary's NodeID so the kill
// can still reach the backend. The fallback is kill-only: invocation does not
// call this path and keeps using the blocking Wait (resolveDirectProxyOwner),
// so dead instances remain unroutable for invocation.
func (c *routingFrontendProxyLifecycleClient) resolveKillAddress(
	req simpleRuntimeKillRequest, paused bool, excluded map[string]struct{},
) (string, error) {
	if paused {
		endpoint, err := proxyrouting.Select(req.ctx, proxyrouting.CapabilityKill, excluded)
		if err != nil {
			return "", fmt.Errorf("select paused instance %s control gateway: %w", req.instanceID, err)
		}
		if !proxyrouting.IsRoutableAddress(endpoint.GRPCAddress) {
			return "", fmt.Errorf("selected paused instance %s gateway has no routable address", req.instanceID)
		}
		return endpoint.GRPCAddress, nil
	}
	if route, err := proxyrouting.Resolve(
		req.instanceID, proxyrouting.CapabilityKill, proxyrouting.TransportGRPC); err == nil {
		return route.Address, nil
	}
	// Instance not in the routable table; fall back to the cached summary to
	// recover the owning proxy node ID. The summary stays until the etcd key is
	// deleted by the watcher, so it remains available while a kill is in flight.
	summary, ok := execendpoint.Default().GetSummary(req.instanceID)
	if !ok || summary.NodeID == "" {
		return "", fmt.Errorf("instance %s is not present in frontend route cache", req.instanceID)
	}
	endpoint, ok := proxyrouting.Lookup(summary.NodeID, proxyrouting.CapabilityKill)
	if !ok {
		return "", fmt.Errorf("owner proxy %s for instance %s does not publish healthy capability %s",
			summary.NodeID, req.instanceID, proxyrouting.CapabilityKill)
	}
	if !proxyrouting.IsRoutableAddress(endpoint.GRPCAddress) {
		return "", fmt.Errorf("owner proxy %s has no routable address for capability %s",
			summary.NodeID, proxyrouting.CapabilityKill)
	}
	return endpoint.GRPCAddress, nil
}

func (c *routingFrontendProxyInvokeClient) InvokeByInstanceIDRaw(req simpleRuntimeRawInvokeRequest) ([]byte, error) {
	if c == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy routing client is not initialized")
	}
	invokeReq := &core.InvokeRequest{}
	if err := proto.Unmarshal(req.invoke, invokeReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontend proxy invoke route request: %w", err)
	}
	route, err := resolveDirectProxyOwner(
		req.ctx, invokeReq.GetInstanceID(), proxyrouting.CapabilityInvoke, proxyrouting.TransportGRPC)
	if err != nil {
		return nil, err
	}
	address := route.Address
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	notify, err := newGRPCFrontendProxyInvokeClient(serviceClient, c.frontendClientID).InvokeByInstanceIDRaw(req)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	return notify, nil
}

// UploadFile resolves the owning proxy for instanceID, acquires a pooled gRPC
type defaultFrontendProxyRouteResolver struct{}

func (defaultFrontendProxyRouteResolver) ResolveFrontendProxyAddress(req simpleRuntimeInvokeRequest) (string, error) {
	route, err := proxyrouting.WaitForInvoke(req.ctx, req.instanceID,
		proxyrouting.CapabilityInvoke, proxyrouting.TransportGRPC, req.options.ForceInvoke)
	if err != nil {
		return "", err
	}
	return route.Address, nil
}

func resolveDirectProxyOwner(ctx context.Context, instanceID string, capability proxyrouting.Capability,
	transport proxyrouting.Transport,
) (proxyrouting.OwnerRoute, error) {
	if execendpoint.Default().IsPaused(instanceID) {
		return proxyrouting.OwnerRoute{}, execendpoint.NewInstancePausedError(instanceID)
	}
	return proxyrouting.Wait(ctx, instanceID, capability, transport)
}

func resolveLifecycleProxy(ctx context.Context, instanceID string, capability proxyrouting.Capability,
	transport proxyrouting.Transport, excluded map[string]struct{},
) (proxyrouting.OwnerRoute, error) {
	if !execendpoint.Default().IsPaused(instanceID) {
		return resolveDirectProxyOwner(ctx, instanceID, capability, transport)
	}
	endpoint, err := proxyrouting.Select(ctx, capability, excluded)
	if err != nil {
		return proxyrouting.OwnerRoute{}, fmt.Errorf("select paused instance %s control gateway: %w", instanceID, err)
	}
	address := endpoint.GRPCAddress
	if transport == proxyrouting.TransportTCPTunnel {
		address = endpoint.TCPTunnelAddress
	}
	if !proxyrouting.IsRoutableAddress(address) {
		return proxyrouting.OwnerRoute{}, fmt.Errorf("selected paused instance %s gateway has no routable address", instanceID)
	}
	return proxyrouting.OwnerRoute{
		OwnerProxyID: endpoint.NodeID,
		Endpoint:     endpoint,
		Address:      address,
	}, nil
}

type frontendProxyGRPCClientPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newFrontendProxyGRPCClientPool() *frontendProxyGRPCClientPool {
	return &frontendProxyGRPCClientPool{conns: make(map[string]*grpc.ClientConn)}
}

func (p *frontendProxyGRPCClientPool) ClientForAddress(
	address string,
) (frontend_proxy.FrontendProxyServiceClient, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("frontend proxy address is empty")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[address]; ok {
		return frontend_proxy.NewFrontendProxyServiceClient(conn), nil
	}
	transportCredentials, err := frontendProxyTransportCredentials()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Hour,
			Timeout: frontendProxyKeepaliveTimeout,
		}),
	)
	if err != nil {
		return nil, err
	}
	p.conns[address] = conn
	return frontend_proxy.NewFrontendProxyServiceClient(conn), nil
}

func frontendProxyTransportCredentials() (credentials.TransportCredentials, error) {
	conf := config.GetConfig()
	if conf == nil || !conf.ComponentMTLSEnable {
		return insecure.NewCredentials(), nil
	}
	if conf.HTTPSConfig == nil {
		return nil, fmt.Errorf("frontend HTTPS config is required for component certificate paths")
	}
	tlsConfig, err := fronttls.NewComponentClientTLSConfig(*conf.HTTPSConfig)
	if err != nil {
		return nil, fmt.Errorf("load frontend proxy mTLS credentials: %w", err)
	}
	return credentials.NewTLS(tlsConfig), nil
}

func evictFrontendProxyClientOnError(factory frontendProxyServiceClientFactory, address string, err error) {
	if err == nil || address == "" || !isFrontendProxyTransportError(err) {
		return
	}
	var statusErr *frontendProxyStatusErr
	if errors.As(err, &statusErr) {
		return
	}
	var businessErr *frontendProxyBusinessErr
	if errors.As(err, &businessErr) {
		return
	}
	evictor, ok := factory.(frontendProxyServiceClientEvictor)
	if ok {
		evictor.EvictAddress(address)
	}
	proxyrouting.MarkSuspect(address)
}

func isFrontendProxyTransportError(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	var networkErr *net.OpError
	if errors.As(err, &networkErr) {
		return true
	}
	return status.Code(err) == codes.Unavailable
}

func (p *frontendProxyGRPCClientPool) EvictAddress(address string) {
	if strings.TrimSpace(address) == "" {
		return
	}
	p.mu.Lock()
	conn, ok := p.conns[address]
	if ok {
		delete(p.conns, address)
	}
	p.mu.Unlock()
	if ok {
		_ = conn.Close()
	}
}

func simpleRuntimeInvokeContext(options api.InvokeOptions) (context.Context, context.CancelFunc) {
	return simpleRuntimeInvokeContextWithParent(context.Background(), options, 0)
}

func simpleRuntimeInvokeContextWithParent(
	parent context.Context,
	options api.InvokeOptions,
	timeoutBuffer time.Duration,
) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := defaultFrontendProxyTimeout
	if options.Timeout > 0 {
		timeout = time.Duration(options.Timeout) * time.Second
	}
	return context.WithTimeout(parent, timeout+timeoutBuffer)
}

func functionInvokeTimeoutMs(options api.InvokeOptions) int64 {
	return timeoutSecondsToMs(options.Timeout)
}

func timeoutSecondsToMs(timeoutSeconds int) int64 {
	if timeoutSeconds <= 0 {
		return 0
	}
	return int64(timeoutSeconds) * int64(time.Second/time.Millisecond)
}

func rawSimpleRuntimeContext(parent context.Context, _ api.RawRequestOption) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithCancel(parent)
}

func newFrontendProxyRuntimeRequestID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:runtimeRequestIDLength]
}

func frontendRequestContextFromInvokeOptions(frontendClientID, tenantID, requestID string,
	options api.InvokeOptions,
) *frontend_proxy.FrontendRequestContext {
	ctx := &frontend_proxy.FrontendRequestContext{
		FrontendClientID: frontendClientID,
		TenantID:         tenantID,
		RequestID:        requestID,
		TraceID:          options.TraceID,
	}
	if traceParent := options.CustomExtensions[traceParentExtensionKey]; traceParent != "" {
		ctx.Labels = map[string]string{traceParentExtensionKey: traceParent}
	}
	return ctx
}

func rawFrontendRequestContext(frontendClientID, requestID, traceID string, option api.RawRequestOption,
	tenantID ...string,
) *frontend_proxy.FrontendRequestContext {
	ctx := &frontend_proxy.FrontendRequestContext{
		FrontendClientID: frontendClientID,
		RequestID:        requestID,
		TraceID:          traceID,
	}
	if len(tenantID) > 0 {
		ctx.TenantID = tenantID[0]
	}
	if option.TraceParent != "" {
		ctx.Labels = map[string]string{traceParentExtensionKey: option.TraceParent}
	}
	return ctx
}

func applyRawRequestOptionsToInvoke(invokeReq *core.InvokeRequest, option api.RawRequestOption) {
	if invokeReq == nil || option.TraceParent == "" {
		return
	}
	if invokeReq.InvokeOptions == nil {
		invokeReq.InvokeOptions = &core.InvokeOptions{}
	}
	if invokeReq.InvokeOptions.CustomTag == nil {
		invokeReq.InvokeOptions.CustomTag = map[string]string{}
	}
	invokeReq.InvokeOptions.CustomTag[traceParentExtensionKey] = option.TraceParent
}

func applyRawRequestOptionsToCreate(createReq *core.CreateRequest, option api.RawRequestOption) {
	if createReq == nil {
		return
	}
	if createReq.CreateOptions == nil {
		createReq.CreateOptions = map[string]string{}
	}
	if _, ok := createReq.CreateOptions[frontendProxyCreateSourceKey]; !ok {
		createReq.CreateOptions[frontendProxyCreateSourceKey] = frontendProxyCreateSource
	}
	if option.TraceParent != "" {
		createReq.CreateOptions[traceParentExtensionKey] = option.TraceParent
	}
}

func tenantIDFromCreateRequest(createReq *core.CreateRequest) string {
	if createReq == nil {
		return ""
	}
	for _, key := range []string{"tenantID", "tenantId", "tenant"} {
		if value := createReq.GetCreateOptions()[key]; value != "" {
			return value
		}
	}
	function := strings.Trim(createReq.GetFunction(), "/")
	if idx := strings.Index(function, "/"); idx > 0 {
		return function[:idx]
	}
	return ""
}

func tenantIDFromInvokeRequest(invokeReq *core.InvokeRequest) string {
	if invokeReq == nil {
		return ""
	}
	function := strings.Trim(invokeReq.GetFunction(), "/")
	if idx := strings.Index(function, "/"); idx > 0 {
		return function[:idx]
	}
	return ""
}

func checkFrontendProxyStatus(operation string, status *frontend_proxy.FrontendProxyStatus) error {
	if status == nil || status.GetCode() == common.ErrorCode_ERR_NONE {
		return nil
	}
	return frontendProxyStatusError(operation, status)
}

func isFrontendProxyCreatePreDispatchStatus(err error) bool {
	var statusErr *frontendProxyStatusErr
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.operation == "create" && statusErr.retryReason == frontendProxyControlNotWired
}

func isFrontendProxyRouteStaleStatus(err error) bool {
	var statusErr *frontendProxyStatusErr
	return errors.As(err, &statusErr) && statusErr.retryable && statusErr.retryReason == "route-stale"
}

func isPausedLifecycleGatewayRetryable(err error) bool {
	var businessErr *frontendProxyBusinessErr
	return errors.As(err, &businessErr) && businessErr.code == common.ErrorCode_ERR_INNER_COMMUNICATION
}

type frontendProxyStatusErr struct {
	operation   string
	code        common.ErrorCode
	message     string
	retryable   bool
	retryReason string
}

type frontendProxyBusinessErr struct {
	operation string
	code      common.ErrorCode
	message   string
}

func (e *frontendProxyBusinessErr) directProxyErrorMetadata() DirectProxyErrorMetadata {
	if e == nil {
		return DirectProxyErrorMetadata{}
	}
	return DirectProxyErrorMetadata{Code: int(e.code)}
}

func (e *frontendProxyBusinessErr) Error() string {
	if e == nil {
		return "frontend proxy failed with nil business error"
	}
	// Business errors originate from Runtime or FunctionProxy and become the
	// user-facing frontend response. Keep the historical libruntime contract by
	// returning the upstream message unchanged; operation and code remain on the
	// typed error for logging, classification, and retry decisions.
	return e.message
}

func (e *frontendProxyStatusErr) Error() string {
	if e == nil {
		return "request failed"
	}
	return fmt.Sprintf("%s failed, code: %v, message: %s",
		e.operation, e.code, e.message)
}

func (e *frontendProxyStatusErr) directProxyErrorMetadata() DirectProxyErrorMetadata {
	if e == nil {
		return DirectProxyErrorMetadata{}
	}
	return DirectProxyErrorMetadata{
		Code:        int(e.code),
		Retryable:   e.retryable,
		RetryReason: e.retryReason,
	}
}

func frontendProxyStatusError(operation string, status *frontend_proxy.FrontendProxyStatus) error {
	if status == nil {
		return fmt.Errorf("frontend proxy %s failed with nil status", operation)
	}
	return &frontendProxyStatusErr{
		operation:   operation,
		code:        status.GetCode(),
		message:     status.GetMessage(),
		retryable:   status.GetRetryable(),
		retryReason: status.GetRetryReason(),
	}
}

func frontendProxyBusinessError(operation string, code common.ErrorCode, message string) error {
	return &frontendProxyBusinessErr{
		operation: operation,
		code:      code,
		message:   message,
	}
}

func marshalRuntimeNotifyFromCallResult(callResult *core.CallResult) ([]byte, error) {
	return marshalRuntimeNotifyFromCallResultWithRequestID(callResult, callResult.GetRequestID())
}

func marshalRuntimeNotifyFromCallResultWithRequestID(
	callResult *core.CallResult,
	requestID string,
) ([]byte, error) {
	var out []byte
	if requestID != "" {
		out = protowire.AppendTag(out, runtimeNotifyRequestIDField, protowire.BytesType)
		out = protowire.AppendString(out, requestID)
	}
	out = protowire.AppendTag(out, runtimeNotifyCodeField, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(callResult.GetCode()))
	if callResult.GetMessage() != "" {
		out = protowire.AppendTag(out, runtimeNotifyMessageField, protowire.BytesType)
		out = protowire.AppendString(out, callResult.GetMessage())
	}
	for _, smallObject := range callResult.GetSmallObjects() {
		payload, err := proto.Marshal(smallObject)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal frontend proxy invoke small object: %w", err)
		}
		out = protowire.AppendTag(out, runtimeNotifySmallObjectField, protowire.BytesType)
		out = protowire.AppendBytes(out, payload)
	}
	for _, stackTraceInfo := range callResult.GetStackTraceInfos() {
		payload, err := proto.Marshal(stackTraceInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal frontend proxy create stack trace info: %w", err)
		}
		out = protowire.AppendTag(out, runtimeNotifyStackTraceField, protowire.BytesType)
		out = protowire.AppendBytes(out, payload)
	}
	if runtimeInfo := callResult.GetRuntimeInfo(); runtimeInfo != nil {
		payload, err := proto.Marshal(runtimeInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal frontend proxy create runtime info: %w", err)
		}
		out = protowire.AppendTag(out, runtimeNotifyRuntimeInfoField, protowire.BytesType)
		out = protowire.AppendBytes(out, payload)
	}
	// The raw frontend create response is consumed by the external libruntime
	// GwClient, whose NotifyRequest wire contract uses field 8 for instanceID.
	// Do not replace this with functionsystem's internal readyInstance message:
	// that is a different NotifyRequest definition on a different boundary.
	if callResult.GetInstanceID() != "" {
		out = protowire.AppendTag(out, runtimeNotifyInstanceIDField, protowire.BytesType)
		out = protowire.AppendString(out, callResult.GetInstanceID())
	}
	return out, nil
}

func createReadyCallResultFromResponse(resp *frontend_proxy.CreateInstanceResponse) (*core.CallResult, error) {
	if resp == nil {
		return nil, fmt.Errorf("frontend proxy create response is nil")
	}
	if callResult, typedFieldKnown, err := typedCreateReadyCallResultFromResponse(resp); err != nil || typedFieldKnown {
		return callResult, err
	}
	unknown := resp.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		number, wireType, n := protowire.ConsumeTag(unknown)
		if n < 0 {
			return nil, fmt.Errorf("failed to parse frontend proxy create unknown field tag: %v", protowire.ParseError(n))
		}
		unknown = unknown[n:]
		if number == createReadyCallResultFieldNumber && wireType == protowire.BytesType {
			payload, n := protowire.ConsumeBytes(unknown)
			if n < 0 {
				return nil, fmt.Errorf("failed to parse frontend proxy create ready call result: %v", protowire.ParseError(n))
			}
			callResult := &core.CallResult{}
			if err := proto.Unmarshal(payload, callResult); err != nil {
				return nil, fmt.Errorf("failed to unmarshal frontend proxy create ready call result: %w", err)
			}
			return callResult, nil
		}
		n = protowire.ConsumeFieldValue(number, wireType, unknown)
		if n < 0 {
			return nil, fmt.Errorf("failed to skip frontend proxy create unknown field %d: %v",
				number, protowire.ParseError(n))
		}
		unknown = unknown[n:]
	}
	return nil, nil
}

func typedCreateReadyCallResultFromResponse(
	resp *frontend_proxy.CreateInstanceResponse,
) (*core.CallResult, bool, error) {
	message := resp.ProtoReflect()
	field := message.Descriptor().Fields().ByName(protoreflect.Name("callResult"))
	if field == nil {
		return nil, false, nil
	}
	if field.Kind() != protoreflect.MessageKind {
		return nil, true, fmt.Errorf("frontend proxy create callResult field has invalid kind %s", field.Kind())
	}
	if !message.Has(field) {
		return nil, false, nil
	}
	payload, err := proto.Marshal(message.Get(field).Message().Interface())
	if err != nil {
		return nil, true, fmt.Errorf("failed to marshal typed frontend proxy create ready call result: %w", err)
	}
	callResult := &core.CallResult{}
	if err := proto.Unmarshal(payload, callResult); err != nil {
		return nil, true, fmt.Errorf("failed to unmarshal typed frontend proxy create ready call result: %w", err)
	}
	return callResult, true, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func currentFrontendClientID() string {
	if podName := os.Getenv(constant.PodNameEnvKey); podName != "" {
		return "frontend:" + podName
	}
	return "frontend"
}

func convertSimpleRuntimeArgs(args []api.Arg) []*common.Arg {
	converted := make([]*common.Arg, 0, len(args))
	for _, arg := range args {
		converted = append(converted, &common.Arg{
			Type:       common.Arg_ArgType(arg.Type),
			Value:      arg.Data,
			NestedRefs: arg.NestedObjectIDs,
		})
	}
	return converted
}

func convertSimpleRuntimeInvokeArgs(funcMeta api.FunctionMeta, args []api.Arg) []*common.Arg {
	converted := convertSimpleRuntimeArgs(args)
	if funcMeta.Api == api.PosixApi {
		return converted
	}
	if simpleRuntimeArgsNeedFaaSPrefix(funcMeta.Api) {
		converted = prefixSimpleRuntimeFaaSInvokeArgs(converted)
	}
	metadata := buildSimpleRuntimeInvokeMetadata(funcMeta)
	withMetadata := make([]*common.Arg, 0, len(converted)+1)
	withMetadata = append(withMetadata, &common.Arg{
		Type:  common.Arg_VALUE,
		Value: metadata,
	})
	withMetadata = append(withMetadata, converted...)
	return withMetadata
}

func convertSimpleRuntimeCreateArgs(funcMeta api.FunctionMeta, args []api.Arg, codePaths []string) []*common.Arg {
	converted := convertSimpleRuntimeArgs(args)
	if funcMeta.Api == api.PosixApi {
		return converted
	}
	if simpleRuntimeArgsNeedFaaSPrefix(funcMeta.Api) {
		converted = prefixSimpleRuntimeFaaSInvokeArgs(converted)
	}
	// Libruntime used to prepend MetaData before sending a non-POSIX create.
	// The direct Proxy path must preserve that runtime wire contract itself.
	withMetadata := make([]*common.Arg, 0, len(converted)+1)
	metaValue := buildSimpleRuntimeCreateMetadata(funcMeta, codePaths)
	withMetadata = append(withMetadata, &common.Arg{
		Type:  common.Arg_VALUE,
		Value: metaValue,
	})
	// The FaaS executor's parse_faas_param (faas_executor.py:339) strips a 16-byte
	// METALEN header from each user arg before json.loads. function_agent's
	// BuildCreateArgs (static_function_util.cpp:BuildCreateArgs) prepends 16 null
	// bytes to every user create arg; the direct-proxy invoke path does the same
	// via prefixSimpleRuntimeFaaSInvokeArgsForRPC. The create path here used to
	// omit it, so parse_faas_param got bare JSON (e.g. "{}" → empty context_meta),
	// leaving _ENV_STORAGE fields unset and invoke failing on
	// update_user_agency. Prepend the same 16-byte prefix to non-MetaData user
	// args so the wire contract matches. MetaData (args[0]) is untouched: the C++
	// side parses it as protobuf, not via parse_faas_param.
	for _, arg := range converted {
		next := arg
		if next != nil && next.GetType() == common.Arg_VALUE &&
			!bytes.HasPrefix(next.GetValue(), []byte(simpleRuntimeFaaSMetaPrefix)) {
			value := make([]byte, 0, len(simpleRuntimeFaaSMetaPrefix)+len(next.GetValue()))
			value = append(value, simpleRuntimeFaaSMetaPrefix...)
			value = append(value, next.GetValue()...)
			next = &common.Arg{Type: next.GetType(), Value: value, NestedRefs: next.GetNestedRefs()}
		}
		withMetadata = append(withMetadata, next)
	}
	// [YRPROBE4201] log the create args this direct-proxy path actually hands off to
	// function_proxy. faas non-Posix init needs args_size>=2: args[0]=MetaData,
	// args[1+]=user context_meta args. If this logs size==1 the user args never left
	// the frontend (the prefix loop above is currently commented out).
	log.GetLogger().Infof("[YRPROBE4201][go] convertSimpleRuntimeCreateArgs funcMeta.Api=%v in=%d out=%d codePaths=%v",
		funcMeta.Api, len(args), len(withMetadata), codePaths)
	for i, a := range withMetadata {
		hl := 16
		if len(a.GetValue()) < hl {
			hl = len(a.GetValue())
		}
		log.GetLogger().Infof("[YRPROBE4201][go]   out[%d] type=%v valueLen=%d head=%x",
			i, a.GetType(), len(a.GetValue()), a.GetValue()[:hl])
	}
	return withMetadata
}

// funcSpecDataArgValue extracts the funcSpecData (args[0]) as a string for diagnostics.
func funcSpecDataArgValue(args []api.Arg) string {
	for _, a := range args {
		if a.Type == api.Value && len(a.Data) > 0 {
			return string(a.Data)
		}
	}
	return "<empty>"
}

func convertSimpleRuntimeInvokeArgsForRPC(funcMeta api.FunctionMeta, args []api.Arg) ([]*common.Arg, func()) {
	converted := convertSimpleRuntimeArgs(args)
	release := func() {}
	if funcMeta.Api == api.PosixApi {
		return converted, release
	}
	if simpleRuntimeArgsNeedFaaSPrefix(funcMeta.Api) {
		converted, release = prefixSimpleRuntimeFaaSInvokeArgsForRPC(converted)
	}
	metadata := buildSimpleRuntimeInvokeMetadata(funcMeta)
	withMetadata := make([]*common.Arg, 0, len(converted)+1)
	withMetadata = append(withMetadata, &common.Arg{Type: common.Arg_VALUE, Value: metadata})
	withMetadata = append(withMetadata, converted...)
	return withMetadata, release
}

func simpleRuntimeArgsNeedFaaSPrefix(apiType api.ApiType) bool {
	return apiType == api.FaaSApi || apiType == api.ServeApi
}

func normalizeSimpleRuntimeInvokePayload(funcMeta api.FunctionMeta, payload []byte) []byte {
	if !simpleRuntimeArgsNeedFaaSPrefix(funcMeta.Api) {
		return payload
	}
	prefix := []byte(simpleRuntimeFaaSMetaPrefix)
	if !bytes.HasPrefix(payload, prefix) {
		return payload
	}
	return payload[len(prefix):]
}

func prefixSimpleRuntimeFaaSInvokeArgs(args []*common.Arg) []*common.Arg {
	prefixed := make([]*common.Arg, 0, len(args))
	for _, arg := range args {
		if arg == nil {
			prefixed = append(prefixed, nil)
			continue
		}
		next := &common.Arg{
			Type:       arg.GetType(),
			Value:      arg.GetValue(),
			NestedRefs: arg.GetNestedRefs(),
		}
		if next.GetType() == common.Arg_VALUE && !bytes.HasPrefix(next.GetValue(), []byte(simpleRuntimeFaaSMetaPrefix)) {
			value := make([]byte, 0, len(simpleRuntimeFaaSMetaPrefix)+len(next.GetValue()))
			value = append(value, simpleRuntimeFaaSMetaPrefix...)
			next.Value = append(value, next.GetValue()...)
		}
		prefixed = append(prefixed, next)
	}
	return prefixed
}

const (
	simpleRuntimeSmallValueBufferSize  = 4 << 10
	simpleRuntimeMediumValueBufferSize = 128 << 10
)

func prefixSimpleRuntimeFaaSInvokeArgsForRPC(args []*common.Arg) ([]*common.Arg, func()) {
	prefixed := make([]*common.Arg, 0, len(args))
	type pooledBuffer struct {
		pool  *sync.Pool
		value []byte
	}
	pooled := make([]pooledBuffer, 0, len(args))
	for _, arg := range args {
		if arg == nil {
			prefixed = append(prefixed, nil)
			continue
		}
		next := &common.Arg{Type: arg.GetType(), Value: arg.GetValue(), NestedRefs: arg.GetNestedRefs()}
		if next.GetType() == common.Arg_VALUE && !bytes.HasPrefix(next.GetValue(), []byte(simpleRuntimeFaaSMetaPrefix)) {
			required := len(simpleRuntimeFaaSMetaPrefix) + len(next.GetValue())
			value, pool := acquireSimpleRuntimeValueBuffer(required)
			value = append(value, simpleRuntimeFaaSMetaPrefix...)
			value = append(value, next.GetValue()...)
			next.Value = value
			if pool != nil {
				pooled = append(pooled, pooledBuffer{pool: pool, value: value})
			}
		}
		prefixed = append(prefixed, next)
	}
	return prefixed, func() {
		for _, item := range pooled {
			item.pool.Put(item.value[:0])
		}
	}
}

func acquireSimpleRuntimeValueBuffer(required int) ([]byte, *sync.Pool) {
	var pool *sync.Pool
	capacity := required
	switch {
	case required <= simpleRuntimeSmallValueBufferSize:
		pool, capacity = &simpleRuntimeSmallValueBufferPool, simpleRuntimeSmallValueBufferSize
	case required <= simpleRuntimeMediumValueBufferSize:
		pool, capacity = &simpleRuntimeMediumValueBufferPool, simpleRuntimeMediumValueBufferSize
	default:
		return make([]byte, 0, required), nil
	}
	if cached := pool.Get(); cached != nil {
		return cached.([]byte)[:0], pool
	}
	return make([]byte, 0, capacity), pool
}

func buildSimpleRuntimeInvokeMetadata(funcMeta api.FunctionMeta) []byte {
	var metadata []byte
	metadata = appendProtoVarint(metadata, 1, uint64(1)) // libruntime.InvokeFunction
	metadata = appendProtoBytes(metadata, functionMetaModuleNameField, buildSimpleRuntimeFunctionMeta(funcMeta))
	metadata = appendProtoBytes(metadata, functionMetaClassNameField, buildSimpleRuntimeInvocationMeta())
	return metadata
}

func buildSimpleRuntimeCreateMetadata(funcMeta api.FunctionMeta, codePaths []string) []byte {
	var metadata []byte
	// InvokeType.CreateInstance is protobuf enum value zero and is therefore
	// represented by the field's default value. FunctionMeta remains required
	// by the runtime initializer.
	metadata = appendProtoBytes(metadata, functionMetaModuleNameField, buildSimpleRuntimeFunctionMeta(funcMeta))
	// MetaConfig.codePaths (repeated string, MetaData field 3.config, field 2) is what
	// the runtime's InitCall reads to drive loadFunctionCallback -> CodeManager.load_functions.
	// Without it faas_init_handler reports "failed to find init handler: None" (4201)
	// because entry_map never gets the userInitEntry/userCallEntry keys. Must be emitted
	// ONCE: protobuf merges repeated-field occurrences, so emitting config twice doubled
	// codePaths (3 -> 6) and load_functions' _are_faas_entries rejected len>3 (else branch,
	// no entry_map) -> 4201. buildSimpleRuntimeMetaConfig uses appendProtoBytes (not
	// appendProtoString) so an empty init entry ("") is still emitted as a zero-length
	// string; CodeManager.load_functions requires len(codePaths) in [2,3] to treat them
	// as FaaS entries, skipping empty strings would drop the count below 2.
	if config := buildSimpleRuntimeMetaConfig(codePaths); len(config) != 0 {
		metadata = appendProtoBytes(metadata, metaDataConfigField, config)
	}
	metadata = appendProtoBytes(metadata, functionMetaClassNameField, buildSimpleRuntimeInvocationMeta())
	return metadata
}

func buildSimpleRuntimeMetaConfig(codePaths []string) []byte {
	var config []byte
	for _, codePath := range codePaths {
		config = appendProtoBytes(config, metaConfigCodePathsField, []byte(codePath))
	}
	return config
}

func buildSimpleRuntimeFunctionMeta(funcMeta api.FunctionMeta) []byte {
	var payload []byte
	payload = appendProtoString(payload, functionMetaAppNameField, funcMeta.AppName)
	payload = appendProtoString(payload, functionMetaModuleNameField, funcMeta.ModuleName)
	payload = appendProtoString(payload, functionMetaFunctionNameField, funcMeta.FuncName)
	payload = appendProtoString(payload, functionMetaClassNameField, funcMeta.ClassName)
	payload = appendProtoVarint(payload, functionMetaLanguageField, uint64(funcMeta.Language))
	payload = appendProtoString(payload, functionMetaSignatureField, funcMeta.Sig)
	payload = appendProtoVarint(payload, functionMetaAPIField, uint64(funcMeta.Api))
	if funcMeta.Name != nil {
		payload = appendProtoString(payload, functionMetaNameField, *funcMeta.Name)
	}
	if funcMeta.Namespace != nil {
		payload = appendProtoString(payload, functionMetaNamespaceField, *funcMeta.Namespace)
	}
	payload = appendProtoString(payload, functionMetaIDField, funcMeta.FuncID)
	return payload
}

func buildSimpleRuntimeInvocationMeta() []byte {
	var payload []byte
	payload = appendProtoString(payload, 1, currentFrontendClientID())
	return payload
}

func appendProtoString(payload []byte, field protowire.Number, value string) []byte {
	if value == "" {
		return payload
	}
	return appendProtoBytes(payload, field, []byte(value))
}

func appendProtoBytes(payload []byte, field protowire.Number, value []byte) []byte {
	payload = protowire.AppendTag(payload, field, protowire.BytesType)
	return protowire.AppendBytes(payload, value)
}

func appendProtoVarint(payload []byte, field protowire.Number, value uint64) []byte {
	payload = protowire.AppendTag(payload, field, protowire.VarintType)
	return protowire.AppendVarint(payload, value)
}

func convertSimpleRuntimeInvokeOptions(options api.InvokeOptions) *core.InvokeOptions {
	customTag := make(map[string]string, len(options.CustomExtensions)+len(options.CreateOpt)+3)
	for key, value := range options.CustomExtensions {
		customTag[key] = value
	}
	for key, value := range options.CreateOpt {
		customTag[key] = value
	}
	// Keep the wire representation used by libruntime. FunctionProxy and
	// runtime already consume these reserved tags; direct frontend calls must
	// encode the same options instead of introducing a second protocol.
	if options.InstanceSession != nil && options.InstanceSession.SessionID != "" {
		customTag["YR_AGENT_SESSION_ID"] = options.InstanceSession.SessionID
	}
	if options.IsInterrupted {
		customTag["IS_INTERRUPTED"] = "true"
	}
	if options.ForceInvoke {
		customTag["ENABLE_FORCE_INVOKE"] = ""
	}
	return &core.InvokeOptions{
		CustomTag:        customTag,
		BypassDatasystem: options.BypassDataSystem,
	}
}

func convertSimpleRuntimeCreateOptions(options api.InvokeOptions) map[string]string {
	createOptions := make(map[string]string, len(options.CustomExtensions)+len(options.CreateOpt)+1)
	for key, value := range options.CustomExtensions {
		createOptions[key] = value
	}
	for key, value := range options.CreateOpt {
		createOptions[key] = value
	}
	if _, ok := createOptions[frontendProxyCreateSourceKey]; !ok {
		createOptions[frontendProxyCreateSourceKey] = frontendProxyCreateSource
	}
	return createOptions
}

// convertSimpleRuntimeSchedulingOps maps InvokeOptions into CreateRequest.SchedulingOps so
// the proxy can read CPU/Memory from the structured scheduling field. CPU/Memory are emitted
// only when non-zero (absent → executor default); values are passed through unvalidated.
func convertSimpleRuntimeSchedulingOps(options api.InvokeOptions) *core.SchedulingOptions {
	resources := make(map[string]float64, 2+len(options.CustomResources))
	if options.Cpu != 0 {
		resources[constant.ResourceCPUName] = float64(options.Cpu)
	}
	if options.Memory != 0 {
		resources[constant.ResourceMemoryName] = float64(options.Memory)
	}
	for k, v := range options.CustomResources {
		resources[k] = v
	}

	if len(resources) == 0 && options.Priority == 0 && options.ScheduleTimeoutMs == 0 {
		return nil
	}
	return &core.SchedulingOptions{
		Priority:          int32(options.Priority),
		Resources:         resources,
		ScheduleTimeoutMs: options.ScheduleTimeoutMs,
	}
}

func firstArgTenantID(args []api.Arg) string {
	for _, arg := range args {
		if arg.TenantID != "" {
			return arg.TenantID
		}
	}
	return ""
}

// InvokeAgentInstanceRaw sends an invoke request to an agent (FaaS executor) instance
// through the raw invoke channel while satisfying the executor's non-Posix arg contract.
//
// The raw path (InvokeInstanceRawWithContext -> InvokeByInstanceIdRaw) transparently
// forwards the InvokeRequest bytes, so callers that target a FaaS apiType executor
// (e.g. 0-system-faasExecutorPython3.11) must themselves prepend the serialized MetaData
// protobuf as args[0] and 16-byte META_PREFIX on each VALUE arg -- exactly what
// convertSimpleRuntimeInvokeArgsForRPC does on the InvokeByInstanceID (simple-runtime
// proxy) path. This helper centralizes that encoding so HTTP entrypoints (agent
// /api/agent/:id/invoke) reuse the same layout as the working kernel FaaS invoke path
// and the C++ executor's ParseMetaData (invoke_adaptor.cpp) no longer fails on a
// bare traceID at args[0].
//
// payloads are the user-visible arg bytes in order (e.g. [traceID, callReqJSON]); they
// are prefixed with META_PREFIX internally. The returned bytes are a marshaled
// runtime.NotifyRequest, mirroring InvokeInstanceRawWithContext.
func InvokeAgentInstanceRaw(ctx context.Context, client Client, funcKey, instanceID,
	traceID string, payloads [][]byte, option api.RawRequestOption) ([]byte, error) {
	args := make([]api.Arg, 0, len(payloads))
	for _, p := range payloads {
		args = append(args, api.Arg{Type: api.Value, Data: p})
	}
	funcMeta := api.FunctionMeta{FuncID: funcKey, Api: api.FaaSApi}
	encodedArgs, releaseEncodedArgs := convertSimpleRuntimeInvokeArgsForRPC(funcMeta, args)
	defer releaseEncodedArgs()
	invokeReq := &core.InvokeRequest{
		Function:   funcKey,
		Args:       encodedArgs,
		InstanceID: instanceID,
		RequestID:  uuid.New().String(),
		TraceID:    traceID,
	}
	invokeReqRaw, err := proto.Marshal(invokeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal agent invoke request: %w", err)
	}
	// Delegate to the libruntime raw-invoke seam. The legacy
	// InvokeInstanceRawWithContext package-level helper was removed when the
	// Client contract was narrowed (see commit 9c23b6f); InvokeAgentInstanceRaw
	// now reaches clientLibruntime.InvokeByInstanceIdRaw directly. The returned
	// bytes are a marshaled runtime.NotifyRequest, matching what
	// parseAgentInvokeResponse expects to proto.Unmarshal.
	dc, ok := client.(*defaultClient)
	if !ok {
		return nil, fmt.Errorf("agent raw invoke requires *defaultClient, got %T", client)
	}
	return dc.clientLibruntime.InvokeByInstanceIdRaw(invokeReqRaw, option)
}
