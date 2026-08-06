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
)

var frontendProxyRequestSeq atomic.Uint64

var (
	simpleRuntimeSmallValueBufferPool  sync.Pool
	simpleRuntimeMediumValueBufferPool sync.Pool
)

var (
	frontendRouteLifecycleObserverMu sync.RWMutex
	frontendRouteLifecycleObserver   func(frontendRouteLifecycleEvent)
)

const (
	frontendProxyRouteKey                               = "YR_ROUTE"
	frontendProxyCreateSourceKey                        = "source"
	frontendProxyCreateSource                           = "frontend"
	frontendProxyControlNotWired                        = "control-path-not-wired"
	simpleRuntimeFaaSMetaPrefix                         = "0000000000000000"
	defaultFrontendProxyTimeout                         = 60 * time.Second
	frontendProxyKeepaliveTimeout                       = 10 * time.Second
	frontendProxyFileTransferChunkSize                  = 4 * 1024 * 1024
	runtimeRequestIDLength                              = 18
	createReadyCallResultFieldNumber   protowire.Number = 4
	runtimeNotifyRequestIDField        protowire.Number = 1
	runtimeNotifyCodeField             protowire.Number = 2
	runtimeNotifyMessageField          protowire.Number = 3
	runtimeNotifySmallObjectField      protowire.Number = 4
	runtimeNotifyStackTraceField       protowire.Number = 5
	runtimeNotifyRuntimeInfoField      protowire.Number = 7
	runtimeNotifyInstanceIDField       protowire.Number = 8
	functionMetaAppNameField           protowire.Number = 1
	functionMetaModuleNameField        protowire.Number = 2
	functionMetaFunctionNameField      protowire.Number = 3
	functionMetaClassNameField         protowire.Number = 4
	functionMetaLanguageField          protowire.Number = 5
	functionMetaSignatureField         protowire.Number = 7
	functionMetaAPIField               protowire.Number = 8
	functionMetaNameField              protowire.Number = 9
	functionMetaNamespaceField         protowire.Number = 10
	functionMetaIDField                protowire.Number = 11
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

func observeFrontendRouteLifecycle(req simpleRuntimeKillRequest, outcome string,
	change instancemanager.RouteOnlyInstanceChange,
) {
	event := frontendRouteLifecycleEvent{
		Operation:          "kill",
		Outcome:            outcome,
		CleanupOutcome:     "route-hint-cleared",
		RequestID:          req.requestID,
		TraceID:            firstNonEmpty(req.options.TraceID, req.options.CustomExtensions[traceParentExtensionKey]),
		InstanceID:         req.instanceID,
		OwningProxyID:      change.Before.FunctionProxyID,
		RoutePresentBefore: change.Before.Present,
		RoutePresentAfter:  change.After.Present,
		ReplayAttempted:    false,
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
	ctx, cancel := simpleRuntimeInvokeContext(req.options)
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
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("frontend proxy invoke response is nil")
	}
	if err := checkFrontendProxyStatus("invoke", resp.GetStatus()); err != nil {
		return nil, err
	}
	callResult := resp.GetCallResult()
	if callResult == nil {
		return nil, fmt.Errorf("frontend proxy invoke missing call result")
	}
	if callResult.GetCode() != common.ErrorCode_ERR_NONE {
		return nil, frontendProxyBusinessError("invoke call result", callResult.GetCode(), callResult.GetMessage())
	}
	smallObjects := callResult.GetSmallObjects()
	if len(smallObjects) == 0 {
		return nil, fmt.Errorf("frontend proxy invoke call result has no small object payload")
	}
	return normalizeSimpleRuntimeInvokePayload(req.funcMeta, smallObjects[0].GetValue()), nil
}

func (c *grpcFrontendProxyLifecycleClient) CreateInstance(req simpleRuntimeCreateRequest) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("frontend proxy grpc client is nil")
	}
	requestID := fmt.Sprintf("frontend-proxy-create-%d", frontendProxyRequestSeq.Add(1))
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
			Args:          convertSimpleRuntimeArgs(req.args),
			RequestID:     requestID,
			TraceID:       req.options.TraceID,
			CreateOptions: convertSimpleRuntimeCreateOptions(req.options),
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
	if routeAddress := resp.GetRouteAddress(); routeAddress != "" {
		instancemanager.RecordRouteOnlyInstance(req.funcMeta.FuncID, instanceID, routeAddress)
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
	externalRequestID := createReq.GetRequestID()
	requestID := newFrontendProxyLifecycleCorrelationID("create")
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
		Create: createReq,
	})
	if err != nil {
		return nil, err
	}
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
		if routeAddress := resp.GetRouteAddress(); routeAddress != "" && instanceID != "" {
			instancemanager.RecordRouteOnlyInstance(createReq.GetFunction(), instanceID, routeAddress)
		}
	}
	return marshalRuntimeNotifyFromCallResultWithRequestID(callResult, firstNonEmpty(externalRequestID, requestID))
}

func (c *grpcFrontendProxyLifecycleClient) KillInstance(req simpleRuntimeKillRequest) error {
	if c.client == nil {
		return fmt.Errorf("frontend proxy grpc client is nil")
	}
	requestID := firstNonEmpty(req.requestID, fmt.Sprintf("frontend-proxy-kill-%d", frontendProxyRequestSeq.Add(1)))
	ctx, cancel := simpleRuntimeInvokeContextWithParent(req.ctx, req.options)
	defer cancel()
	resp, err := c.client.KillInstance(ctx, &frontend_proxy.KillInstanceRequest{
		Context: frontendRequestContextFromInvokeOptions(c.frontendClientID, req.tenantID, requestID, req.options),
		Kill: &core.KillRequest{
			InstanceID: req.instanceID,
			Signal:     int32(req.signal),
			Payload:    req.payload,
			RequestID:  requestID,
		},
	})
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("frontend proxy kill response is nil")
	}
	if err := checkFrontendProxyStatus("kill", resp.GetStatus()); err != nil {
		return err
	}
	killResp := resp.GetKill()
	if killResp == nil {
		return fmt.Errorf("frontend proxy kill missing kill response")
	}
	if killResp.GetCode() != common.ErrorCode_ERR_NONE {
		return frontendProxyBusinessError("kill", killResp.GetCode(), killResp.GetMessage())
	}
	change := instancemanager.RemoveRouteOnlyInstanceWithSnapshot(req.instanceID)
	observeFrontendRouteLifecycle(req, "success", change)
	return nil
}

func (c *grpcFrontendProxyInvokeClient) InvokeByInstanceIDRaw(req simpleRuntimeRawInvokeRequest) ([]byte, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	invokeReq := &core.InvokeRequest{}
	if err := proto.Unmarshal(req.invoke, invokeReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontend proxy invoke request: %w", err)
	}
	externalRequestID := invokeReq.GetRequestID()
	requestID := newFrontendProxyLifecycleCorrelationID("invoke")
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

// UploadFile streams the contents of reader to the owning frontend proxy of
// instanceID using the client-streaming UploadFile RPC. The reader is chunked
// at frontendProxyFileTransferChunkSize bytes; the final chunk carries isLast.
func (c *grpcFrontendProxyInvokeClient) UploadFile(ctx context.Context, instanceID string, path string,
	reader io.Reader, tenantID string,
) (*frontend_proxy.FileTransferResponse, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	if instanceID == "" {
		return nil, fmt.Errorf("frontend proxy upload file requires non-empty instance id")
	}
	if path == "" {
		return nil, fmt.Errorf("frontend proxy upload file requires non-empty path")
	}
	stream, err := c.client.UploadFile(ctx)
	if err != nil {
		return nil, err
	}
	requestID := newFrontendProxyRuntimeRequestID()
	chunkBuffer := make([]byte, frontendProxyFileTransferChunkSize)
	var offset int64
	for {
		n, readErr := reader.Read(chunkBuffer)
		if n > 0 {
			offset += int64(n)
			if err := stream.Send(&frontend_proxy.FileChunk{
				Context: &frontend_proxy.FrontendRequestContext{
					FrontendClientID: c.frontendClientID,
					TenantID:         tenantID,
					RequestID:        requestID,
				},
				InstanceID: instanceID,
				Path:       path,
				Offset:     offset - int64(n),
				Data:       chunkBuffer[:n],
				IsLast:     errors.Is(readErr, io.EOF),
			}); err != nil {
				return nil, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			readErr = fmt.Errorf("frontend proxy upload file read failed: %w", readErr)
			_, _ = stream.CloseAndRecv()
			return nil, readErr
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("frontend proxy upload file response is nil")
	}
	if err := checkFrontendProxyStatus("upload", resp.GetStatus()); err != nil {
		return nil, err
	}
	return resp, nil
}

// DownloadFile opens a server-streaming DownloadFile RPC against the owning
// frontend proxy of instanceID and returns the stream receiver for the caller
// to iterate. The caller is responsible for reading chunks until io.EOF.
func (c *grpcFrontendProxyInvokeClient) DownloadFile(ctx context.Context, instanceID string, path string,
	offset int64, tenantID string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	if c.client == nil {
		return nil, fmt.Errorf("frontend proxy grpc client is nil")
	}
	if instanceID == "" {
		return nil, fmt.Errorf("frontend proxy download file requires non-empty instance id")
	}
	if path == "" {
		return nil, fmt.Errorf("frontend proxy download file requires non-empty path")
	}
	requestID := newFrontendProxyRuntimeRequestID()
	return c.client.DownloadFile(ctx, &frontend_proxy.FileTransferRequest{
		Context: &frontend_proxy.FrontendRequestContext{
			FrontendClientID: c.frontendClientID,
			TenantID:         tenantID,
			RequestID:        requestID,
		},
		InstanceID: instanceID,
		Path:       path,
		Offset:     offset,
		ChunkSize:  frontendProxyFileTransferChunkSize,
	})
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
		return nil, err
	}
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	payload, err := newGRPCFrontendProxyInvokeClient(serviceClient, c.frontendClientID).InvokeByInstanceID(req)
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
	for {
		endpoint, ok := resolveNextFrontendProxyEndpoint(frontendProxyCapabilityCreate)
		if !ok {
			if lastErr != nil {
				return "", lastErr
			}
			return "", fmt.Errorf("no frontend proxy endpoint supports capability %s", frontendProxyCapabilityCreate)
		}
		if _, ok := tried[endpoint.Address]; ok {
			if lastErr != nil {
				return "", lastErr
			}
			return "", fmt.Errorf("no untried frontend proxy endpoint supports capability %s", frontendProxyCapabilityCreate)
		}
		tried[endpoint.Address] = struct{}{}
		serviceClient, err := c.clientFactory.ClientForAddress(endpoint.Address)
		if err != nil {
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.Address, err)
			lastErr = err
			continue
		}
		instanceID, err := newGRPCFrontendProxyLifecycleClient(serviceClient, c.frontendClientID).CreateInstance(req)
		if err != nil {
			if isFrontendProxyCreatePreDispatchStatus(err) {
				lastErr = err
				continue
			}
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.Address, err)
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
		endpoint, ok := resolveNextFrontendProxyEndpoint(frontendProxyCapabilityCreate)
		if !ok {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, fmt.Errorf("no frontend proxy endpoint supports capability %s", frontendProxyCapabilityCreate)
		}
		if _, ok := tried[endpoint.Address]; ok {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, fmt.Errorf("no untried frontend proxy endpoint supports capability %s", frontendProxyCapabilityCreate)
		}
		tried[endpoint.Address] = struct{}{}
		serviceClient, err := c.clientFactory.ClientForAddress(endpoint.Address)
		if err != nil {
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.Address, err)
			lastErr = err
			continue
		}
		notify, err := newGRPCFrontendProxyLifecycleClient(serviceClient, c.frontendClientID).CreateInstanceRaw(req)
		if err != nil {
			if isFrontendProxyCreatePreDispatchStatus(err) {
				lastErr = err
				continue
			}
			evictFrontendProxyClientOnError(c.clientFactory, endpoint.Address, err)
			return nil, err
		}
		return notify, nil
	}
}

func (c *routingFrontendProxyLifecycleClient) KillInstance(req simpleRuntimeKillRequest) error {
	if c == nil || c.clientFactory == nil {
		return fmt.Errorf("frontend proxy lifecycle routing client is not initialized")
	}
	address := resolveProxyAddressByKill(req)
	if !isHostPort(address) {
		if endpoint, ok := resolveSoleFrontendProxyEndpoint(frontendProxyCapabilityKill); ok {
			address = endpoint.Address
		}
	}
	if !isHostPort(address) {
		return fmt.Errorf("frontend proxy kill route is not configured for instance %s", req.instanceID)
	}
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return err
	}
	if err := newGRPCFrontendProxyLifecycleClient(serviceClient, c.frontendClientID).KillInstance(req); err != nil {
		if isFrontendProxyRouteStaleStatus(err) {
			// A stale owner response is a refresh hint, not permission to replay a
			// kill whose dispatch outcome may be unknown. Drop only the local
			// route-only hint so a later caller can resolve fresh watcher state.
			change := instancemanager.RemoveRouteOnlyInstanceWithSnapshot(req.instanceID)
			observeFrontendRouteLifecycle(req, "route-stale", change)
		}
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return err
	}
	return nil
}

func (c *routingFrontendProxyInvokeClient) InvokeByInstanceIDRaw(req simpleRuntimeRawInvokeRequest) ([]byte, error) {
	if c == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy routing client is not initialized")
	}
	invokeReq := &core.InvokeRequest{}
	if err := proto.Unmarshal(req.invoke, invokeReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontend proxy invoke route request: %w", err)
	}
	requestRoute := ""
	if invokeReq.GetInvokeOptions() != nil {
		requestRoute = invokeReq.GetInvokeOptions().GetCustomTag()[frontendProxyRouteKey]
	}
	// Raw function-system requests can carry an old YR_ROUTE from their serialized
	// request. Prefer the frontend watcher cache for the current owning proxy when
	// it is available; fall back to the request route only when the instance route
	// is not known locally. Do not retry after an invoke is sent, because unknown
	// status requests may not be safe to replay.
	address := resolveProxyAddressByRawInvoke(invokeReq)
	if !isHostPort(address) {
		address = resolveFrontendProxyAddressFromRoute(requestRoute)
	}
	if !isHostPort(address) {
		if endpoint, ok := resolveSoleFrontendProxyEndpoint(frontendProxyCapabilityInvoke); ok {
			address = endpoint.Address
		}
	}
	if !isHostPort(address) {
		return nil, fmt.Errorf("frontend proxy invoke route is not configured for instance %s", invokeReq.GetInstanceID())
	}
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

// resolveFileTransferProxyAddress resolves the owning frontend proxy gRPC
// address for instanceID. It mirrors the invoke route resolution: the instance
// watcher cache is preferred, and when the owning proxy is published by node
// id the proxy_discovery cache (LookupFrontendProxyEndpoint) resolves the
// concrete host:port. It falls back to the sole published invoke endpoint
// before failing so single-proxy rollouts keep working.
func resolveFileTransferProxyAddress(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("frontend proxy file transfer requires non-empty instance id")
	}
	instance := instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(instanceID)
	if instance != nil {
		if isHostPort(instance.FunctionProxyID) && !frontendProxyAddressIsSuspect(instance.FunctionProxyID) {
			return instance.FunctionProxyID, nil
		}
		if instance.FunctionProxyID != "" {
			if endpoint, ok := LookupFrontendProxyEndpoint(instance.FunctionProxyID, frontendProxyCapabilityInvoke); ok {
				return endpoint.Address, nil
			}
		}
		if endpoint, ok := resolveFrontendProxyEndpointByRuntimeHost(
			instance.RuntimeAddress, frontendProxyCapabilityInvoke); ok {
			return endpoint.Address, nil
		}
	}
	if endpoint, ok := LookupFrontendProxyEndpoint(instanceID, frontendProxyCapabilityInvoke); ok {
		return endpoint.Address, nil
	}
	if endpoint, ok := resolveSoleFrontendProxyEndpoint(frontendProxyCapabilityInvoke); ok {
		return endpoint.Address, nil
	}
	if instance != nil && isHostPort(instance.ProxyGrpcAddress) {
		return instance.ProxyGrpcAddress, nil
	}
	return "", fmt.Errorf("frontend proxy file transfer route is not configured for instance %s", instanceID)
}

// UploadFile resolves the owning proxy for instanceID, acquires a pooled gRPC
// client, and streams reader to the proxy via the client-streaming UploadFile RPC.
func (c *routingFrontendProxyInvokeClient) UploadFile(ctx context.Context, instanceID string, path string,
	reader io.Reader, tenantID string,
) (*frontend_proxy.FileTransferResponse, error) {
	if c == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy routing client is not initialized")
	}
	address, err := resolveFileTransferProxyAddress(instanceID)
	if err != nil {
		return nil, err
	}
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	resp, err := newGRPCFrontendProxyInvokeClient(serviceClient, c.frontendClientID).
		UploadFile(ctx, instanceID, path, reader, tenantID)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	return resp, nil
}

// DownloadFile resolves the owning proxy for instanceID, acquires a pooled gRPC
// client, and opens the server-streaming DownloadFile RPC. The caller reads
// chunks from the returned stream until io.EOF.
func (c *routingFrontendProxyInvokeClient) DownloadFile(ctx context.Context, instanceID string, path string,
	offset int64, tenantID string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	if c == nil || c.clientFactory == nil {
		return nil, fmt.Errorf("frontend proxy routing client is not initialized")
	}
	address, err := resolveFileTransferProxyAddress(instanceID)
	if err != nil {
		return nil, err
	}
	serviceClient, err := c.clientFactory.ClientForAddress(address)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	stream, err := newGRPCFrontendProxyInvokeClient(serviceClient, c.frontendClientID).
		DownloadFile(ctx, instanceID, path, offset, tenantID)
	if err != nil {
		evictFrontendProxyClientOnError(c.clientFactory, address, err)
		return nil, err
	}
	return stream, nil
}

type defaultFrontendProxyRouteResolver struct{}

func (defaultFrontendProxyRouteResolver) ResolveFrontendProxyAddress(req simpleRuntimeInvokeRequest) (string, error) {
	route := req.options.CreateOpt[frontendProxyRouteKey]
	// Prefer frontend's current instance cache over request-scoped route tags.
	// The cache is updated by the instance watcher and represents the current
	// owning proxy; a request tag can be absent or stale after reschedule/failover.
	if address := resolveProxyAddressByInstance(req); address != "" {
		return address, nil
	}
	if route == "" {
		return "", fmt.Errorf("frontend proxy route %s is empty for instance %s", frontendProxyRouteKey, req.instanceID)
	}
	if address := resolveFrontendProxyAddressFromRoute(route); address != "" {
		return address, nil
	}
	return "", fmt.Errorf("frontend proxy route %q is not resolvable for instance %s", route, req.instanceID)
}

func resolveFrontendProxyAddressFromRoute(route string) string {
	return resolveFrontendProxyAddressFromRouteWithCapability(route, frontendProxyCapabilityInvoke)
}

func resolveFrontendProxyAddressFromRouteWithCapability(route string, capability string) string {
	if isHostPort(route) {
		if frontendProxyAddressIsSuspect(route) {
			return ""
		}
		return route
	}
	if endpoint, ok := resolveFrontendProxyEndpointByNode(route, capability); ok {
		return endpoint.Address
	}
	return ""
}

func resolveFrontendProxyEndpointByNode(nodeID string, capability string) (frontendProxyEndpoint, bool) {
	return lookupFrontendProxyEndpointByNode(nodeID, capability)
}

func lookupFrontendProxyEndpointByNode(nodeID string, capability string) (frontendProxyEndpoint, bool) {
	discovery := currentFrontendProxyDiscovery()
	if discovery == nil {
		return frontendProxyEndpoint{}, false
	}
	endpoint, ok := discovery.GetByNode(nodeID, capability)
	if !ok || !isHostPort(endpoint.Address) {
		return frontendProxyEndpoint{}, false
	}
	return endpoint, true
}

func resolveSoleFrontendProxyEndpoint(capability string) (frontendProxyEndpoint, bool) {
	return lookupSoleFrontendProxyEndpoint(capability)
}

func lookupSoleFrontendProxyEndpoint(capability string) (frontendProxyEndpoint, bool) {
	discovery := currentFrontendProxyDiscovery()
	if discovery == nil {
		return frontendProxyEndpoint{}, false
	}
	endpoint, ok := discovery.GetSoleEndpoint(capability)
	if !ok || !isHostPort(endpoint.Address) {
		return frontendProxyEndpoint{}, false
	}
	return endpoint, true
}

func resolveNextFrontendProxyEndpoint(capability string) (frontendProxyEndpoint, bool) {
	return lookupNextFrontendProxyEndpoint(capability)
}

func lookupNextFrontendProxyEndpoint(capability string) (frontendProxyEndpoint, bool) {
	discovery := currentFrontendProxyDiscovery()
	if discovery == nil {
		return frontendProxyEndpoint{}, false
	}
	endpoint, ok := discovery.GetNextEndpoint(capability)
	if !ok || !isHostPort(endpoint.Address) {
		return frontendProxyEndpoint{}, false
	}
	return endpoint, true
}

func resolveProxyAddressByInstance(req simpleRuntimeInvokeRequest) string {
	if req.instanceID == "" {
		return ""
	}
	var instance *types.InstanceSpecification
	if req.funcMeta.FuncID != "" {
		instance = instancemanager.GetGlobalInstanceScheduler().GetInstanceByID(req.funcMeta.FuncID, req.instanceID)
	}
	if instance == nil {
		instance = instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(req.instanceID)
	}
	return proxyAddressFromInstanceForCapability(instance, frontendProxyCapabilityInvoke)
}

func resolveProxyAddressByKill(req simpleRuntimeKillRequest) string {
	if req.instanceID == "" {
		return ""
	}
	var instance *types.InstanceSpecification
	if instance == nil {
		instance = instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(req.instanceID)
	}
	if address := proxyAddressFromInstanceForCapability(instance, frontendProxyCapabilityKill); address != "" {
		return address
	}
	if req.options.CreateOpt != nil {
		return resolveFrontendProxyAddressFromRouteWithCapability(req.options.CreateOpt[frontendProxyRouteKey],
			frontendProxyCapabilityKill)
	}
	return ""
}

func resolveProxyAddressByRawInvoke(req *core.InvokeRequest) string {
	if req == nil || req.GetInstanceID() == "" {
		return ""
	}
	var instance *types.InstanceSpecification
	if req.GetFunction() != "" {
		instance = instancemanager.GetGlobalInstanceScheduler().GetInstanceByID(req.GetFunction(), req.GetInstanceID())
	}
	if instance == nil {
		instance = instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(req.GetInstanceID())
	}
	return proxyAddressFromInstance(instance)
}

func isHostPort(address string) bool {
	host, port, err := net.SplitHostPort(address)
	return err == nil && strings.TrimSpace(host) != "" && strings.TrimSpace(port) != ""
}

func proxyAddressFromInstance(instance *types.InstanceSpecification) string {
	return proxyAddressFromInstanceForCapability(instance, frontendProxyCapabilityInvoke)
}

func proxyAddressFromInstanceForCapability(instance *types.InstanceSpecification, capability string) string {
	if instance == nil {
		return ""
	}
	if isHostPort(instance.FunctionProxyID) {
		if frontendProxyAddressIsSuspect(instance.FunctionProxyID) {
			return ""
		}
		return instance.FunctionProxyID
	}
	if endpoint, ok := resolveFrontendProxyEndpointByNode(instance.FunctionProxyID, capability); ok {
		return endpoint.Address
	}
	// If discovery already knows this owning proxy node but it is not eligible
	// for this frontend capability, do not bypass rollout with another endpoint.
	if frontendProxyNodeExistsInDiscovery(instance.FunctionProxyID) {
		return ""
	}
	// Some scheduler allocation responses do not carry RouteAddress, while the
	// instance watcher still provides the runtime address. Match that runtime's
	// node IP against the published proxy discovery endpoint instead of guessing
	// the legacy static gRPC port. Process-mode proxy ports are dynamic.
	if endpoint, ok := resolveFrontendProxyEndpointByRuntimeHost(instance.RuntimeAddress, capability); ok {
		return endpoint.Address
	}
	if frontendProxyRuntimeHostExists(instance.RuntimeAddress) {
		return ""
	}
	return ""
}

func frontendProxyRuntimeHostExists(runtimeAddress string) bool {
	host, _, err := net.SplitHostPort(runtimeAddress)
	if err != nil || host == "" {
		return false
	}
	discovery, ok := currentFrontendProxyDiscovery().(frontendProxyDiscoveryByHost)
	if !ok {
		return false
	}
	_, found := discovery.GetByHost(host, "")
	return found
}

func resolveFrontendProxyEndpointByRuntimeHost(runtimeAddress string, capability string) (frontendProxyEndpoint, bool) {
	host, _, err := net.SplitHostPort(runtimeAddress)
	if err != nil || host == "" {
		return frontendProxyEndpoint{}, false
	}
	discovery, ok := currentFrontendProxyDiscovery().(frontendProxyDiscoveryByHost)
	if !ok {
		return frontendProxyEndpoint{}, false
	}
	if endpoint, found := discovery.GetByHost(host, capability); found {
		return endpoint, true
	}
	return frontendProxyEndpoint{}, false
}

func frontendProxyNodeExistsInDiscovery(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	discovery := currentFrontendProxyDiscovery()
	if discovery == nil {
		return false
	}
	_, ok := discovery.GetByNode(nodeID, "")
	return ok
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
	MarkFrontendProxyEndpointSuspect(address)
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
	return simpleRuntimeInvokeContextWithParent(context.Background(), options)
}

func simpleRuntimeInvokeContextWithParent(
	parent context.Context,
	options api.InvokeOptions,
) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := defaultFrontendProxyTimeout
	if options.Timeout > 0 {
		timeout = time.Duration(options.Timeout) * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func rawSimpleRuntimeContext(parent context.Context, _ api.RawRequestOption) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithCancel(parent)
}

func newFrontendProxyLifecycleCorrelationID(operation string) string {
	return fmt.Sprintf("frontend-proxy-%s-%s", operation, uuid.NewString())
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

func (e *frontendProxyBusinessErr) Error() string {
	if e == nil {
		return "frontend proxy failed with nil business error"
	}
	return fmt.Sprintf("frontend proxy %s failed, code: %v, message: %s",
		e.operation, e.code, e.message)
}

func (e *frontendProxyStatusErr) Error() string {
	if e == nil {
		return "frontend proxy failed with nil status error"
	}
	message := fmt.Sprintf("frontend proxy %s failed, code: %v, message: %s",
		e.operation, e.code, e.message)
	if e.retryable {
		message += ", retryable: true"
	}
	if e.retryReason != "" {
		message += fmt.Sprintf(", retryReason: %s", e.retryReason)
	}
	return message
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

// IsFileTransferNotFoundError reports whether err was returned by a file
// transfer RPC and indicates the instance/path does not exist on the owning
// proxy. It inspects the gRPC status code (NotFound) and the frontend proxy
// business/status error envelopes (code ERR_INSTANCE_NOT_FOUND). It is the
// exported seam that lets the HTTP handler map the failure to 404 without
// depending on the unexported error types.
func IsFileTransferNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.NotFound {
		return true
	}
	var businessErr *frontendProxyBusinessErr
	if errors.As(err, &businessErr) {
		return businessErr.code == common.ErrorCode_ERR_INSTANCE_NOT_FOUND
	}
	var statusErr *frontendProxyStatusErr
	if errors.As(err, &statusErr) {
		return statusErr.code == common.ErrorCode_ERR_INSTANCE_NOT_FOUND
	}
	return false
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
	if simpleRuntimeInvokeArgsNeedFaaSPrefix(funcMeta.Api) {
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

func convertSimpleRuntimeInvokeArgsForRPC(funcMeta api.FunctionMeta, args []api.Arg) ([]*common.Arg, func()) {
	converted := convertSimpleRuntimeArgs(args)
	release := func() {}
	if funcMeta.Api == api.PosixApi {
		return converted, release
	}
	if simpleRuntimeInvokeArgsNeedFaaSPrefix(funcMeta.Api) {
		converted, release = prefixSimpleRuntimeFaaSInvokeArgsForRPC(converted)
	}
	metadata := buildSimpleRuntimeInvokeMetadata(funcMeta)
	withMetadata := make([]*common.Arg, 0, len(converted)+1)
	withMetadata = append(withMetadata, &common.Arg{Type: common.Arg_VALUE, Value: metadata})
	withMetadata = append(withMetadata, converted...)
	return withMetadata, release
}

func simpleRuntimeInvokeArgsNeedFaaSPrefix(apiType api.ApiType) bool {
	return apiType == api.FaaSApi || apiType == api.ServeApi
}

func normalizeSimpleRuntimeInvokePayload(funcMeta api.FunctionMeta, payload []byte) []byte {
	if !simpleRuntimeInvokeArgsNeedFaaSPrefix(funcMeta.Api) {
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
	customTag := make(map[string]string, len(options.CustomExtensions)+len(options.CreateOpt))
	for key, value := range options.CustomExtensions {
		customTag[key] = value
	}
	for key, value := range options.CreateOpt {
		customTag[key] = value
	}
	return &core.InvokeOptions{CustomTag: customTag}
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

func firstArgTenantID(args []api.Arg) string {
	for _, arg := range args {
		if arg.TenantID != "" {
			return arg.TenantID
		}
	}
	return ""
}
