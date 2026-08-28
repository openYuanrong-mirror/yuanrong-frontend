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
	"fmt"
	"sync"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/proxyrouting"
)

// DirectProxyClient is the explicit frontend-to-function_proxy boundary used
// by FaaS, Agent, and Frontend Sandbox APIs. It intentionally does not embed
// Client or invokerLibruntime and therefore cannot fall back to libruntime.
type DirectProxyClient interface {
	Invoke(req DirectInvokeRequest) ([]byte, error)
	// CreateInstance creates a FaaS instance from the typed Frontend request.
	// Actor direct creation has a different wire contract and must use a dedicated
	// request constructor and adapter instead of this method.
	CreateInstance(req DirectCreateRequest) (string, error)
	CreateRaw(req DirectRawRequest) ([]byte, error)
	InvokeRaw(req DirectRawRequest) ([]byte, error)
	KillInstance(req DirectKillRequest) error
	KillInstanceWithResponse(req DirectKillRequest) (*core.KillResponse, error)
}

// DirectInvokeRequest is the inline-data DTO exposed by DirectProxyClient. It
// contains no libruntime Arg or InvokeOptions types.
type DirectInvokeRequest struct {
	Function, InstanceID, TraceID, TraceParent, TenantID string
	InvokeTag                                            map[string]string
	InvokeTimeout                                        int64
	RetryTimes                                           int
	ForceInvoke, IsInterrupted, BypassDataSystem         bool
	AcceptHeader, SessionCtxID                           string
	InstanceSession                                      *types.InstanceSessionConfig
	Args                                                 [][]byte
	ResponseWriter                                       types.ResponseWriter
}

// DirectCreateRequest carries a validated direct FunctionProxy create request.
type DirectCreateRequest struct {
	functionMeta api.FunctionMeta
	args         []api.Arg
	options      api.InvokeOptions
}

// DirectRawRequest carries a raw FunctionSystem request and its trace context.
type DirectRawRequest struct {
	Context     context.Context
	Payload     []byte
	TraceParent string
	// CreateTimeoutSeconds is used only by CreateRaw. InvokeRaw continues to use
	// the timeout carried by its core InvokeRequest payload.
	CreateTimeoutSeconds int
}

// DirectKillRequest carries a direct FunctionProxy kill request.
type DirectKillRequest struct {
	Context              context.Context
	InstanceID, TenantID string
	Signal               int
	Payload              []byte
	TraceID, TraceParent string
	TimeoutSeconds       int
	RequestID            string
}

// NewDirectInvokeRequest validates and converts an inline invocation request.
func NewDirectInvokeRequest(req InvokeRequest) (DirectInvokeRequest, error) {
	inlineArgs := make([][]byte, 0, len(req.Args))
	for i, arg := range req.Args {
		if arg == nil || arg.Type != api.Value || len(arg.NestedObjectIDs) != 0 {
			return DirectInvokeRequest{}, directProxyArgError(i)
		}
		inlineArgs = append(inlineArgs, append([]byte(nil), arg.Data...))
	}
	return DirectInvokeRequest{
		Function: req.Function, InstanceID: req.InstanceID, TraceID: req.TraceID, TraceParent: req.TraceParent,
		TenantID: req.TenantID, InvokeTag: req.InvokeTag, InvokeTimeout: req.InvokeTimeout,
		RetryTimes: req.RetryTimes, ForceInvoke: req.ForceInvoke, IsInterrupted: req.IsInterrupted,
		BypassDataSystem: req.BypassDataSystem,
		AcceptHeader:     req.AcceptHeader, SessionCtxID: req.SessionCtxID, InstanceSession: req.InstanceSession,
		Args: inlineArgs, ResponseWriter: req.ResponseWriter,
	}, nil
}

// NewDirectCreateRequest builds a typed direct-create request for a FaaS instance.
// Actor direct creation must use a separate constructor because its create wire
// contract must not inherit the FaaS argument encoding performed by this path.
func NewDirectCreateRequest(
	funcMeta api.FunctionMeta, args []api.Arg, options api.InvokeOptions,
) (DirectCreateRequest, error) {
	if err := validateDirectProxyArgs(args); err != nil {
		return DirectCreateRequest{}, err
	}
	return DirectCreateRequest{functionMeta: funcMeta, args: args, options: options}, nil
}

// AdaptedCreateValues is used by entry adapters and test doubles while typed
// handler construction is being moved away from libruntime-owned option types.
func (r DirectCreateRequest) AdaptedCreateValues() (api.FunctionMeta, []api.Arg, api.InvokeOptions) {
	return r.functionMeta, r.args, r.options
}

// NewDirectRawRequest builds a raw request without changing its payload.
func NewDirectRawRequest(ctx context.Context, payload []byte, option api.RawRequestOption) DirectRawRequest {
	return DirectRawRequest{Context: ctx, Payload: payload, TraceParent: option.TraceParent}
}

// NewDirectRawCreateRequest builds a raw create request with the caller's
// logical create timeout kept alongside the serialized core request.
func NewDirectRawCreateRequest(
	ctx context.Context, payload []byte, option api.RawRequestOption, timeoutSeconds int,
) DirectRawRequest {
	return DirectRawRequest{
		Context: ctx, Payload: payload, TraceParent: option.TraceParent, CreateTimeoutSeconds: timeoutSeconds,
	}
}

// AdaptedRawOption restores the libruntime option used by Raw/SDK adapters.
func (r DirectRawRequest) AdaptedRawOption() api.RawRequestOption {
	return api.RawRequestOption{TraceParent: r.TraceParent}
}

// NewDirectKillRequest builds a direct kill request from the entrypoint values.
func NewDirectKillRequest(ctx context.Context, instanceID string, signal int, payload []byte,
	tenantID string, option api.InvokeOptions,
) DirectKillRequest {
	return DirectKillRequest{Context: ctx, InstanceID: instanceID, Signal: signal, Payload: payload,
		TenantID: tenantID, TraceID: option.TraceID, TraceParent: option.CustomExtensions[traceParentExtensionKey],
		TimeoutSeconds: option.Timeout}
}

// AdaptedInvokeOptions restores invoke options used by compatibility adapters.
func (r DirectKillRequest) AdaptedInvokeOptions() api.InvokeOptions {
	options := api.InvokeOptions{TraceID: r.TraceID, Timeout: r.TimeoutSeconds}
	if r.TraceParent != "" {
		options.CustomExtensions = map[string]string{traceParentExtensionKey: r.TraceParent}
	}
	return options
}

type directProxyClient struct {
	invokeClient    frontendProxyInvokeClient
	lifecycleClient frontendProxyLifecycleClient
}

var directProxyClientState = struct {
	sync.RWMutex
	client DirectProxyClient
}{}

// GetDirectProxyClient returns the process-wide direct client. Construction is
// lazy so frontend config and identity are available before the gRPC adapters
// and their shared connection pools are created.
func GetDirectProxyClient() DirectProxyClient {
	directProxyClientState.RLock()
	client := directProxyClientState.client
	directProxyClientState.RUnlock()
	if client != nil {
		return client
	}

	directProxyClientState.Lock()
	defer directProxyClientState.Unlock()
	if directProxyClientState.client == nil {
		directProxyClientState.client = &directProxyClient{
			invokeClient:    newRoutingFrontendProxyInvokeClient(),
			lifecycleClient: newRoutingFrontendProxyLifecycleClient(),
		}
	}
	return directProxyClientState.client
}

// InitializeDirectProxyRouting explicitly selects the watcher-backed routing
// cache during frontend startup. Client construction remains lazy.
func InitializeDirectProxyRouting() {
	proxyrouting.UseCache()
}

// SetDirectProxyClientForTest replaces the process-wide direct client and
// returns a restore function. Production initialization does not use it.
func SetDirectProxyClientForTest(client DirectProxyClient) func() {
	directProxyClientState.Lock()
	previous := directProxyClientState.client
	directProxyClientState.client = client
	directProxyClientState.Unlock()
	return func() {
		directProxyClientState.Lock()
		directProxyClientState.client = previous
		directProxyClientState.Unlock()
	}
}

func (c *directProxyClient) Invoke(req DirectInvokeRequest) ([]byte, error) {
	if c == nil || c.invokeClient == nil {
		return nil, fmt.Errorf("direct proxy invoke client is not initialized")
	}
	if req.InstanceID == "" {
		return nil, fmt.Errorf("direct proxy invoke requires instance id")
	}
	funcArgs := make([]api.Arg, 0, len(req.Args))
	for _, arg := range req.Args {
		funcArgs = append(funcArgs, api.Arg{Type: api.Value, Data: arg, TenantID: req.TenantID})
	}
	funcMeta := api.FunctionMeta{FuncID: req.Function, Api: api.FaaSApi}
	invokeOpts := api.InvokeOptions{TraceID: req.TraceID, Timeout: int(req.InvokeTimeout),
		CustomExtensions: make(map[string]string, len(req.InvokeTag)+1)}
	for key, value := range req.InvokeTag {
		invokeOpts.CustomExtensions[key] = value
	}
	if req.TraceParent != "" {
		invokeOpts.CustomExtensions[traceParentExtensionKey] = req.TraceParent
	}
	if req.AcceptHeader == "text/event-stream" {
		// Direct invocation bypasses libruntime's scheduler label conversion.
		// CallRequest.createOptions is the runtime invocation header, so carry
		// Accept there using the casing expected by the FaaS executor.
		invokeOpts.CustomExtensions["Accept"] = req.AcceptHeader
	}
	if req.InstanceSession != nil {
		invokeOpts.InstanceSession = &api.InstanceSessionConfig{SessionID: req.InstanceSession.SessionID,
			SessionTTL: req.InstanceSession.SessionTTL, Concurrency: req.InstanceSession.Concurrency}
	}
	invokeOpts.IsInterrupted, invokeOpts.SessionCtxID = req.IsInterrupted, req.SessionCtxID
	// Owner routing is resolved exclusively from instance state. Do not forward
	// the scheduler's historical YR_ROUTE hint into the direct invoke.
	invokeOpts.CreateOpt = nil
	invokeOpts.RetryTimes = req.RetryTimes
	invokeOpts.ForceInvoke = req.ForceInvoke
	invokeOpts.BypassDataSystem = req.BypassDataSystem
	routeCtx, cancelRoute := simpleRuntimeInvokeContextWithParent(
		context.Background(), invokeOpts, frontendProxyInvokeResultBuffer,
	)
	defer cancelRoute()
	proxyReq := simpleRuntimeInvokeRequest{
		ctx:        routeCtx,
		funcMeta:   funcMeta,
		instanceID: req.InstanceID,
		args:       funcArgs,
		options:    invokeOpts,
	}
	if req.AcceptHeader == "text/event-stream" {
		if req.ResponseWriter == nil {
			return nil, fmt.Errorf("direct proxy streaming invoke requires response writer")
		}
		return c.invokeClient.InvokeByInstanceIDStream(proxyReq, req.ResponseWriter)
	}
	return c.invokeClient.InvokeByInstanceID(proxyReq)
}

// CreateInstance sends the typed FaaS create contract to FunctionProxy. Actor
// direct creation is intentionally out of scope for this adapter.
func (c *directProxyClient) CreateInstance(req DirectCreateRequest) (string, error) {
	if c == nil || c.lifecycleClient == nil {
		return "", fmt.Errorf("direct proxy lifecycle client is not initialized")
	}
	tenantID := ""
	if req.options.CreateOpt != nil {
		tenantID = req.options.CreateOpt["tenantId"]
	}
	return c.lifecycleClient.CreateInstance(simpleRuntimeCreateRequest{
		funcMeta: req.functionMeta,
		tenantID: tenantID,
		args:     req.args,
		options:  req.options,
	})
}

func validateDirectProxyArgs(args []api.Arg) error {
	for i, arg := range args {
		if arg.Type != api.Value || len(arg.NestedObjectIDs) != 0 {
			return directProxyArgError(i)
		}
	}
	return nil
}

func directProxyArgError(index int) error {
	return fmt.Errorf(
		"argument %d uses ObjectRef or nested refs, which the direct proxy inline-data API does not support", index)
}

func (c *directProxyClient) CreateRaw(req DirectRawRequest) ([]byte, error) {
	if c == nil || c.lifecycleClient == nil {
		return nil, fmt.Errorf("direct proxy lifecycle client is not initialized")
	}
	return c.lifecycleClient.CreateInstanceRaw(simpleRuntimeRawCreateRequest{
		ctx: req.Context, create: req.Payload, timeoutSeconds: req.CreateTimeoutSeconds,
		options: api.RawRequestOption{TraceParent: req.TraceParent},
	})
}

func (c *directProxyClient) InvokeRaw(req DirectRawRequest) ([]byte, error) {
	if c == nil || c.invokeClient == nil {
		return nil, fmt.Errorf("direct proxy invoke client is not initialized")
	}
	return c.invokeClient.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{
		ctx: req.Context, invoke: req.Payload, options: api.RawRequestOption{TraceParent: req.TraceParent},
	})
}

func (c *directProxyClient) KillInstance(req DirectKillRequest) error {
	_, err := c.KillInstanceWithResponse(req)
	return err
}

// KillInstanceWithResponse dispatches a lifecycle signal and returns its
// authoritative payload. Pause and Resume use this to synchronously return the
// committed snapshot or route while ordinary callers may ignore the response.
func (c *directProxyClient) KillInstanceWithResponse(req DirectKillRequest) (*core.KillResponse, error) {
	if c == nil || c.lifecycleClient == nil {
		return nil, fmt.Errorf("direct proxy lifecycle client is not initialized")
	}
	options := req.AdaptedInvokeOptions()
	return c.lifecycleClient.KillInstanceWithResponse(simpleRuntimeKillRequest{
		ctx: req.Context, instanceID: req.InstanceID, tenantID: req.TenantID,
		signal: req.Signal, payload: req.Payload, requestID: req.RequestID, options: options,
	})
}
