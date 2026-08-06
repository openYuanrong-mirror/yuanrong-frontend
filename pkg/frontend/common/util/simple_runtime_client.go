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
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/logger/log"
)

const (
	traceParentPartCount = 4
	traceIDHexLength     = 32
)

func traceIDFromTraceParent(traceParent string) string {
	parts := strings.Split(traceParent, "-")
	if len(parts) != traceParentPartCount || len(parts[1]) != traceIDHexLength {
		return ""
	}
	if _, err := hex.DecodeString(parts[1]); err != nil || parts[1] == strings.Repeat("0", traceIDHexLength) {
		return ""
	}
	return parts[1]
}

var _ invokerLibruntime = (*clientSimpleRuntime)(nil)

type simpleRuntimeInvokeRequest struct {
	funcMeta   api.FunctionMeta
	instanceID string
	args       []api.Arg
	options    api.InvokeOptions
}

type simpleRuntimeRawInvokeRequest struct {
	ctx     context.Context
	invoke  []byte
	options api.RawRequestOption
}

type simpleRuntimeRawCreateRequest struct {
	ctx     context.Context
	create  []byte
	options api.RawRequestOption
}

type simpleRuntimeCreateRequest struct {
	funcMeta api.FunctionMeta
	tenantID string
	args     []api.Arg
	options  api.InvokeOptions
}

type simpleRuntimeKillRequest struct {
	ctx        context.Context
	instanceID string
	tenantID   string
	signal     int
	payload    []byte
	requestID  string
	options    api.InvokeOptions
}

type frontendProxyInvokeClient interface {
	InvokeByInstanceID(simpleRuntimeInvokeRequest) ([]byte, error)
	InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest) ([]byte, error)
	UploadFile(ctx context.Context, instanceID string, path string,
		reader io.Reader, tenantID string) (*frontend_proxy.FileTransferResponse, error)
	DownloadFile(ctx context.Context, instanceID string, path string,
		offset int64, tenantID string) (frontend_proxy.FrontendProxyService_DownloadFileClient, error)
}

type frontendProxyLifecycleClient interface {
	CreateInstance(simpleRuntimeCreateRequest) (string, error)
	CreateInstanceRaw(simpleRuntimeRawCreateRequest) ([]byte, error)
	KillInstance(simpleRuntimeKillRequest) error
}

// clientSimpleRuntime is the Go-native implementation point for replacing the
// frontend-side CGO/libruntime client. It deliberately implements the existing
// invokerLibruntime seam instead of introducing a second frontend runtime API,
// so defaultClient and the HTTP/API layers keep using the same call boundary.
//
// M1 only wires the shape and the local small-result store that mirrors the old
// bypass-datasystem/memStore contract. Calls that have not been moved to the
// new proxy protocol fail explicitly instead of silently pretending success.
//
// Important boundary: the control fallback below is a staged compatibility
// guard, not the final target. When the frontend-proxy backend is explicitly
// enabled, the product target is for the supported FaaS/sandbox non-actor
// methods (create/acquire/invoke/kill/get-result) to move onto the Go-native
// frontend→proxy path. Actor/ref/datasystem/legacy-stream semantics are
// explicitly legacy-owned and may still fall back to the old libruntime path.
// Raw create may only use the new path when proxy create returns the final
// ready CallResult that can be adapted to the old NotifyRequest wire contract.
type clientSimpleRuntime struct {
	mu              sync.Mutex
	seq             atomic.Uint64
	results         map[string][]byte
	traceID         string
	tenantID        string
	proxyClient     frontendProxyInvokeClient
	lifecycleClient frontendProxyLifecycleClient
	control         invokerLibruntime
	legacyFallback  bool
}

func newClientSimpleRuntime() *clientSimpleRuntime {
	return newClientSimpleRuntimeWithControl(nil)
}

func newClientSimpleRuntimeWithControl(control invokerLibruntime) *clientSimpleRuntime {
	return newClientSimpleRuntimeWithProxyClientControlAndFallback(newRoutingFrontendProxyInvokeClient(), control, false)
}

func newClientSimpleRuntimeWithLegacyFallback(control invokerLibruntime) *clientSimpleRuntime {
	return newClientSimpleRuntimeWithProxyClientControlAndFallback(newRoutingFrontendProxyInvokeClient(), control, true)
}

func newClientSimpleRuntimeWithProxyClient(proxyClient frontendProxyInvokeClient) *clientSimpleRuntime {
	return newClientSimpleRuntimeWithProxyClientControlAndFallback(proxyClient, nil, false)
}

func newClientSimpleRuntimeWithProxyClientAndControl(
	proxyClient frontendProxyInvokeClient,
	control invokerLibruntime,
) *clientSimpleRuntime {
	return newClientSimpleRuntimeWithProxyClientControlAndFallback(proxyClient, control, false)
}

func newClientSimpleRuntimeWithProxyClientControlAndFallback(
	proxyClient frontendProxyInvokeClient,
	control invokerLibruntime,
	legacyFallback bool,
) *clientSimpleRuntime {
	return &clientSimpleRuntime{
		results:         make(map[string][]byte),
		proxyClient:     proxyClient,
		lifecycleClient: newRoutingFrontendProxyLifecycleClient(),
		control:         control,
		legacyFallback:  legacyFallback,
	}
}

func (c *clientSimpleRuntime) unsupported(method string) error {
	return fmt.Errorf("clientSimpleRuntime.%s is not implemented in the Go-native frontend runtime path", method)
}

func supportsFrontendProxyLifecycle(funcMeta api.FunctionMeta) bool {
	return funcMeta.Api == api.FaaSApi || funcMeta.Api == api.ServeApi
}

func (c *clientSimpleRuntime) shouldUseControlForFunction(funcMeta api.FunctionMeta) bool {
	if c.control == nil {
		return false
	}
	if c.legacyFallback {
		return true
	}
	// Actor/default-zero and Posix still carry old runtime identity, ref,
	// callback or stream semantics, so they are explicit legacy lanes rather
	// than silent fallback. FaaS/Serve are frontend-proxy lifecycle candidates
	// and must either use the new client or fail fast when it is not wired.
	return funcMeta.Api == api.ActorApi || funcMeta.Api == api.PosixApi
}

func requiresLegacyControlForFunction(funcMeta api.FunctionMeta) bool {
	return funcMeta.Api == api.ActorApi || funcMeta.Api == api.PosixApi
}

func (c *clientSimpleRuntime) putLocalResult(payload []byte) string {
	id := fmt.Sprintf("frontend-simple-runtime-%d", c.seq.Add(1))
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStoresLocked()
	// The simple runtime takes ownership of payload from the proxy response.
	// Do not copy here; GetAsync consumes the local object exactly once.
	c.results[id] = payload
	return id
}

func (c *clientSimpleRuntime) CreateInstance(funcMeta api.FunctionMeta, args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	if supportsFrontendProxyLifecycle(funcMeta) && !c.legacyFallback && c.lifecycleClient != nil {
		return c.lifecycleClient.CreateInstance(simpleRuntimeCreateRequest{
			funcMeta: funcMeta,
			tenantID: c.currentTenantID(),
			args:     args,
			options:  invokeOpt,
		})
	}
	if c.shouldUseControlForFunction(funcMeta) {
		return c.control.CreateInstance(funcMeta, args, invokeOpt)
	}
	return "", c.unsupported("CreateInstance")
}

func (c *clientSimpleRuntime) InvokeByInstanceId(funcMeta api.FunctionMeta, instanceID string, args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	if requiresLegacyControlForFunction(funcMeta) {
		if c.control == nil {
			return "", fmt.Errorf("clientSimpleRuntime.InvokeByInstanceId requires legacy control for API type %d", funcMeta.Api)
		}
		return c.control.InvokeByInstanceId(funcMeta, instanceID, args, invokeOpt)
	}
	if c.proxyClient == nil {
		return "", c.unsupported("InvokeByInstanceId")
	}
	payload, err := c.proxyClient.InvokeByInstanceID(simpleRuntimeInvokeRequest{
		funcMeta:   funcMeta,
		instanceID: instanceID,
		args:       args,
		options:    invokeOpt,
	})
	if err != nil {
		return "", err
	}
	return c.putLocalResult(payload), nil
}

func (c *clientSimpleRuntime) InvokeByFunctionName(funcMeta api.FunctionMeta, args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	if c.shouldUseControlForFunction(funcMeta) {
		return c.control.InvokeByFunctionName(funcMeta, args, invokeOpt)
	}
	return "", c.unsupported("InvokeByFunctionName")
}

func (c *clientSimpleRuntime) AcquireInstance(state string, funcMeta api.FunctionMeta,
	acquireOpt api.InvokeOptions,
) (api.InstanceAllocation, error) {
	// Ordinary FaaS HTTP invoke does not use this old libruntime acquire seam:
	// function_invoke_for_kernel.go acquires via leaseadaptor/function scheduler
	// and passes FunctionProxyID as RouteAddress into the simple-runtime invoke.
	// Keep this method fail-fast for FaaS unless the operator explicitly enables
	// legacy fallback, so future direct/FaaS callers cannot silently bypass the
	// Go-native target through old libruntime.
	if c.legacyFallback && c.control != nil {
		return c.control.AcquireInstance(state, funcMeta, acquireOpt)
	}
	return api.InstanceAllocation{}, c.unsupported("AcquireInstance")
}

func (c *clientSimpleRuntime) ReleaseInstance(
	allocation api.InstanceAllocation,
	stateID string,
	abnormal bool,
	option api.InvokeOptions,
) {
	// See AcquireInstance: the current FaaS HTTP path releases through
	// leaseadaptor/function scheduler. This old seam is only a legacy fallback
	// hook, not the final release implementation for frontend-proxy mode.
	if c.legacyFallback && c.control != nil {
		c.control.ReleaseInstance(allocation, stateID, abnormal, option)
		return
	}
	log.GetLogger().Warnf(
		"clientSimpleRuntime.ReleaseInstance is not implemented in the Go-native frontend runtime path, "+
			"skip release for instanceID(%s), stateID(%s), abnormal(%t)",
		allocation.InstanceID, stateID, abnormal,
	)
}

func (c *clientSimpleRuntime) Kill(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
	if c.control != nil {
		// This invokerLibruntime method has no API type, and current callers are
		// legacy actor/system delete paths (for example sandbox/app delete). Keep
		// those explicit legacy lanes working while typed FaaS/sandbox-non-actor
		// kill is designed on the frontend-proxy protocol. Do not route a future
		// non-actor sandbox/FaaS kill through this untyped fallback as "new path".
		return c.control.Kill(instanceID, signal, payload, invokeOpt)
	}
	return c.unsupported("Kill")
}

func (c *clientSimpleRuntime) KillInstance(
	funcMeta api.FunctionMeta,
	instanceID string,
	signal int,
	payload []byte,
	invokeOpt api.InvokeOptions,
) error {
	if supportsFrontendProxyLifecycle(funcMeta) && !c.legacyFallback && c.lifecycleClient != nil {
		return c.lifecycleClient.KillInstance(simpleRuntimeKillRequest{
			instanceID: instanceID,
			tenantID:   c.currentTenantID(),
			signal:     signal,
			payload:    payload,
			options:    invokeOpt,
		})
	}
	if c.shouldUseControlForFunction(funcMeta) {
		return c.control.Kill(instanceID, signal, payload, invokeOpt)
	}
	return c.unsupported("KillInstance")
}

func (c *clientSimpleRuntime) CreateInstanceRaw(createReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	return c.CreateInstanceRawContext(context.Background(), createReqRaw, option)
}

func (c *clientSimpleRuntime) CreateInstanceRawContext(ctx context.Context, createReqRaw []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	// Raw create is a two-phase contract: the HTTP/POSIX caller expects the final
	// runtime NotifyRequest-compatible result, not the immediate scheduler
	// CreateResponse. The frontend-proxy path is eligible only after proxy create
	// carries the ready CallResult; explicit legacy fallback remains the rollback
	// lane for actor/old stream semantics.
	if !c.legacyFallback && c.lifecycleClient != nil {
		return c.lifecycleClient.CreateInstanceRaw(simpleRuntimeRawCreateRequest{
			ctx: ctx, create: createReqRaw, options: option,
		})
	}
	if c.legacyFallback && c.control != nil {
		return c.control.CreateInstanceRaw(createReqRaw, option)
	}
	return nil, fmt.Errorf(
		"raw create requires frontend-proxy lifecycle client or explicit legacy libruntime control fallback",
	)
}

func (c *clientSimpleRuntime) InvokeByInstanceIdRaw(invokeReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	return c.InvokeByInstanceIdRawContext(context.Background(), invokeReqRaw, option)
}

func (c *clientSimpleRuntime) InvokeByInstanceIdRawContext(ctx context.Context, invokeReqRaw []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if c.proxyClient == nil {
		return nil, c.unsupported("InvokeByInstanceIdRaw")
	}
	return c.proxyClient.InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest{
		ctx: ctx, invoke: invokeReqRaw, options: option,
	})
}

func (c *clientSimpleRuntime) KillRaw(killReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	return c.KillRawContext(context.Background(), killReqRaw, option)
}

func (c *clientSimpleRuntime) KillRawContext(ctx context.Context, killReqRaw []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	if !c.legacyFallback && c.lifecycleClient != nil {
		killReq := &core.KillRequest{}
		if err := proto.Unmarshal(killReqRaw, killReq); err != nil {
			return nil, fmt.Errorf("failed to unmarshal frontend proxy kill request: %w", err)
		}
		if err := c.lifecycleClient.KillInstance(simpleRuntimeKillRequest{
			ctx:        ctx,
			instanceID: killReq.GetInstanceID(),
			tenantID:   c.currentTenantID(),
			signal:     int(killReq.GetSignal()),
			payload:    killReq.GetPayload(),
			requestID:  newFrontendProxyLifecycleCorrelationID("kill"),
			options: api.InvokeOptions{
				TraceID: traceIDFromTraceParent(option.TraceParent),
				CustomExtensions: map[string]string{
					traceParentExtensionKey: option.TraceParent,
				},
			},
		}); err != nil {
			return nil, err
		}
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE})
	}
	if c.legacyFallback && c.control != nil {
		return c.control.KillRaw(killReqRaw, option)
	}
	return nil, c.unsupported("KillRaw")
}

func (c *clientSimpleRuntime) SaveState([]byte) (string, error) {
	return "", c.unsupported("SaveState")
}

func (c *clientSimpleRuntime) LoadState(string) ([]byte, error) {
	return nil, c.unsupported("LoadState")
}

func (c *clientSimpleRuntime) Exit(code int, message string) {
	log.GetLogger().Warnf(
		"clientSimpleRuntime.Exit is not implemented in the Go-native frontend runtime path, code(%d), message(%s)",
		code, message,
	)
}

func (c *clientSimpleRuntime) KVSet(string, []byte, api.SetParam) error {
	return c.unsupported("KVSet")
}

func (c *clientSimpleRuntime) KVSetWithoutKey([]byte, api.SetParam) (string, error) {
	return "", c.unsupported("KVSetWithoutKey")
}

func (c *clientSimpleRuntime) KVGet(string, uint) ([]byte, error) {
	return nil, c.unsupported("KVGet")
}

func (c *clientSimpleRuntime) KVGetMulti([]string, uint) ([][]byte, error) {
	return nil, c.unsupported("KVGetMulti")
}

func (c *clientSimpleRuntime) KVDel(string) error {
	return c.unsupported("KVDel")
}

func (c *clientSimpleRuntime) KVDelMulti([]string) ([]string, error) {
	return nil, c.unsupported("KVDelMulti")
}

func (c *clientSimpleRuntime) SetTraceID(traceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traceID = traceID
}

func (c *clientSimpleRuntime) Put(objectID string, value []byte, _ api.PutParam, _ ...string) error {
	if objectID == "" {
		return c.unsupported("Put")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStoresLocked()
	c.results[objectID] = value
	return nil
}

func (c *clientSimpleRuntime) Get(objectIDs []string, timeoutMs int) ([][]byte, error) {
	allLocal, mixed := c.classifyObjectIDs(objectIDs)
	if mixed {
		return nil, fmt.Errorf("clientSimpleRuntime.Get does not support mixed local and legacy object IDs")
	}
	if !allLocal {
		if c.control == nil {
			return nil, fmt.Errorf("clientSimpleRuntime.Get requires legacy control for non-local objects")
		}
		return c.control.Get(objectIDs, timeoutMs)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make([][]byte, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		value, ok := c.results[objectID]
		if !ok {
			return nil, fmt.Errorf("clientSimpleRuntime.Get local object %q not found", objectID)
		}
		values = append(values, value)
	}
	return values, nil
}

func (c *clientSimpleRuntime) GIncreaseRef(objectIDs []string, remoteClientID ...string) ([]string, error) {
	allLocal, mixed := c.classifyObjectIDs(objectIDs)
	if mixed {
		return objectIDs, fmt.Errorf("clientSimpleRuntime.GIncreaseRef does not support mixed local and legacy object IDs")
	}
	if !allLocal {
		if c.control == nil {
			return objectIDs, fmt.Errorf("clientSimpleRuntime.GIncreaseRef requires legacy control for non-local objects")
		}
		return c.control.GIncreaseRef(objectIDs, remoteClientID...)
	}
	failed := c.missingLocalObjects(objectIDs)
	if len(failed) > 0 {
		return failed, fmt.Errorf(
			"clientSimpleRuntime.GIncreaseRef only supports local simple-runtime objects, failed: %v", failed,
		)
	}
	return nil, nil
}

func (c *clientSimpleRuntime) GDecreaseRef(objectIDs []string, remoteClientID ...string) ([]string, error) {
	allLocal, mixed := c.classifyObjectIDs(objectIDs)
	if mixed {
		return objectIDs, fmt.Errorf("clientSimpleRuntime.GDecreaseRef does not support mixed local and legacy object IDs")
	}
	if !allLocal {
		if c.control == nil {
			return objectIDs, fmt.Errorf("clientSimpleRuntime.GDecreaseRef requires legacy control for non-local objects")
		}
		return c.control.GDecreaseRef(objectIDs, remoteClientID...)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, objectID := range objectIDs {
		delete(c.results, objectID)
	}
	// Ref release is intentionally idempotent. GetAsync already transfers the
	// payload to its caller and removes it from results, so retaining tombstones
	// merely to distinguish a second release would grow with every invocation.
	return nil, nil
}

func (c *clientSimpleRuntime) GetAsync(objectID string, cb api.GetAsyncCallback) {
	if !c.isLocalObjectID(objectID) {
		if c.control == nil {
			cb(nil, fmt.Errorf("clientSimpleRuntime.GetAsync local object %q not found", objectID))
			return
		}
		c.control.GetAsync(objectID, cb)
		return
	}
	value, err := c.consumeLocalResult(objectID)
	if err != nil {
		cb(nil, err)
		return
	}
	cb(value, nil)
}

func (c *clientSimpleRuntime) GetEvent(objectID string, cb api.GetEventCallback) {
	if c.isLocalObjectID(objectID) {
		cb(nil, c.unsupported("GetEvent"))
		return
	}
	if c.control == nil {
		cb(nil, c.unsupported("GetEvent"))
		return
	}
	c.control.GetEvent(objectID, cb)
}

func (c *clientSimpleRuntime) DeleteGetEventCallback(objectID string) {
	if !c.isLocalObjectID(objectID) && c.control != nil {
		c.control.DeleteGetEventCallback(objectID)
	}
}

func (c *clientSimpleRuntime) GetFormatLogger() api.FormatLogger {
	return nil
}

func (c *clientSimpleRuntime) GetCredential() api.Credential {
	return api.Credential{}
}

func (c *clientSimpleRuntime) SetTenantID(tenantID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tenantID = tenantID
	return nil
}

func (c *clientSimpleRuntime) currentTenantID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tenantID
}

func (c *clientSimpleRuntime) IsHealth() bool {
	if c.control != nil {
		return c.control.IsHealth()
	}
	return false
}

func (c *clientSimpleRuntime) IsDsHealth() bool {
	if c.control != nil {
		return c.control.IsDsHealth()
	}
	return false
}

func (c *clientSimpleRuntime) GetActiveMasterAddr() string {
	if c.control != nil {
		return c.control.GetActiveMasterAddr()
	}
	return ""
}

// fileTransferClient is the capability interface implemented by a frontend
// proxy invoke client that supports streaming file upload/download.
type fileTransferClient interface {
	UploadFile(ctx context.Context, instanceID string, path string,
		reader io.Reader, tenantID string) (*frontend_proxy.FileTransferResponse, error)
	DownloadFile(ctx context.Context, instanceID string, path string,
		offset int64, tenantID string) (frontend_proxy.FrontendProxyService_DownloadFileClient, error)
}

// UploadFile streams reader to the owning frontend proxy of instanceID via the
// pooled gRPC client. It is only available when the proxy invoke backend exposes
// the file-transfer capability (routingFrontendProxyInvokeClient).
func (c *clientSimpleRuntime) UploadFile(ctx context.Context, instanceID string, path string,
	reader io.Reader, tenantID string,
) (*frontend_proxy.FileTransferResponse, error) {
	transferClient, ok := c.proxyClient.(fileTransferClient)
	if !ok || transferClient == nil {
		return nil, fmt.Errorf("clientSimpleRuntime.UploadFile requires frontend proxy file transfer client")
	}
	return transferClient.UploadFile(ctx, instanceID, path, reader, tenantID)
}

// DownloadFile opens a server-streaming DownloadFile RPC against the owning
// frontend proxy of instanceID. The caller reads chunks from the returned
// stream until io.EOF.
func (c *clientSimpleRuntime) DownloadFile(ctx context.Context, instanceID string, path string,
	offset int64, tenantID string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	transferClient, ok := c.proxyClient.(fileTransferClient)
	if !ok || transferClient == nil {
		return nil, fmt.Errorf("clientSimpleRuntime.DownloadFile requires frontend proxy file transfer client")
	}
	return transferClient.DownloadFile(ctx, instanceID, path, offset, tenantID)
}

func (c *clientSimpleRuntime) missingLocalObjects(objectIDs []string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	failed := make([]string, 0)
	for _, objectID := range objectIDs {
		if _, ok := c.results[objectID]; !ok {
			failed = append(failed, objectID)
		}
	}
	return failed
}

func (c *clientSimpleRuntime) isLocalObjectID(objectID string) bool {
	if strings.HasPrefix(objectID, "frontend-simple-runtime-") {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.results[objectID]
	return ok
}

func (c *clientSimpleRuntime) classifyObjectIDs(objectIDs []string) (bool, bool) {
	if len(objectIDs) == 0 {
		return true, false
	}
	firstLocal := c.isLocalObjectID(objectIDs[0])
	for _, objectID := range objectIDs[1:] {
		if c.isLocalObjectID(objectID) != firstLocal {
			return false, true
		}
	}
	return firstLocal, false
}

func (c *clientSimpleRuntime) consumeLocalResult(objectID string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.results[objectID]
	if !ok {
		return nil, fmt.Errorf("clientSimpleRuntime.GetAsync local object %q not found", objectID)
	}
	delete(c.results, objectID)
	return value, nil
}

func (c *clientSimpleRuntime) ensureStoresLocked() {
	if c.results == nil {
		c.results = make(map[string][]byte)
	}
}
