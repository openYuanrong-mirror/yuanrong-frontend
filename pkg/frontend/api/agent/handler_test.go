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

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/resspeckey"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/functionmeta"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/route"
)

// stubInstanceFound patches the instance cache to report instanceID as present.
func stubInstanceFound(t *testing.T, instanceID string) *gomonkey.Patches {
	t.Helper()
	return gomonkey.ApplyMethod(
		reflect.TypeOf(instancemanager.GetGlobalInstanceScheduler()),
		"GetInstanceByIDAcrossFunctions",
		func(_ *instancemanager.FunctionInstancesMap, id string) *types.InstanceSpecification {
			if id == instanceID {
				return &types.InstanceSpecification{}
			}
			return nil
		})
}

// runtimeStub is a minimal invokerLibruntime implementation for unit tests.
// Only CreateInstance and Kill are exercised; the rest are no-ops to satisfy
// the interface (mirrors sandbox/handler_test.go).
type runtimeStub struct {
	createInstance func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error)
	kill           func(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error
}

func setAPIClientsForTest(t *testing.T, runtime *runtimeStub) {
	t.Helper()
	util.SetAPIClientLibruntime(runtime)
	restore := util.SetDirectProxyClientForTest(&directRuntimeStub{runtime: runtime})
	t.Cleanup(restore)
}

type directRuntimeStub struct{ runtime *runtimeStub }

func (r *directRuntimeStub) Invoke(util.DirectInvokeRequest) ([]byte, error) { return nil, nil }

func (r *directRuntimeStub) CreateInstance(req util.DirectCreateRequest) (string, error) {
	funcMeta, args, options := req.AdaptedCreateValues()
	return r.runtime.CreateInstance(funcMeta, args, options)
}

func (r *directRuntimeStub) CreateRaw(util.DirectRawRequest) ([]byte, error) { return nil, nil }
func (r *directRuntimeStub) InvokeRaw(util.DirectRawRequest) ([]byte, error) { return nil, nil }

func (r *directRuntimeStub) KillInstance(req util.DirectKillRequest) error {
	return r.runtime.Kill(req.InstanceID, req.Signal, req.Payload, req.AdaptedInvokeOptions())
}

func (r *directRuntimeStub) UploadFile(ctx context.Context, instanceID, path string, reader io.Reader,
	tenantID string,
) (*frontend_proxy.FileTransferResponse, error) {
	return r.runtime.UploadFile(ctx, instanceID, path, reader, tenantID)
}

func (r *directRuntimeStub) DownloadFile(ctx context.Context, instanceID, path string, offset int64,
	tenantID string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	return r.runtime.DownloadFile(ctx, instanceID, path, offset, tenantID)
}

func (r *directRuntimeStub) ListFile(ctx context.Context, instanceID, path string,
	recursive bool, maxDepth int, tenantID string,
) (*frontend_proxy.FileListResponse, error) {
	return r.runtime.ListFile(ctx, instanceID, path, recursive, maxDepth, tenantID)
}

func (r *runtimeStub) Invoke(util.InvokeRequest) ([]byte, error) {
	return nil, nil
}

func (r *runtimeStub) CreateInstance(
	funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions,
) (string, error) {
	if r.createInstance != nil {
		return r.createInstance(funcMeta, args, invokeOpt)
	}
	return "", nil
}

func (r *runtimeStub) InvokeByInstanceId(
	funcMeta api.FunctionMeta, instanceID string, args []api.Arg, invokeOpt api.InvokeOptions,
) (string, error) {
	return "", nil
}

func (r *runtimeStub) InvokeByFunctionName(
	funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions,
) (string, error) {
	return "", nil
}

func (r *runtimeStub) AcquireInstance(
	state string, funcMeta api.FunctionMeta, acquireOpt api.InvokeOptions,
) (api.InstanceAllocation, error) {
	return api.InstanceAllocation{}, nil
}

func (r *runtimeStub) ReleaseInstance(
	allocation api.InstanceAllocation, stateID string, abnormal bool, option api.InvokeOptions,
) {
}

func (r *runtimeStub) Kill(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
	if r.kill != nil {
		return r.kill(instanceID, signal, payload, invokeOpt)
	}
	return nil
}

func (r *runtimeStub) UploadFile(
	context.Context, string, string, io.Reader, string,
) (*frontend_proxy.FileTransferResponse, error) {
	return nil, nil
}

func (r *runtimeStub) DownloadFile(
	context.Context, string, string, int64, string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	return nil, nil
}

func (r *runtimeStub) ListFile(
	context.Context, string, string, bool, int, string,
) (*frontend_proxy.FileListResponse, error) {
	return nil, nil
}

func (r *runtimeStub) CreateInstanceRaw(createReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	return nil, nil
}

func (r *runtimeStub) InvokeByInstanceIdRaw(invokeReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	return nil, nil
}

func (r *runtimeStub) KillRaw(killReqRaw []byte, option api.RawRequestOption) ([]byte, error) {
	return nil, nil
}

func (r *runtimeStub) SaveState(state []byte) (string, error) {
	return "", nil
}

func (r *runtimeStub) LoadState(checkpointID string) ([]byte, error) {
	return nil, nil
}

func (r *runtimeStub) Exit(code int, message string) {}

func (r *runtimeStub) KVSet(key string, value []byte, param api.SetParam) error {
	return nil
}

func (r *runtimeStub) KVSetWithoutKey(value []byte, param api.SetParam) (string, error) {
	return "", nil
}

func (r *runtimeStub) KVGet(key string, timeoutms uint) ([]byte, error) {
	return nil, nil
}

func (r *runtimeStub) KVGetMulti(keys []string, timeoutms uint) ([][]byte, error) {
	return nil, nil
}

func (r *runtimeStub) KVDel(key string) error {
	return nil
}

func (r *runtimeStub) KVDelMulti(keys []string) ([]string, error) {
	return nil, nil
}

func (r *runtimeStub) SetTraceID(traceID string) {}

func (r *runtimeStub) Put(objectID string, value []byte, param api.PutParam, nestedObjectIDs ...string) error {
	return nil
}

func (r *runtimeStub) Get(objectIDs []string, timeoutMs int) ([][]byte, error) {
	return nil, nil
}

func (r *runtimeStub) GIncreaseRef(objectIDs []string, remoteClientID ...string) ([]string, error) {
	return nil, nil
}

func (r *runtimeStub) GDecreaseRef(objectIDs []string, remoteClientID ...string) ([]string, error) {
	return nil, nil
}

func (r *runtimeStub) GetAsync(objectID string, cb api.GetAsyncCallback) {}

func (r *runtimeStub) GetEvent(objectID string, cb api.GetEventCallback) {}

func (r *runtimeStub) DeleteGetEventCallback(objectID string) {}

func (r *runtimeStub) GetFormatLogger() api.FormatLogger {
	return nil
}

func (r *runtimeStub) GetCredential() api.Credential {
	return api.Credential{}
}

func (r *runtimeStub) SetTenantID(tenantID string) error {
	return nil
}

func (r *runtimeStub) IsHealth() bool {
	return true
}

func (r *runtimeStub) IsDsHealth() bool {
	return true
}

func (r *runtimeStub) GetActiveMasterAddr() string {
	return ""
}

// stubFuncSpec patches functionmeta.LoadFuncSpec so unit tests do not hit etcd (whose client is
// nil in unit tests → nil-pointer panic in fetchMetaEtcdWithSingleFlight). The stub returns a
// deterministic funcSpec (runtime=python3.11 + rootfs/sandboxType) that CreateHandler maps to the
// faas system executor function key. Callers must run it via `defer stubFuncSpec(t).Reset()`.
func stubFuncSpec(t *testing.T) *gomonkey.Patches {
	t.Helper()
	return gomonkey.ApplyFunc(functionmeta.LoadFuncSpec, func(funcKey string) (*types.FuncSpec, bool) {
		return &types.FuncSpec{
			FuncMetaData: types.FuncMetaData{Runtime: "python3.11"},
			RootfsSpecMeta: types.RootfsSpecMeta{
				Type: "image", ImageURL: "yr-docker-runtime:v0", User: "agentos", Ports: []string{"tcp:22"},
			},
			SandboxType: "docker",
		}, true
	})
}

// validAgentURN is a URN accepted by urnutils.FunctionURN.ParseFrom.
// Format: ProductID:RegionID:BusinessID:TenantID:TypeSign:FuncName:Version.
// CombineFunctionKey parses it into the funcKey "agentTenant/agentFunc/1".
const validAgentURN = "urn:sn:default:agentTenant:sign:agentFunc:1"

const (
	// testAgentCPU/testAgentMemory are the resource values stubbed into funcMeta and asserted
	// in resource-sinking tests (registered mode sinks them from funcMeta.ResourceMetaData).
	testAgentCPU    = 600
	testAgentMemory = 512
)

// newAgentCreateRecorder builds a gin test context with a JSON CreateAgentRequest body.
func newAgentCreateRecorder(t *testing.T, req CreateAgentRequest) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(req)
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/agent", bytes.NewReader(body))
	require.NoError(t, err)
	return recorder, ctx
}

func TestCreateHandlerBuildsAgentFuncMetaFromURN(t *testing.T) {
	var capturedFuncMeta api.FunctionMeta
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-urn", nil
		},
	})
	// agent reuses the faas system executor function as FuncID; the runtime to map comes from the
	// frontend-watched funcSpecMap. Mock LoadFuncSpec so the test does not hit etcd (uninitialized in
	// unit tests → nil client panic) and returns a deterministic runtime (python3.11).
	defer stubFuncSpec(t).Reset()

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-a",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	// FuncID is the faas system executor function key (mapped from runtime via
	// getAgentExecutorFuncKey), not the URN string nor the user funcKey.
	require.NotEqual(t, validAgentURN, capturedFuncMeta.FuncID)
	require.Contains(t, capturedFuncMeta.FuncID, "0-system-faasExecutorPython3.11")
	require.Equal(t, api.Python, capturedFuncMeta.Language)
	require.Equal(t, api.ActorApi, capturedFuncMeta.Api)
	// Name intentionally left empty so function_proxy generates a random UUID instance_id.
	require.Nil(t, capturedFuncMeta.Name)
	require.NotNil(t, capturedFuncMeta.Namespace)
	require.Equal(t, "agent-ns", *capturedFuncMeta.Namespace)
	// The URN is propagated as the function key note for downstream routing.
	require.Equal(t, validAgentURN, capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])

	// Response is direct JSON (not base64-wrapped job.Response).
	var resp struct {
		Code       int    `json:"code"`
		InstanceID string `json:"instance_id"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, "instance-urn", resp.InstanceID)
}

func TestCreateHandlerReturnsInstanceIDDirectly(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			return "0b6c6322-6533-4901-8000-00000000bb0b", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-uuid",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"code":200,"instance_id":"0b6c6322-6533-4901-8000-00000000bb0b"}`, recorder.Body.String())
}

func TestCreateHandlerRejectsInvalidURN(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called for an invalid URN")
			return "", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-bad",
		Urn:       "not-a-valid-urn",
		Workspace: "/home/snuser/workspaceA",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "invalid urn")
}

func TestCreateHandlerRejectsMissingRequiredFields(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called for an invalid request body")
			return "", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req, err := http.NewRequest(http.MethodPost, "/api/agent", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	ctx.Request = req

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "invalid request body")
}

func TestCreateHandlerSetsDetachedAndReservedCreateOptions(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-opts", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-opts",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})
	ctx.Request.Header.Set(constant.HeaderTenantID, "header-tenant")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "detached", capturedInvokeOpt.CustomExtensions["lifecycle"])
	require.Equal(t, agentConcurrency, capturedInvokeOpt.CustomExtensions["Concurrency"])
	require.Equal(t, agentInstanceType, capturedInvokeOpt.CreateOpt[constant.InstanceTypeNote])
	require.Equal(t, "false", capturedInvokeOpt.CreateOpt[constant.SchedulerManagedNote])
	require.Equal(t, validAgentURN, capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
	require.Equal(t, "header-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
	createOpts := capturedInvokeOpt.CreateOpt
	require.Equal(t, fmt.Sprintf("%d", agentCreateTimeoutSeconds), createOpts["call_timeout"])
	require.Equal(t, fmt.Sprintf("%d", agentInitTimeoutSeconds), createOpts["init_call_timeout"])
	require.Equal(t, fmt.Sprintf("%d", agentGracefulShutdownSeconds),
		createOpts["GRACEFUL_SHUTDOWN_TIME"])
	require.Equal(t, agentDelegateDirectory, createOpts["DELEGATE_DIRECTORY_INFO"])
	require.Equal(t, fmt.Sprintf("%d", agentDirectoryQuotaMB), createOpts["DELEGATE_DIRECTORY_QUOTA"])
	require.Equal(t, agentConcurrency, createOpts["ConcurrentNum"])

	var resSpec resspeckey.ResourceSpecification
	require.NoError(t, json.Unmarshal([]byte(capturedInvokeOpt.CreateOpt[constant.ResourceSpecNote]), &resSpec))
	require.EqualValues(t, defaultAgentCPU, resSpec.CPU)
	require.EqualValues(t, defaultAgentMemory, resSpec.Memory)
	// recover_retry_times 未传 → frontend 填默认 defaultRecoverRetryTimes=3 启用重拉。
	require.Equal(t, "3", capturedInvokeOpt.CreateOpt["RecoverRetryTimes"])
}

func TestCreateHandlerRegisteredSinksFuncMetaResources(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-res", nil
		},
	})
	// stub funcSpec with CPU/Memory so registered mode sinks them from funcMeta.
	defer gomonkey.ApplyFunc(functionmeta.LoadFuncSpec, func(funcKey string) (*types.FuncSpec, bool) {
		return &types.FuncSpec{
			FuncMetaData:     types.FuncMetaData{Runtime: "python3.11"},
			RootfsSpecMeta:   types.RootfsSpecMeta{ImageURL: "yr-docker-runtime:v0", User: "agentos"},
			SandboxType:      "docker",
			ResourceMetaData: types.ResourceMetaData{CPU: testAgentCPU, Memory: testAgentMemory},
		}, true
	}).Reset()

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-res",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resSpec resspeckey.ResourceSpecification
	require.NoError(t, json.Unmarshal([]byte(capturedInvokeOpt.CreateOpt[constant.ResourceSpecNote]), &resSpec))
	require.EqualValues(t, testAgentCPU, resSpec.CPU)
	require.EqualValues(t, testAgentMemory, resSpec.Memory)
}

func TestCreateHandlerMountsWorkspaceAndCustomMounts(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-mounts", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-mounts",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
		Mounts: []Mount{
			{Source: "/home/snuser/workspaceB", Target: "/mnt/workspaceB", ReadOnly: true},
		},
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	// host_user is transparently passed by frontend from funcMeta.rootfs.user (proxy does not
	// merge funcMeta). The stub funcSpec sets rootfs.user=agentos.
	require.Equal(t, "agentos", capturedInvokeOpt.CreateOpt["host_user"])
	// rootfs JSON carries workspace + custom mounts + the image (type/imageurl merged by
	// applyAgentFuncMeta from funcMeta.rootfs.imageurl); the workspace target placeholder
	// (__AGENT_USER__) is replaced by frontend with funcMeta.rootfs.user.
	require.JSONEq(t,
		`{"mounts":[
			{"source":"/home/snuser/workspaceA","target":"/home/agentos","readonly":false},
			{"source":"/home/snuser/workspaceB","target":"/mnt/workspaceB","readonly":true}],
			"type":"image","imageurl":"yr-docker-runtime:v0"}`,
		capturedInvokeOpt.CreateOpt["rootfs"],
	)
}

func TestCreateHandlerRejectsUnsafeWorkspace(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called for an unsafe workspace path")
			return "", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-unsafe",
		Urn:       validAgentURN,
		Workspace: "/etc",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "unsafe workspace")
}

func TestCreateHandlerRejectsRelativeWorkspace(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called for a relative workspace path")
			return "", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-rel",
		Urn:       validAgentURN,
		Workspace: "relative/path",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "absolute path")
}

func TestCreateHandlerSinksDynamicEnvVars(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-env", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-env",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
		EnvVars:   map[string]string{"userid": "u-123", "TRACE_ID": "trace-456"},
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"userid":"u-123","TRACE_ID":"trace-456"}`, capturedInvokeOpt.CreateOpt["DELEGATE_ENV_VAR"])
}

func TestCreateHandlerReturnsInstanceIDWhenCreateTimesOutAfterScheduling(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	oldWaitForRunning := waitForAgentInstanceRunning
	waitCalled := false
	waitForAgentInstanceRunning = func(instanceID, functionID, resourceSpecNote string) bool {
		waitCalled = true
		require.Equal(t, "instance-created-late", instanceID)
		require.NotEmpty(t, functionID)
		require.NotEmpty(t, resourceSpecNote)
		return true
	}
	defer func() {
		waitForAgentInstanceRunning = oldWaitForRunning
	}()

	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			return "instance-created-late", api.ErrorInfo{
				Code: agentCreateTimeoutCode,
				Err:  fmt.Errorf("create instance timeout"),
			}
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-late",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, waitCalled)
	require.JSONEq(t, `{"code":200,"instance_id":"instance-created-late"}`, recorder.Body.String())
}

func TestCreateHandlerReturns500WhenCreateFails(t *testing.T) {
	defer stubFuncSpec(t).Reset()
	oldWaitForRunning := waitForAgentInstanceRunning
	// Non-timeout error must not trigger the wait-for-running path.
	waitForAgentInstanceRunning = func(instanceID, functionID, resourceSpecNote string) bool {
		t.Fatalf("wait should not be called for a non-timeout create error")
		return false
	}
	defer func() {
		waitForAgentInstanceRunning = oldWaitForRunning
	}()

	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			return "", fmt.Errorf("scheduler refused")
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-fail",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "failed to create agent")
}

func TestShouldTreatCreateTimeoutAsSuccess(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		err        error
		want       bool
	}{
		{
			name:       "timeout with instance id",
			instanceID: "inst-1",
			err:        api.ErrorInfo{Code: agentCreateTimeoutCode},
			want:       true,
		},
		{name: "non-timeout error", instanceID: "inst-1", err: api.ErrorInfo{Code: 5000}, want: false},
		{name: "empty instance id", instanceID: "", err: api.ErrorInfo{Code: agentCreateTimeoutCode}, want: false},
		{name: "nil error", instanceID: "inst-1", err: nil, want: false},
		{name: "non-errorinfo error", instanceID: "inst-1", err: fmt.Errorf("plain"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldTreatCreateTimeoutAsSuccess(tt.instanceID, tt.err))
		})
	}
}

func TestIsSafeBindSource(t *testing.T) {
	for _, p := range []string{"/", "/etc", "/proc", "/sys", "/dev", "/boot"} {
		require.False(t, isSafeBindSource(p), "expected %s to be unsafe", p)
	}
	for _, p := range []string{
		"/home/agentos", "/home/snuser/workspaceA", "/tmp/workspace", "/mnt/data",
	} {
		require.True(t, isSafeBindSource(p), "expected %s to be safe", p)
	}
	// path traversal and docker socket must be rejected
	require.False(t, isSafeBindSource("/home/../etc"))
	require.False(t, isSafeBindSource("/var/run/docker.sock"))
}

func TestDeleteHandlerDeletesAgentInstance(t *testing.T) {
	const instanceID = "0b6c6322-6533-4901-8000-00000000bb0b"
	defer stubInstanceFound(t, instanceID).Reset()

	var (
		capturedInstanceID string
		capturedSignal     int
		capturedPayload    []byte
		capturedInvokeOpt  api.InvokeOptions
	)
	setAPIClientsForTest(t, &runtimeStub{
		kill: func(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
			capturedInstanceID = instanceID
			capturedSignal = signal
			capturedPayload = append([]byte(nil), payload...)
			capturedInvokeOpt = invokeOpt
			return nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: instanceID}}
	req, err := http.NewRequest(http.MethodDelete, "/api/agent/"+instanceID, nil)
	require.NoError(t, err)
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, instanceID, capturedInstanceID)
	require.Equal(t, agentKillInstanceSignal, capturedSignal)
	require.Equal(t, []byte("agent deleted"), capturedPayload)
	// KillByLibRt always passes an empty InvokeOptions to the libruntime client.
	require.Equal(t, api.InvokeOptions{}, capturedInvokeOpt)
	require.JSONEq(t, `{"code":200,"status":"deleted"}`, recorder.Body.String())
}

func TestDeleteHandlerReturns500WhenKillFails(t *testing.T) {
	const instanceID = "agent-delete-fail"
	defer stubInstanceFound(t, instanceID).Reset()

	setAPIClientsForTest(t, &runtimeStub{
		kill: func(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
			return fmt.Errorf("kill failed")
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: instanceID}}
	req, err := http.NewRequest(http.MethodDelete, "/api/agent/"+instanceID, nil)
	require.NoError(t, err)
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "failed to delete agent")
}

func TestDeleteHandlerReturns404ForNonExistentInstance(t *testing.T) {
	// No stubInstanceFound patch: instance cache reports nil for unknown IDs,
	// so a non-existent or already-deleted instanceID returns 404, not 200.
	killCalled := false
	setAPIClientsForTest(t, &runtimeStub{
		kill: func(string, int, []byte, api.InvokeOptions) error {
			killCalled = true
			return nil
		},
	})

	for _, instanceID := range []string{"non-existent-uuid-12345", "agent-kill-twice"} {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Params = gin.Params{{Key: "instanceId", Value: instanceID}}
		req, err := http.NewRequest(http.MethodDelete, "/api/agent/"+instanceID, nil)
		require.NoError(t, err)
		ctx.Request = req

		DeleteHandler(ctx)

		require.Equal(t, http.StatusNotFound, recorder.Code, instanceID)
		require.Contains(t, recorder.Body.String(), "instance not found", instanceID)
	}
	require.False(t, killCalled, "kill must not be called for non-existent instance")
}

// inlineRootfsReq is an inline-mode CreateAgentRequest (RuntimeSpec set, no Urn) used by the
// inline-mode tests below. Mirrors the stubFuncSpec spec (python3.11 / docker / agentos) so
// expectations line up with the registered-mode equivalents.
func inlineRootfsReq() CreateAgentRequest {
	return CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-inline",
		RuntimeSpec: &RuntimeSpec{
			Runtime:     "python3.11",
			SandboxType: "docker",
			Rootfs: &RootfsSpec{
				ImageURL: "yr-docker-runtime:v0",
				User:     "agentos",
				Ports:    []string{"tcp:22"},
			},
		},
		Workspace: "/home/snuser/workspaceA",
	}
}

func TestCreateHandlerInlineBuildsFuncMeta(t *testing.T) {
	var capturedFuncMeta api.FunctionMeta
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-inline", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, inlineRootfsReq())
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, capturedFuncMeta.FuncID, "0-system-faasExecutorPython3.11")
	require.Equal(t, api.Python, capturedFuncMeta.Language)
	require.Equal(t, api.ActorApi, capturedFuncMeta.Api)
	require.Nil(t, capturedFuncMeta.Name)
	require.Equal(t, "agent-ns", *capturedFuncMeta.Namespace)
	// inline mode: FunctionKeyNote is the composed funcKey, not a URN.
	require.Equal(t, "default/agent-inline/latest", capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
	// container config comes straight from the request.
	require.Equal(t, "docker", capturedInvokeOpt.CreateOpt["sandbox_type"])
	require.Equal(t, "agentos", capturedInvokeOpt.CreateOpt["host_user"])
	require.JSONEq(t,
		`{"mounts":[{"source":"/home/snuser/workspaceA","target":"/home/agentos","readonly":false}],
		  "type":"image","imageurl":"yr-docker-runtime:v0"}`,
		capturedInvokeOpt.CreateOpt["rootfs"])
	require.JSONEq(t, `{"portForwardings":[{"port":22,"protocol":"TCP"}]}`,
		capturedInvokeOpt.CreateOpt["network"])
}

func TestCreateHandlerInlineEmptyUserFallsBackToDefaultTarget(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-inline-nouser", nil
		},
	})

	req := inlineRootfsReq()
	req.RuntimeSpec.Rootfs.User = "" // optional field omitted
	recorder, ctx := newAgentCreateRecorder(t, req)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	// no host_user when User is empty; workspace target falls back to /home/agentos (not literal __AGENT_USER__).
	_, hasHostUser := capturedInvokeOpt.CreateOpt["host_user"]
	require.False(t, hasHostUser)
	require.NotContains(t, capturedInvokeOpt.CreateOpt["rootfs"], agentUserPlaceholder)
	require.Contains(t, capturedInvokeOpt.CreateOpt["rootfs"], `"/home/agentos"`)
}

func TestCreateHandlerInlineSupervisorToleratesEmptyImageURL(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-inline-supervisor", nil
		},
	})

	req := inlineRootfsReq()
	req.RuntimeSpec.SandboxType = agentSandboxTypeSupervisor
	req.RuntimeSpec.Rootfs.ImageURL = "" // supervisor runs without a container image
	recorder, ctx := newAgentCreateRecorder(t, req)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	// sandbox_type is sunk as-is; rootfs carries mounts only (no image url).
	require.Equal(t, agentSandboxTypeSupervisor, capturedInvokeOpt.CreateOpt["sandbox_type"])
	require.NotContains(t, capturedInvokeOpt.CreateOpt["rootfs"], "imageurl")
}

func TestCreateHandlerInlineRejectsEmptyImageURLForDocker(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called when imageurl is empty for non-supervisor sandbox")
			return "", nil
		},
	})

	req := inlineRootfsReq()
	req.RuntimeSpec.Rootfs.ImageURL = "" // docker requires an image
	recorder, ctx := newAgentCreateRecorder(t, req)
	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "rootfs.imageurl is required")
}

func TestCreateHandlerRegisteredSupervisorToleratesEmptyImageURL(t *testing.T) {
	defer gomonkey.ApplyFunc(functionmeta.LoadFuncSpec, func(funcKey string) (*types.FuncSpec, bool) {
		return &types.FuncSpec{
			FuncMetaData:   types.FuncMetaData{Runtime: "python3.11"},
			RootfsSpecMeta: types.RootfsSpecMeta{User: "agentos"}, // imageurl intentionally empty
			SandboxType:    agentSandboxTypeSupervisor,
		}, true
	}).Reset()
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			return "instance-reg-supervisor", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-reg-supervisor",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestCreateHandlerRegisteredRejectsEmptyImageURLForDocker(t *testing.T) {
	defer gomonkey.ApplyFunc(functionmeta.LoadFuncSpec, func(funcKey string) (*types.FuncSpec, bool) {
		return &types.FuncSpec{
			FuncMetaData:   types.FuncMetaData{Runtime: "python3.11"},
			RootfsSpecMeta: types.RootfsSpecMeta{User: "agentos"}, // imageurl empty, sandboxType docker
			SandboxType:    "docker",
		}, true
	}).Reset()
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called when registered funcSpec lacks imageurl for docker")
			return "", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-reg-noimg",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})
	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "rootfs.imageurl is required")
}

func TestCreateHandlerInlineSinksEnvVars(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-inline-env", nil
		},
	})

	req := inlineRootfsReq()
	req.EnvVars = map[string]string{"AGENT_MODE": "prod", "userid": "u-9f3a"}
	recorder, ctx := newAgentCreateRecorder(t, req)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"AGENT_MODE":"prod","userid":"u-9f3a"}`,
		capturedInvokeOpt.CreateOpt["DELEGATE_ENV_VAR"])
}

func TestCreateHandlerRejectsMissingInlineAndUrn(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called when neither inline nor urn is set")
			return "", nil
		},
	})

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-none",
		Workspace: "/home/snuser/workspaceA",
	})
	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "runtime_spec")
	require.Contains(t, recorder.Body.String(), "urn")
}

func TestCreateHandlerInlineOverridesUrn(t *testing.T) {
	var capturedFuncMeta api.FunctionMeta
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-both", nil
		},
	})
	// stubFuncSpec still applied: inline fields must win, LoadFuncSpec result must be ignored.
	defer stubFuncSpec(t).Reset()

	req := inlineRootfsReq()
	req.Urn = validAgentURN // both set → inline wins
	recorder, ctx := newAgentCreateRecorder(t, req)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	// FuncID from inline runtime (python3.11), FunctionKeyNote from inline funcKey (not URN).
	require.Contains(t, capturedFuncMeta.FuncID, "0-system-faasExecutorPython3.11")
	require.Equal(t, "default/agent-inline/latest", capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
}

// --- List/Get handler tests ---

// stubListGetLookups swaps the package-level indirections for List/Get handlers
// (cache is empty in unit tests). The returned func restores the originals.
func stubListGetLookups(t *testing.T, summaries []execendpoint.Summary,
	endpoints map[string]execendpoint.Endpoint,
) func() {
	t.Helper()
	oldSummaries := lookupAgentInstanceSummaries
	oldEndpoint := lookupAgentInstanceEndpoint
	oldExtract := extractNodeIP
	lookupAgentInstanceSummaries = func(tenantID, instanceID string) []execendpoint.Summary {
		return summaries
	}
	lookupAgentInstanceEndpoint = func(instanceID string) (execendpoint.Endpoint, bool) {
		ep, ok := endpoints[instanceID]
		return ep, ok
	}
	extractNodeIP = func(proxyGrpcAddress string) string { return route.ExtractIP(proxyGrpcAddress) }
	return func() {
		lookupAgentInstanceSummaries = oldSummaries
		lookupAgentInstanceEndpoint = oldEndpoint
		extractNodeIP = oldExtract
	}
}

func newAgentGetRecorder(t *testing.T, method, target string) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req, err := http.NewRequest(method, target, nil)
	require.NoError(t, err)
	ctx.Request = req
	return recorder, ctx
}

// sampleAgentSummaries builds two canned summaries (docker + supervisor) for List/Get tests.
func sampleAgentSummaries() []execendpoint.Summary {
	return []execendpoint.Summary{
		{
			InstanceID:  "inst-docker-1",
			TenantID:    "tenantA",
			Function:    "tenantA/agentFunc/1",
			ContainerID: "4fb6aa1c",
			ContainerIP: "172.17.0.5",
			SandboxType: "docker",
			StartTime:   "2026-07-30T03:00:00Z",
			CreateOptions: map[string]string{
				"sandbox_type":     "docker",
				"host_user":        "agentos",
				"DELEGATE_ENV_VAR": `{"FOO":"bar"}`,
				"rootfs": `{"type":"image","imageurl":"yr-docker-runtime:v0","mounts":[` +
					`{"source":"/data","target":"/data","readonly":false}]}`,
				"network": `{"portForwardings":[{"port":22,"protocol":"TCP"}]}`,
			},
		},
		{
			InstanceID:  "inst-sup-1",
			TenantID:    "tenantA",
			Function:    "tenantA/agentFunc/2",
			SandboxType: "supervisor",
			StartTime:   "2026-07-30T03:01:00Z",
			CreateOptions: map[string]string{
				"sandbox_type": "supervisor",
			},
		},
	}
}

func TestListHandlerReturnsAllInstances(t *testing.T) {
	summaries := sampleAgentSummaries()
	endpoints := map[string]execendpoint.Endpoint{
		"inst-docker-1": {InstanceID: "inst-docker-1", ProxyGrpcAddress: "10.0.0.5:50051"},
		"inst-sup-1":    {InstanceID: "inst-sup-1", ProxyGrpcAddress: "10.0.0.6:50051"},
	}
	defer stubListGetLookups(t, summaries, endpoints)()

	recorder, ctx := newAgentGetRecorder(t, http.MethodGet, "/api/agent")
	ListHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Code      int             `json:"code"`
		Instances []InstanceBrief `json:"instances"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)
	require.Len(t, resp.Instances, len(summaries))
	// ListSummaries sorts by InstanceID, so inst-docker-1 precedes inst-sup-1.
	docker := resp.Instances[0]
	require.Equal(t, "inst-docker-1", docker.InstanceID)
	require.Equal(t, "10.0.0.5", docker.NodeIP)
	require.Equal(t, "172.17.0.5", docker.SandboxIP)
	require.Equal(t, "docker", docker.SandboxType)
	sup := resp.Instances[1]
	require.Equal(t, "inst-sup-1", sup.InstanceID)
	require.Equal(t, "10.0.0.6", sup.NodeIP)
	// supervisor is host-networked: no containerIP in etcd, sandbox_ip stays empty.
	require.Empty(t, sup.SandboxIP)
	require.Equal(t, "supervisor", sup.SandboxType)
}

func TestListHandlerSkipsInstancesWithoutSandboxType(t *testing.T) {
	// System drivers (driver-frontend/driver-scheduler) register in the instance cache
	// without a sandbox_type; List must not surface them as user agent instances.
	summaries := []execendpoint.Summary{
		{InstanceID: "driver-frontend-node1"},
		{InstanceID: "driver-scheduler-node1"},
		{InstanceID: "inst-docker-1", SandboxType: "docker"},
	}
	defer stubListGetLookups(t, summaries, nil)()

	recorder, ctx := newAgentGetRecorder(t, http.MethodGet, "/api/agent")
	ListHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Code      int             `json:"code"`
		Instances []InstanceBrief `json:"instances"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Len(t, resp.Instances, 1)
	require.Equal(t, "inst-docker-1", resp.Instances[0].InstanceID)
}

func TestListHandlerEmptyReturnsEmptyArray(t *testing.T) {
	defer stubListGetLookups(t, nil, nil)()

	recorder, ctx := newAgentGetRecorder(t, http.MethodGet, "/api/agent")
	ListHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"code":200,"instances":[]}`, recorder.Body.String())
}

func TestGetHandlerReturnsSingleInstance(t *testing.T) {
	summaries := sampleAgentSummaries()
	endpoints := map[string]execendpoint.Endpoint{
		"inst-docker-1": {InstanceID: "inst-docker-1", ProxyGrpcAddress: "10.0.0.5:50051"},
	}
	defer stubListGetLookups(t, summaries, endpoints)()

	recorder, ctx := newAgentGetRecorder(t, http.MethodGet, "/api/agent/inst-docker-1")
	ctx.Params = gin.Params{{Key: "instanceId", Value: "inst-docker-1"}}
	GetHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Code     int            `json:"code"`
		Instance InstanceDetail `json:"instance"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)

	d := resp.Instance
	require.Equal(t, "inst-docker-1", d.InstanceID)
	require.Equal(t, "10.0.0.5", d.NodeIP)
	require.Equal(t, "172.17.0.5", d.SandboxIP)
	require.Equal(t, "docker", d.SandboxType)
	require.Equal(t, "4fb6aa1c", d.SandboxID)
	require.Equal(t, "agentos", d.HostUser)
	require.Equal(t, "bar", d.EnvVars["FOO"])
	require.Equal(t, []string{"tcp:22"}, d.Ports)
	require.NotNil(t, d.Rootfs)
	require.Equal(t, "image", d.Rootfs.Type)
	require.Equal(t, "yr-docker-runtime:v0", d.Rootfs.ImageURL)
	require.Len(t, d.Rootfs.Mounts, 1)
	require.Equal(t, "/data", d.Rootfs.Mounts[0].Source)
	require.False(t, d.Rootfs.Mounts[0].ReadOnly)
}

func TestGetHandlerReturns404WhenNotFound(t *testing.T) {
	// Empty summary list ⇒ cache miss (instance not RUNNING or absent).
	defer stubListGetLookups(t, nil, nil)()

	recorder, ctx := newAgentGetRecorder(t, http.MethodGet, "/api/agent/unknown-id")
	ctx.Params = gin.Params{{Key: "instanceId", Value: "unknown-id"}}
	GetHandler(ctx)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Contains(t, recorder.Body.String(), "instance not found")
}

// ---- File transfer handler tests ----

func TestParseSingleRange(t *testing.T) {
	convey.Convey("parseSingleRange should handle various Range headers", t, func() {
		convey.Convey("valid bytes=<start>-", func() {
			offset, ok := parseSingleRange("bytes=100-")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(offset, convey.ShouldEqual, 100)
		})
		convey.Convey("valid with spaces", func() {
			offset, ok := parseSingleRange("  bytes=50-  ")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(offset, convey.ShouldEqual, 50)
		})
		convey.Convey("case insensitive prefix", func() {
			offset, ok := parseSingleRange("BYTES=200-")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(offset, convey.ShouldEqual, 200)
		})
		convey.Convey("zero offset", func() {
			offset, ok := parseSingleRange("bytes=0-")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(offset, convey.ShouldEqual, 0)
		})
		convey.Convey("suffix range not supported", func() {
			_, ok := parseSingleRange("bytes=-500")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("missing prefix", func() {
			_, ok := parseSingleRange("0-100")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("missing dash", func() {
			_, ok := parseSingleRange("bytes=100")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("empty start", func() {
			_, ok := parseSingleRange("bytes=-")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("negative offset", func() {
			_, ok := parseSingleRange("bytes=-1-")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("non-numeric", func() {
			_, ok := parseSingleRange("bytes=abc-")
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}

func TestCountingReader(t *testing.T) {
	convey.Convey("countingReader should count bytes", t, func() {
		data := bytes.Repeat([]byte("A"), 100)
		cr := &countingReader{reader: bytes.NewReader(data)}
		buf := make([]byte, 50)
		n, err := cr.Read(buf)
		convey.So(err, convey.ShouldBeNil)
		convey.So(n, convey.ShouldEqual, 50)
		convey.So(cr.count, convey.ShouldEqual, 50)

		n, err = cr.Read(buf)
		convey.So(err, convey.ShouldBeNil)
		convey.So(n, convey.ShouldEqual, 50)
		convey.So(cr.count, convey.ShouldEqual, 100)
	})

	convey.Convey("countingReader should reject oversized reads", t, func() {
		data := bytes.Repeat([]byte("B"), 200)
		cr := &countingReader{reader: bytes.NewReader(data)}

		cr.count = maxFileUploadSize - 50
		buf := make([]byte, 100)
		n, err := cr.Read(buf)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(strings.Contains(err.Error(), "exceeds max"), convey.ShouldBeTrue)
		convey.So(n <= 100, convey.ShouldBeTrue)
		convey.So(cr.count > maxFileUploadSize, convey.ShouldBeTrue)
	})
}

func TestWriteFileTransferError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	convey.Convey("writeFileTransferError should map errors to HTTP status codes", t, func() {
		convey.Convey("nil error does nothing", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeFileTransferError(ctx, nil)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		})
		convey.Convey("contains 'exceeds max' → 413", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeFileTransferError(ctx, errors.New("upload size exceeds max 512MB"))
			convey.So(w.Code, convey.ShouldEqual, http.StatusRequestEntityTooLarge)
		})
		convey.Convey("contains 'is required' → 400", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeFileTransferError(ctx, errors.New("path is required"))
			convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		})
		convey.Convey("generic error → 500", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeFileTransferError(ctx, errors.New("internal failure"))
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})
	})
}

func TestWriteMultipartReadError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	convey.Convey("writeMultipartReadError should map errors correctly", t, func() {
		convey.Convey("nil error does nothing", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeMultipartReadError(ctx, "inst1", nil)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		})
		convey.Convey("body too large → 413", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeMultipartReadError(ctx, "inst1", errors.New("http: request body too large"))
			convey.So(w.Code, convey.ShouldEqual, http.StatusRequestEntityTooLarge)
		})
		convey.Convey("generic multipart error → 400", func() {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			writeMultipartReadError(ctx, "inst1", io.ErrUnexpectedEOF)
			convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		})
	})
}

func TestIsFileNotFoundError(t *testing.T) {
	convey.Convey("isFileNotFoundError should return false for nil", t, func() {
		convey.So(isFileNotFoundError(nil), convey.ShouldBeFalse)
	})
	convey.Convey("isFileNotFoundError should return false for generic error", t, func() {
		convey.So(isFileNotFoundError(errors.New("something else")), convey.ShouldBeFalse)
	})
}
