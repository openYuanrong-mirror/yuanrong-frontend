package sandbox

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/ugorji/go/codec"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/resspeckey"
	"frontend/pkg/common/job"
	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
)

const (
	invokeEnvelopeArgCount     = 4
	packedArgPairSize          = 2
	customCreateTimeoutSeconds = 37
	testCPULimit               = 2000
	testMemoryLimit            = 4096
)

type sandboxTimeoutTestCase struct {
	name             string
	createTimeout    int
	scheduleTimeout  int
	wantCreate       int
	wantSchedule     int
	wantErrorMessage string
}

type runtimeStub struct {
	createInstance func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error)
	invokeInstance func(
		funcMeta api.FunctionMeta,
		instanceID string,
		args []api.Arg,
		invokeOpt api.InvokeOptions,
	) (string, error)
	getAsync func(objectID string, cb api.GetAsyncCallback)
	kill     func(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error
}

func (r *runtimeStub) CreateInstance(
	funcMeta api.FunctionMeta,
	args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	if r.createInstance != nil {
		return r.createInstance(funcMeta, args, invokeOpt)
	}
	return "", nil
}

func (r *runtimeStub) InvokeByInstanceId(
	funcMeta api.FunctionMeta,
	instanceID string,
	args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	if r.invokeInstance != nil {
		return r.invokeInstance(funcMeta, instanceID, args, invokeOpt)
	}
	return "", nil
}

func (r *runtimeStub) InvokeByFunctionName(
	funcMeta api.FunctionMeta,
	args []api.Arg,
	invokeOpt api.InvokeOptions,
) (string, error) {
	return "", nil
}

func (r *runtimeStub) AcquireInstance(
	state string,
	funcMeta api.FunctionMeta,
	acquireOpt api.InvokeOptions,
) (api.InstanceAllocation, error) {
	return api.InstanceAllocation{}, nil
}

func (r *runtimeStub) ReleaseInstance(
	allocation api.InstanceAllocation,
	stateID string,
	abnormal bool,
	option api.InvokeOptions,
) {
}

func (r *runtimeStub) Kill(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
	if r.kill != nil {
		return r.kill(instanceID, signal, payload, invokeOpt)
	}
	return nil
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

func (r *runtimeStub) GetAsync(objectID string, cb api.GetAsyncCallback) {
	if r.getAsync != nil {
		r.getAsync(objectID, cb)
	}
}

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

func TestCreateHandlerPropagatesHeaderTenantID(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-from-header-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-from-header", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-a",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTenantID, "header-tenant")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.Equal(t, sandboxModuleName, capturedFuncMeta.ModuleName)
	require.Equal(t, sandboxClassName, capturedFuncMeta.ClassName)
	require.Equal(t, sandboxCreateTimeoutSeconds, capturedInvokeOpt.Timeout)
	require.Equal(t, defaultSandboxFunctionID, capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
	require.Equal(t, "header-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
	require.Empty(t, capturedInvokeOpt.CreateOpt[constant.SchedulerIDNote])
	require.Empty(t, capturedInvokeOpt.SchedulerInstanceIDs)

	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)
}

func TestCreateHandlerFallsBackToBodyTenant(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-from-body-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-from-body", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-b",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
		CpuLimit:  testCPULimit,
		MemLimit:  testMemoryLimit,
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.Equal(t, sandboxCreateTimeoutSeconds, capturedInvokeOpt.Timeout)
	require.Equal(t, defaultSandboxFunctionID, capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
	require.Equal(t, "body-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
	require.Equal(t, strconv.Itoa(testCPULimit), capturedInvokeOpt.CustomExtensions["CPU_LIMIT"])
	require.Equal(t, strconv.Itoa(testMemoryLimit), capturedInvokeOpt.CustomExtensions["Memory_LIMIT"])
	require.Equal(t, testCPULimit, capturedInvokeOpt.CpuLimit)
	require.Equal(t, testMemoryLimit, capturedInvokeOpt.MemoryLimit)
	require.Empty(t, capturedInvokeOpt.CreateOpt[constant.SchedulerIDNote])
	require.Empty(t, capturedInvokeOpt.SchedulerInstanceIDs)
}

func TestCreateHandlerAttributesTenantFromTokenClaim(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(_ api.FunctionMeta, _ []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-from-token", nil
		},
	})

	encode := func(value interface{}) string {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(data)
	}
	token := encode(jwtauth.JWTHeader{Alg: "none", Typ: "JWT"}) + "." +
		encode(jwtauth.JWTPayload{Sub: "token-tenant"}) + ".sig"

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{Name: "sandbox-token", Namespace: "sandbox"})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(jwtauth.HeaderXAuth, token)

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "token-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
}

func TestCreateHandlerReturnsInstanceIDWhenCreateTimesOutAfterScheduling(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-timeout-test", nil
	}
	oldWaitForSandboxInstanceRunning := waitForSandboxInstanceRunning
	waitCalled := false
	waitForSandboxInstanceRunning = func(instanceID, functionID, resourceSpecNote string) bool {
		waitCalled = true
		require.Equal(t, "instance-created-late", instanceID)
		require.Equal(t, defaultSandboxFunctionID, functionID)
		require.NotEmpty(t, resourceSpecNote)
		return true
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
		waitForSandboxInstanceRunning = oldWaitForSandboxInstanceRunning
	}()

	installCreateTimeoutRuntimeStub()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-c",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	assertCreateTimeoutResponse(t, recorder, waitCalled)
}

func installCreateTimeoutRuntimeStub() {
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			return "instance-created-late", api.ErrorInfo{
				Code: createTimeoutSuccessCode,
				Err:  fmt.Errorf("create instance timeout"),
			}
		},
	})
}

func assertCreateTimeoutResponse(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	waitCalled bool,
) {
	t.Helper()
	require.Equal(t, http.StatusOK, recorder.Code)
	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)

	var data map[string]string
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, "instance-created-late", requireStringMapValue(t, data, "instance_id"))
	require.True(t, waitCalled)
}

func TestSandboxFunctionIDUsesRuntime(t *testing.T) {
	tests := []struct {
		name     string
		runtime  string
		wantFunc string
	}{
		{
			name:     "python310",
			runtime:  "python3.10",
			wantFunc: "default/0-defaultservice-py310/$latest",
		},
		{
			name:     "python39",
			runtime:  "python3.9",
			wantFunc: "default/0-defaultservice-py39/$latest",
		},
		{
			name:     "py310",
			runtime:  "py310",
			wantFunc: "default/0-defaultservice-py310/$latest",
		},
		{
			name:     "rust",
			runtime:  "rust",
			wantFunc: "default/0-defaultservice-rrt/$latest",
		},
		{
			name:     "rrt",
			runtime:  "rrt",
			wantFunc: "default/0-defaultservice-rrt/$latest",
		},
		{
			name:     "empty uses default (rust)",
			runtime:  "",
			wantFunc: "default/0-defaultservice-rrt/$latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sandboxFunctionIDForRuntime(tt.runtime)
			require.NoError(t, err)
			require.Equal(t, tt.wantFunc, got)
		})
	}
}

func TestSandboxFunctionIDRejectsUnsupportedRuntime(t *testing.T) {
	_, err := sandboxFunctionIDForRuntime("python3.11")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported sandbox runtime")
}

func TestDefaultSandboxFunctionIDUsesRustService(t *testing.T) {
	// Default sandbox backend is the dedicated Rust (rrt) slot.
	require.Equal(t, "default/0-defaultservice-rrt/$latest", defaultSandboxFunctionID)
}

func TestCreateHandlerUsesRequestedRuntime(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-runtime", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-runtime",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
		Runtime:   "python3.9",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "default/0-defaultservice-py39/$latest", capturedFuncMeta.FuncID)
	require.Equal(t, "default/0-defaultservice-py39/$latest", capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
}

func TestCreateHandlerRejectsUnsupportedRuntime(t *testing.T) {
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called for unsupported runtime")
			return "", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-runtime",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
		Runtime:   "python3.11",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "unsupported sandbox runtime")
}

func TestCreateHandlerAddsSchedulerCreateOptions(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-options-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-options", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-d",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assertSchedulerCreateOptions(t, capturedInvokeOpt)
}

func assertSchedulerCreateOptions(t *testing.T, capturedInvokeOpt api.InvokeOptions) {
	t.Helper()
	require.Equal(t, "detached", capturedInvokeOpt.CustomExtensions["lifecycle"])
	require.Equal(t, sandboxConcurrency, capturedInvokeOpt.CustomExtensions["Concurrency"])
	require.Equal(t, "reserved", capturedInvokeOpt.CreateOpt[constant.InstanceTypeNote])
	_, hasStaticOwner := capturedInvokeOpt.CreateOpt["resource.owner"]
	require.False(t, hasStaticOwner)
	require.Equal(t, fmt.Sprintf("%d", sandboxCreateTimeoutSeconds), capturedInvokeOpt.CreateOpt["call_timeout"])
	require.Equal(t, "305", capturedInvokeOpt.CreateOpt["init_call_timeout"])
	require.Equal(t, "5", capturedInvokeOpt.CreateOpt["GRACEFUL_SHUTDOWN_TIME"])
	require.Equal(t, "/tmp", capturedInvokeOpt.CreateOpt["DELEGATE_DIRECTORY_INFO"])
	require.Equal(t, "512", capturedInvokeOpt.CreateOpt["DELEGATE_DIRECTORY_QUOTA"])
	require.Equal(t, "1", capturedInvokeOpt.CreateOpt["ConcurrentNum"])

	var resSpec resspeckey.ResourceSpecification
	require.NoError(t, json.Unmarshal([]byte(capturedInvokeOpt.CreateOpt[constant.ResourceSpecNote]), &resSpec))
	require.EqualValues(t, sandboxDefaultCPU, resSpec.CPU)
	require.EqualValues(t, sandboxDefaultMemory, resSpec.Memory)
	require.Equal(t, "", resSpec.InvokeLabel)
	require.Empty(t, capturedInvokeOpt.SchedulerInstanceIDs)
}

func TestCreateHandlerPassesRootfsToSandboxCustomExtensions(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-rootfs-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-rootfs", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-rootfs",
		Namespace: "sandbox",
		Rootfs:    "python:3.12-slim",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "python:3.12-slim", capturedInvokeOpt.CustomExtensions["rootfs"])
}

func TestCreateHandlerAcceptsImageAliasForRootfs(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-image-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-image", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-image",
		Namespace: "sandbox",
		Image:     "ubuntu:22.04",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "ubuntu:22.04", capturedInvokeOpt.CustomExtensions["rootfs"])
}

func TestCreateHandlerPassesPortForwardingsToNetworkCreateOption(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-ports-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-ports", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-ports",
		Namespace: "sandbox",
		Ports:     []string{"8080", "https:9090"},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(
		t,
		`{
			"portForwardings": [
				{"port": 8080, "protocol": "http", "routeKind": "public"},
				{"port": 9090, "protocol": "https", "routeKind": "public"}
			]
		}`,
		capturedInvokeOpt.CreateOpt["network"],
	)
}

func TestCreateHandlerRejectsInvalidPortForwarding(t *testing.T) {
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			t.Fatalf("createInstance should not be called for invalid port forwarding")
			return "", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-ports",
		Namespace: "sandbox",
		Ports:     []string{"sctp:8080"},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "port scheme must be http or https")
}

func TestCreateHandlerBuildsBuiltinDetachedSandboxRequest(t *testing.T) {
	oldSelectScheduler := selectSandboxSchedulerID
	selectSandboxSchedulerID = func(string) (string, error) {
		return "scheduler-contract-test", nil
	}
	defer func() {
		selectSandboxSchedulerID = oldSelectScheduler
	}()

	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-contract", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-f",
		Namespace: "sandbox",
		Tenant:    "contract-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(constant.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assertBuiltinDetachedSandboxRequest(t, capturedFuncMeta, capturedInvokeOpt, recorder)
}

func assertBuiltinDetachedSandboxRequest(
	t *testing.T,
	capturedFuncMeta api.FunctionMeta,
	capturedInvokeOpt api.InvokeOptions,
	recorder *httptest.ResponseRecorder,
) {
	t.Helper()
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.Equal(t, "yr.sandbox.sandbox", capturedFuncMeta.ModuleName)
	require.Equal(t, "SandboxInstance", capturedFuncMeta.ClassName)
	require.Equal(t, api.Python, capturedFuncMeta.Language)
	require.Equal(t, api.ActorApi, capturedFuncMeta.Api)
	require.NotNil(t, capturedFuncMeta.Name)
	require.NotNil(t, capturedFuncMeta.Namespace)
	require.Equal(t, "sandbox-f", *capturedFuncMeta.Name)
	require.Equal(t, "sandbox", *capturedFuncMeta.Namespace)

	require.Equal(t, "detached", capturedInvokeOpt.CustomExtensions["lifecycle"])
	require.Equal(t, sandboxConcurrency, capturedInvokeOpt.CustomExtensions["Concurrency"])
	require.Equal(t, "trace-create", capturedInvokeOpt.TraceID)
	require.Equal(
		t,
		"00-123e4567e89b12d3a456426614174000-0123456789abcdef-01",
		capturedInvokeOpt.CustomExtensions["traceparent"],
	)
	require.Equal(t, "trace-create", recorder.Header().Get(constant.HeaderTraceID))
	require.Equal(t, "contract-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
	require.Equal(t, defaultSandboxFunctionID, capturedInvokeOpt.CreateOpt[constant.FunctionKeyNote])
	_, hasStaticOwner := capturedInvokeOpt.CreateOpt["resource.owner"]
	require.False(t, hasStaticOwner)
	require.Equal(t, sandboxInstanceType, capturedInvokeOpt.CreateOpt[constant.InstanceTypeNote])
	require.Empty(t, capturedInvokeOpt.CreateOpt[constant.SchedulerIDNote])
}

func TestCreateV1HandlerDefaultsAndReturnsSandboxID(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "sandbox-v1", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Image:              "ubuntu:22.04",
		IdleTimeoutSeconds: 123,
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.NotNil(t, capturedFuncMeta.Name)
	require.NotEmpty(t, *capturedFuncMeta.Name)
	require.NotNil(t, capturedFuncMeta.Namespace)
	require.Equal(t, "default", *capturedFuncMeta.Namespace)
	require.Equal(t, "123", capturedInvokeOpt.CustomExtensions["idle_timeout"])
	require.JSONEq(
		t,
		`{"runtime":"runsc","type":"image","imageurl":"ubuntu:22.04"}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)

	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	var data map[string]string
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, "sandbox-v1", requireStringMapValue(t, data, "sandboxId"))
	require.Equal(t, "running", requireStringMapValue(t, data, "status"))
}

func TestCreateV1HandlerUsesRRTForKataIsolationRuntime(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "sandbox-kata", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"runtime":"kata",
		"image":"ubuntu:22.04"
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.JSONEq(
		t,
		`{"runtime":"kata","type":"image","imageurl":"ubuntu:22.04"}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)
}

func TestCreateV1HandlerPassesS3RootfsAndRequiredNodeAffinity(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-s3", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"runtime":"runsc",
		"scheduleAffinities":[{
			"kind":0,
			"affinity":2,
			"labelOps":[{
				"type":0,
				"labelKey":"NODE_ID",
				"labelValues":["node-a"]
			}]
		}],
		"rootfs":{
			"type":"s3",
			"storageInfo":{
				"endpoint":"https://s3.example",
				"bucket":"rootfs",
				"object":"images/base"
			}
		}
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(
		t,
		`{
			"runtime":"runsc",
			"type":"s3",
			"storageInfo":{
				"endpoint":"https://s3.example",
				"bucket":"rootfs",
				"object":"images/base"
			}
		}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)
	require.Equal(t, []api.Affinity{{
		Kind:     api.AffinityKindResource,
		Affinity: api.RequiredAffinity,
		LabelOps: []api.LabelOperator{{
			Type:        api.LabelOpIn,
			LabelKey:    "NODE_ID",
			LabelValues: []string{"node-a"},
		}},
	}}, capturedInvokeOpt.ScheduleAffinities)
}

func TestCreateV1HandlerRejectsInvalidScheduleAffinity(t *testing.T) {
	createCalled := false
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			createCalled = true
			return "sandbox-invalid-affinity", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"runtime":"runsc",
		"image":"ubuntu:22.04",
		"scheduleAffinities":[{
			"kind":9,
			"affinity":2,
			"labelOps":[{
				"type":0,
				"labelKey":"NODE_ID",
				"labelValues":["node-a"]
			}]
		}]
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.False(t, createCalled)
	require.Contains(t, recorder.Body.String(), "scheduleAffinities[0].kind")
}

func TestCreateV1HandlerSSEUsesRequestedTimeoutAndReturnsFinal(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-sse", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Name:                 "sandbox-sse",
		Namespace:            "default",
		Image:                "ubuntu:22.04",
		CreateTimeoutSeconds: customCreateTimeoutSeconds,
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")
	ctx.Request.Header.Set(constant.HeaderRequestID, "create-request-sse")

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "text/event-stream", recorder.Header().Get("Content-Type"))
	require.Contains(t, recorder.Body.String(), "event: accepted")
	require.Contains(t, recorder.Body.String(), `"status":"creating"`)
	require.Contains(t, recorder.Body.String(), `"requestId":"create-request-sse"`)
	require.Contains(t, recorder.Body.String(), "event: final")
	require.Contains(t, recorder.Body.String(), `"sandboxId":"sandbox-sse"`)
	require.Contains(t, recorder.Body.String(), `"status":"running"`)
	require.Equal(t, customCreateTimeoutSeconds, capturedInvokeOpt.Timeout)
	expectedScheduleMs := int64(customCreateTimeoutSeconds-sandboxScheduleBufferSeconds) * millisecondsPerSecond
	require.Equal(t, expectedScheduleMs, capturedInvokeOpt.ScheduleTimeoutMs)
	require.Equal(t, strconv.Itoa(customCreateTimeoutSeconds), capturedInvokeOpt.CreateOpt["call_timeout"])
}

func TestCreateV1HandlerRejectsConcurrentExplicitNameWithDifferentRequestID(t *testing.T) {
	var createCalls atomic.Int32
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			if createCalls.Add(1) == 1 {
				close(createStarted)
			}
			<-releaseCreate
			return "sandbox-singleflight", nil
		},
	})

	type response struct {
		recorder *httptest.ResponseRecorder
	}
	runCreate := func(requestID, tenant string, responses chan<- response) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		body := []byte(fmt.Sprintf(`{
			"name":"sandbox-singleflight",
			"namespace":"default",
			"tenant":%q
		}`, tenant))
		ctx.Request = httptest.NewRequest(
			http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
		)
		ctx.Request.Header.Set(constant.HeaderRequestID, requestID)
		CreateV1Handler(ctx)
		responses <- response{recorder: recorder}
	}

	responses := make(chan response, 2)
	var requests sync.WaitGroup
	requests.Add(2)
	go func() {
		defer requests.Done()
		runCreate("create-leader", "tenant-a", responses)
	}()
	<-createStarted
	go func() {
		defer requests.Done()
		runCreate("create-duplicate", "tenant-b", responses)
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseCreate)
	requests.Wait()
	close(responses)

	require.Equal(t, int32(1), createCalls.Load())
	statuses := map[int]*httptest.ResponseRecorder{}
	for result := range responses {
		statuses[result.recorder.Code] = result.recorder
	}
	require.Contains(t, statuses, http.StatusOK)
	require.Contains(t, statuses, http.StatusConflict)
	requireCreateV1SandboxID(t, statuses[http.StatusOK], "sandbox-singleflight")
	require.Contains(
		t,
		statuses[http.StatusConflict].Body.String(),
		"sandbox 'default/sandbox-singleflight' is already being created by request create-leader",
	)
	require.Contains(t, statuses[http.StatusConflict].Body.String(), "request create-duplicate")
}

func TestCreateV1HandlerReplaysCompletedCreateByRequestID(t *testing.T) {
	var createCalls atomic.Int32
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			createCalls.Add(1)
			return "sandbox-request-replay", nil
		},
	})

	body := []byte(`{
		"name":"sandbox-request-replay",
		"namespace":"default",
		"tenant":"tenant-replay"
	}`)
	runCreate := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(
			http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
		)
		ctx.Request.Header.Set(constant.HeaderRequestID, "create-request-replay")
		CreateV1Handler(ctx)
		return recorder
	}

	first := runCreate()
	second := runCreate()

	require.Equal(t, int32(1), createCalls.Load())
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	requireCreateV1SandboxID(t, first, "sandbox-request-replay")
	requireCreateV1SandboxID(t, second, "sandbox-request-replay")
}

func TestCreateV1HandlerRejectsRequestIDBodyConflict(t *testing.T) {
	var createCalls atomic.Int32
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			createCalls.Add(1)
			return "sandbox-request-conflict", nil
		},
	})

	runCreate := func(name string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		body, err := json.Marshal(CreateV1Request{
			Name:      name,
			Namespace: "default",
			Tenant:    "tenant-conflict",
		})
		require.NoError(t, err)
		ctx.Request = httptest.NewRequest(
			http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
		)
		ctx.Request.Header.Set(constant.HeaderRequestID, "create-request-conflict")
		CreateV1Handler(ctx)
		return recorder
	}

	first := runCreate("sandbox-request-conflict-a")
	second := runCreate("sandbox-request-conflict-b")

	require.Equal(t, int32(1), createCalls.Load())
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusConflict, second.Code)
	require.Contains(
		t,
		second.Body.String(),
		"requestId 'create-request-conflict' has already been used to create a sandbox with different parameters",
	)
}

func TestCreateV1HandlerRejectsExplicitNameAlreadyInSandboxRouterCache(t *testing.T) {
	const (
		tenantID  = "tenant-existing"
		namespace = "default"
		name      = "sandbox-existing"
		instance  = namespace + "-" + name
	)
	execendpoint.Default().PutSummary(execendpoint.Summary{
		InstanceID: instance,
		TenantID:   "another-tenant",
		StatusCode: 3,
	})
	defer execendpoint.Default().Delete(instance)

	var createCalls atomic.Int32
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			createCalls.Add(1)
			return instance, nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Name:      name,
		Namespace: namespace,
		Tenant:    tenantID,
	})
	require.NoError(t, err)
	ctx.Request = httptest.NewRequest(
		http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
	)
	ctx.Request.Header.Set(constant.HeaderRequestID, "create-existing")

	CreateV1Handler(ctx)

	require.Equal(t, int32(0), createCalls.Load())
	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Contains(
		t,
		recorder.Body.String(),
		"sandbox 'default/sandbox-existing' already exists",
	)
	require.Contains(t, recorder.Body.String(), "sandboxId=default-sandbox-existing")
	require.Contains(t, recorder.Body.String(), "requestId=create-existing")
}

func TestCreateV1HandlerReplaysUnnamedCreateByRequestID(t *testing.T) {
	var createCalls atomic.Int32
	var createdName string
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			createCalls.Add(1)
			require.NotNil(t, funcMeta.Name)
			createdName = *funcMeta.Name
			return "sandbox-unnamed-replay", nil
		},
	})

	runCreate := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(
			http.MethodPost,
			"/api/sandbox/v1/sandboxes",
			bytes.NewReader([]byte(`{"namespace":"default","tenant":"tenant-unnamed"}`)),
		)
		ctx.Request.Header.Set(constant.HeaderRequestID, "create-request-unnamed")
		CreateV1Handler(ctx)
		return recorder
	}

	first := runCreate()
	second := runCreate()

	require.Equal(t, int32(1), createCalls.Load())
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Contains(t, createdName, "sandbox-")
	requireCreateV1SandboxID(t, first, "sandbox-unnamed-replay")
	requireCreateV1SandboxID(t, second, "sandbox-unnamed-replay")
}

func TestGeneratedSandboxNamesUseIndependentUUIDs(t *testing.T) {
	first := newSandboxName()
	second := newSandboxName()

	require.NotEqual(t, first, second)
	require.Regexp(
		t,
		`^sandbox-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
		first,
	)
}

func requireCreateV1SandboxID(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	expected string,
) {
	t.Helper()
	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, expected, data["sandboxId"])
}

func TestSandboxCreateReplayStoreExpiresCompletedResults(t *testing.T) {
	now := time.Unix(100, 0)
	store := newSandboxCreateReplayStore(time.Second, 10)
	store.now = func() time.Time { return now }
	digest := [32]byte{1}
	createCalls := 0
	create := func() (sandboxCreateResult, error) {
		createCalls++
		return sandboxCreateResult{
			instanceID: fmt.Sprintf("sandbox-%d", createCalls),
			status:     sandboxCreateStatusRunning,
		}, nil
	}

	first, firstErr, firstReuse := store.do("tenant\x00request", "request", digest, create)
	second, secondErr, secondReuse := store.do("tenant\x00request", "request", digest, create)
	now = now.Add(2 * time.Second)
	third, thirdErr, thirdReuse := store.do("tenant\x00request", "request", digest, create)

	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	require.NoError(t, thirdErr)
	require.Equal(t, sandboxCreateReuseNone, firstReuse)
	require.Equal(t, sandboxCreateReuseCompleted, secondReuse)
	require.Equal(t, sandboxCreateReuseNone, thirdReuse)
	require.Equal(t, "sandbox-1", first.instanceID)
	require.Equal(t, "sandbox-1", second.instanceID)
	require.Equal(t, "sandbox-2", third.instanceID)
	require.Equal(t, 2, createCalls)
}

func TestSandboxCreateReplayStoreReplaysCompletedError(t *testing.T) {
	store := newSandboxCreateReplayStore(time.Minute, 10)
	digest := [32]byte{2}
	createCalls := 0
	create := func() (sandboxCreateResult, error) {
		createCalls++
		return sandboxCreateResult{
			instanceID: "sandbox-error",
			status:     sandboxCreateStatusFailed,
		}, fmt.Errorf("runtime outcome unknown")
	}

	first, firstErr, firstReuse := store.do("tenant\x00request", "request", digest, create)
	second, secondErr, secondReuse := store.do("tenant\x00request", "request", digest, create)

	require.EqualError(t, firstErr, "runtime outcome unknown")
	require.EqualError(t, secondErr, "runtime outcome unknown")
	require.Equal(t, sandboxCreateReuseNone, firstReuse)
	require.Equal(t, sandboxCreateReuseCompleted, secondReuse)
	require.Equal(t, first, second)
	require.Equal(t, 1, createCalls)
}

var timeoutTestCases = []sandboxTimeoutTestCase{
	{
		name:         "default create derives schedule",
		wantCreate:   60,
		wantSchedule: 30,
	},
	{
		name:          "create derives schedule",
		createTimeout: 120,
		wantCreate:    120,
		wantSchedule:  90,
	},
	{
		name:            "schedule derives create",
		scheduleTimeout: 90,
		wantCreate:      120,
		wantSchedule:    90,
	},
	{
		name:             "create must exceed buffer",
		createTimeout:    30,
		wantErrorMessage: "createTimeoutSeconds must be greater than 30",
	},
	{
		name:             "create must be positive",
		createTimeout:    -1,
		wantErrorMessage: "createTimeoutSeconds must be a positive integer",
	},
	{
		name:             "schedule must be positive",
		scheduleTimeout:  -1,
		wantErrorMessage: "scheduleTimeoutSeconds must be a positive integer",
	},
	{
		name:             "schedule must not exceed create",
		createTimeout:    60,
		scheduleTimeout:  70,
		wantErrorMessage: "scheduleTimeoutSeconds must be less than or equal to createTimeoutSeconds",
	},
	{
		name:             "explicit timeouts must reserve buffer",
		createTimeout:    60,
		scheduleTimeout:  45,
		wantErrorMessage: "createTimeoutSeconds - scheduleTimeoutSeconds must be at least 30",
	},
	{
		name:            "explicit timeouts preserve caller budgets",
		createTimeout:   120,
		scheduleTimeout: 80,
		wantCreate:      120,
		wantSchedule:    80,
	},
}

func TestResolveSandboxCreateTimeouts(t *testing.T) {
	t.Setenv("YR_SANDBOX_CREATE_TIMEOUT", "")
	for _, tt := range timeoutTestCases {
		t.Run(tt.name, func(t *testing.T) {
			createTimeout, scheduleTimeout, err := resolveSandboxCreateTimeouts(
				tt.createTimeout, tt.scheduleTimeout,
			)
			if tt.wantErrorMessage != "" {
				require.EqualError(t, err, tt.wantErrorMessage)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantCreate, createTimeout)
			require.Equal(t, tt.wantSchedule, scheduleTimeout)
		})
	}
}

func TestCreateV1HandlerSSEDoesNotReportUnconfirmedTimeoutAsRunning(t *testing.T) {
	oldWaitForSandboxInstanceRunning := waitForSandboxInstanceRunning
	waitForSandboxInstanceRunning = func(instanceID, functionID, resourceSpecNote string) bool {
		return false
	}
	defer func() { waitForSandboxInstanceRunning = oldWaitForSandboxInstanceRunning }()

	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			return "sandbox-timeout", api.ErrorInfo{Code: 3002, Err: fmt.Errorf("create instance timeout")}
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{Name: "sandbox-timeout", Namespace: "default"})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")
	ctx.Request.Header.Set(constant.HeaderRequestID, "create-request-timeout")

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "event: final")
	require.Contains(t, recorder.Body.String(), `"sandboxId":"sandbox-timeout"`)
	require.Contains(t, recorder.Body.String(), `"status":"timeout"`)
	require.Contains(t, recorder.Body.String(), `"errorCode":3002`)
	require.Contains(t, recorder.Body.String(), `"requestId":"create-request-timeout"`)
	require.NotContains(t, recorder.Body.String(), `"status":"running"`)
}

func TestCreateV1HandlerFrontendOwnsTunnelSetup(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "default/sandbox_demo.1", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Image: "ubuntu:22.04",
		Env: map[string]string{
			"RRT_HTTP_PORT":      "19000",
			"RRT_TUNNEL_WS_PORT": "19001",
			"USER_ENV":           "ok",
		},
		Tunnel: TunnelSpec{Enabled: true},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assertTunnelCreateOptions(t, capturedInvokeOpt)
	assertTunnelResponse(t, recorder)
}

func assertTunnelCreateOptions(t *testing.T, capturedInvokeOpt api.InvokeOptions) {
	t.Helper()
	require.JSONEq(
		t,
		`{
			"portForwardings":[
				{"port":50090,"protocol":"http","routeKind":"direct"},
				{"port":8765,"protocol":"http","routeKind":"tunnel"},
				{"port":8766,"protocol":"http","routeKind":"direct"}
			]
		}`,
		capturedInvokeOpt.CreateOpt["network"],
	)
	require.JSONEq(
		t,
		`{"RRT_HTTP_PORT":"50090","RRT_TUNNEL_WS_PORT":"8765","RRT_TUNNEL_HTTP_PORT":"8766","USER_ENV":"ok"}`,
		capturedInvokeOpt.CreateOpt[constant.DelegateEnvVar],
	)
}

func assertTunnelResponse(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	tunnelValue := requireInterfaceMapValue(t, data, "tunnel")
	tunnel, ok := tunnelValue.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "/tunnel/default-sandbox-demo-1", tunnel["url"])
	require.Equal(t, "/tunnel/default-sandbox-demo-1", tunnel["path"])
	require.Equal(t, "http://127.0.0.1:8766", tunnel["proxyUrl"])
}

func TestCreateV1HandlerForwardsIsolationRuntimeWithoutOwningRegistry(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	var capturedFuncMeta api.FunctionMeta
	util.SetAPIClientLibruntime(&runtimeStub{
		createInstance: func(funcMeta api.FunctionMeta, args []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "sandbox-next-runtime", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Runtime: "gvisor-next",
		Image:   "ubuntu:22.04",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.JSONEq(
		t,
		`{"runtime":"gvisor-next","type":"image","imageurl":"ubuntu:22.04"}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)
}

func TestNormalizeJSONValuePreservesFractionalAndConvertsIntegers(t *testing.T) {
	normalized := normalizeJSONValue(map[string]interface{}{
		"pid":     float64(123),
		"timeout": float64(0.5),
		"nested":  []interface{}{float64(7)},
	})
	got, ok := normalized.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, int64(123), got["pid"])
	require.Equal(t, float64(0.5), got["timeout"])
	nested, ok := got["nested"].([]interface{})
	require.True(t, ok)
	require.Equal(t, int64(7), nested[0])
}

func TestInvokeV1HandlerRoutesEnvelopeToRRTSandboxInvoke(t *testing.T) {
	capture := setupInvokeV1RuntimeStub(t)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-123"}}
	body := []byte(`{"action":"process.exec","args":{"cmd":"echo hi","cwd":"/tmp"}}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/sandbox-123/invoke",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	ctx.Request.Header.Set(constant.HeaderTraceID, "trace-invoke")
	ctx.Request.Header.Set(constant.HeaderTraceParent, "00-abcdefabcdefabcdefabcdefabcdefab-0123456789abcdef-01")

	InvokeV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "sandbox-123", capture.instanceID)
	require.Equal(t, "sandbox_invoke", capture.funcMeta.FuncName)
	require.Equal(t, api.ActorApi, capture.funcMeta.Api)
	require.Equal(t, "trace-invoke", capture.invokeOpt.TraceID)
	require.Equal(
		t,
		"00-abcdefabcdefabcdefabcdefabcdefab-0123456789abcdef-01",
		capture.invokeOpt.CustomExtensions["traceparent"],
	)
	require.Equal(t, "trace-invoke", recorder.Header().Get(constant.HeaderTraceID))
	require.Equal(t, sandboxCreateTimeoutSeconds, capture.invokeOpt.Timeout)
	require.True(t, capture.invokeOpt.BypassDataSystem)
	require.Len(t, capture.args, invokeEnvelopeArgCount)
	decodedArgs := decodeInvokeArgs(t, capture.args)
	require.Equal(t, "process.exec", decodedArgs["action"])
	require.Equal(t, map[string]interface{}{
		"cmd": "echo hi",
		"cwd": "/tmp",
	}, decodedArgs["args"])

	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.JSONEq(t, `{"ok":true}`, string(resp.Data))
}

type invokeV1Capture struct {
	funcMeta   api.FunctionMeta
	instanceID string
	args       []api.Arg
	invokeOpt  api.InvokeOptions
}

func setupInvokeV1RuntimeStub(t *testing.T) *invokeV1Capture {
	t.Helper()
	capture := &invokeV1Capture{}
	util.SetAPIClientLibruntime(&runtimeStub{
		invokeInstance: func(
			funcMeta api.FunctionMeta,
			instanceID string,
			args []api.Arg,
			invokeOpt api.InvokeOptions,
		) (string, error) {
			capture.funcMeta = funcMeta
			capture.instanceID = instanceID
			capture.args = append([]api.Arg(nil), args...)
			capture.invokeOpt = invokeOpt
			return "object-1", nil
		},
		getAsync: func(objectID string, cb api.GetAsyncCallback) {
			require.Equal(t, "object-1", objectID)
			result, err := encodeMsgpack(map[string]interface{}{"ok": true})
			require.NoError(t, err)
			cb(append(make([]byte, constant.LibruntimeHeaderSize), result...), nil)
		},
	})
	return capture
}

func decodePackedArg(data []byte) (interface{}, error) {
	if len(data) < constant.LibruntimeHeaderSize {
		return nil, fmt.Errorf("arg too short: %d", len(data))
	}
	var out interface{}
	dec := codec.NewDecoderBytes(data[constant.LibruntimeHeaderSize:], &msgpackHandle)
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return normalizeMsgpack(out), nil
}

func decodeInvokeArgs(t *testing.T, args []api.Arg) map[string]interface{} {
	t.Helper()
	decodedArgs := make(map[string]interface{}, len(args)/packedArgPairSize)
	for i := 0; i+1 < len(args); i += packedArgPairSize {
		key, err := decodePackedArg(args[i].Data)
		require.NoError(t, err)
		value, err := decodePackedArg(args[i+1].Data)
		require.NoError(t, err)
		decodedArgs[fmt.Sprint(key)] = value
	}
	return decodedArgs
}

func requireStringMapValue(t *testing.T, data map[string]string, key string) string {
	t.Helper()
	value, ok := data[key]
	require.True(t, ok)
	return value
}

func requireInterfaceMapValue(t *testing.T, data map[string]interface{}, key string) interface{} {
	t.Helper()
	value, ok := data[key]
	require.True(t, ok)
	return value
}

func TestInvokeV1HandlerRejectsMissingAction(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-123"}}
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/sandbox-123/invoke",
		bytes.NewReader([]byte(`{"args":{}}`)),
	)
	require.NoError(t, err)

	InvokeV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "action is required")
}
func TestDeleteHandlerDeletesSandboxInstance(t *testing.T) {
	var (
		capturedInstanceID string
		capturedSignal     int
		capturedPayload    []byte
	)
	util.SetAPIClientLibruntime(&runtimeStub{
		kill: func(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
			capturedInstanceID = instanceID
			capturedSignal = signal
			capturedPayload = append([]byte(nil), payload...)
			require.Equal(t, api.InvokeOptions{}, invokeOpt)
			return nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: "sandbox-delete-ok"}}
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/sandbox-delete-ok", nil)
	require.NoError(t, err)
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "sandbox-delete-ok", capturedInstanceID)
	require.Equal(t, constant.KillSignalVal, capturedSignal)
	require.Equal(t, []byte("sandbox deleted"), capturedPayload)

	var resp job.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)

	var data map[string]string
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, "deleted", requireStringMapValue(t, data, "status"))
}

func TestDeleteHandlerReturns500WhenKillFails(t *testing.T) {
	util.SetAPIClientLibruntime(&runtimeStub{
		kill: func(instanceID string, signal int, payload []byte, invokeOpt api.InvokeOptions) error {
			return fmt.Errorf("kill failed")
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: "sandbox-delete-fail"}}
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/sandbox-delete-fail", nil)
	require.NoError(t, err)
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "failed to delete sandbox")
}

func setupDeleteTenantSummary(t *testing.T) string {
	t.Helper()
	targetInstance := "sandbox-delete-rbac-target"
	execendpoint.Default().PutSummary(execendpoint.Summary{
		InstanceID: targetInstance,
		TenantID:   "tenant-owner",
	})
	t.Cleanup(func() { execendpoint.Default().Delete(targetInstance) })
	return targetInstance
}

func deleteTestContext(t *testing.T, targetInstance, sub, role string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	originalEnableAuth := config.GetConfig().IamConfig.EnableFuncTokenAuth
	config.GetConfig().IamConfig.EnableFuncTokenAuth = true
	t.Cleanup(func() {
		config.GetConfig().IamConfig.EnableFuncTokenAuth = originalEnableAuth
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: targetInstance}}
	ctx.Set("jwt_sub", sub)
	ctx.Set("jwt_role", role)
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/"+targetInstance, nil)
	require.NoError(t, err)
	ctx.Request = req
	return ctx, recorder
}

func TestDeleteHandlerAllowsPlaceholderTokenWhenAuthDisabled(t *testing.T) {
	originalEnableAuth := config.GetConfig().IamConfig.EnableFuncTokenAuth
	config.GetConfig().IamConfig.EnableFuncTokenAuth = false
	t.Cleanup(func() {
		config.GetConfig().IamConfig.EnableFuncTokenAuth = originalEnableAuth
	})

	targetInstance := "sandbox-delete-auth-disabled"
	killCalled := false
	util.SetAPIClientLibruntime(&runtimeStub{kill: func(instanceID string, _ int, _ []byte, _ api.InvokeOptions) error {
		killCalled = true
		require.Equal(t, targetInstance, instanceID)
		return nil
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: targetInstance}}
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/"+targetInstance, nil)
	require.NoError(t, err)
	req.Header.Set(jwtauth.HeaderXAuth, "ci")
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, killCalled)
}

func TestDeleteHandlerRejectsCrossTenant(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	util.SetAPIClientLibruntime(&runtimeStub{kill: func(string, int, []byte, api.InvokeOptions) error {
		killCalled = true
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "tenant-other", jwtauth.RoleDeveloper)
	DeleteHandler(ctx)
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.False(t, killCalled)
}

func TestDeleteHandlerAllowsSameTenant(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	util.SetAPIClientLibruntime(&runtimeStub{kill: func(instanceID string, _ int, _ []byte, _ api.InvokeOptions) error {
		killCalled = true
		require.Equal(t, targetInstance, instanceID)
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "tenant-owner", jwtauth.RoleDeveloper)
	DeleteHandler(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, killCalled)
}

func TestDeleteHandlerEnforcesHeaderWithoutMiddleware(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	util.SetAPIClientLibruntime(&runtimeStub{kill: func(string, int, []byte, api.InvokeOptions) error {
		killCalled = true
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "", "")
	ctx.Request.Header.Set(jwtauth.HeaderXAuth, testJWT("tenant-other", jwtauth.RoleDeveloper))
	DeleteHandler(ctx)
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.False(t, killCalled)
}

func TestDeleteHandlerAllowsSystemTenantDeveloper(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	util.SetAPIClientLibruntime(&runtimeStub{kill: func(instanceID string, _ int, _ []byte, _ api.InvokeOptions) error {
		killCalled = true
		require.Equal(t, targetInstance, instanceID)
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "0", jwtauth.RoleDeveloper)
	DeleteHandler(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, killCalled)
}

func testJWT(sub, role string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(
		`{"sub":%q,"role":%q,"exp":%d}`,
		sub, role, time.Now().Add(time.Hour).Unix(),
	)))
	return header + "." + payload + ".signature"
}
