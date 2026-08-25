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

// Package agent provides HTTP handlers for agent instance lifecycle (create/kill)
// and read-only status query (get/list).
package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/constants"
	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/runtime"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/resspeckey"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/urnutils"
	"frontend/pkg/frontend/api/app"
	"frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/functionmeta"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/route"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/wsproxy"
)

// agentUserPlaceholder is the workspace mount target placeholder. It is replaced with
// rootfs.user (registered: from funcSpecMap; inline: from req.Rootfs.User) in
// applyAgentFuncMeta / applyAgentInlineMeta. When rootfs.user is empty, the placeholder is
// replaced with agentDefaultWorkspaceTarget so the mount target is a real path, not a literal.
const (
	agentUserPlaceholder        = "__AGENT_USER__"
	agentDefaultWorkspaceTarget = "/home/agentos"
	agentBootstrapCmdEnv        = "YR_RUNTIME_BOOTSTRAP_CMD"
	agentSandboxTypeSupervisor  = "supervisor"
	agentExecutorInitEntry      = "yr.agentexecutor.handler.initialize"
	agentExecutorCallEntry      = "yr.agentexecutor.handler.handle"
	agentExecutorPreStopEntry   = "yr.agentexecutor.handler.pre_stop"
	// Keep this container-internal tunnel target aligned with DEFAULT_EXECUTOR_PORT
	// in yuanrong-agentruntime's agentexecutor runtime. It must not be added to
	// rootfs portForwardings: the owner proxy reaches it through containerIP:port.
	agentExecutorHTTPPort = 18093
)

// agentExecutorFormat is the system executor function funcKey pattern. agent reuses the
// faas system executor function (loaded into function_proxy's funcMetaMap_ at startup from
// executor-meta/*_meta.json) as the CreateInstance FuncID so proxy's GetFuncMeta hits and
// does not return "invalid function (1015)". agent's real config (imageurl/user/ports/
// sandboxType) is passed via createOptions — proxy sinks them as-is, docker executor reads.
const agentExecutorFormat = "default/0-system-faasExecutor%s/$latest"

// createParams mirrors functionscaler.CreateParams (go/pkg/functionscaler/instancepool/
// instancepool.go:141) for the registered-mode local-codePath non-HTTP/non-CustomContainer
// path. JSON tags must align so the executor process can deserialize the same payload.
type createParams struct {
	InstanceLabel     string            `json:"instanceLabel,omitempty"`
	EventCreateParams eventCreateParams `json:",inline"`
}

type eventCreateParams struct {
	UserInitEntry string `json:"userInitEntry,omitempty"`
	UserCallEntry string `json:"userCallEntry,omitempty"`
}

// agentCreateConfig is the shared carrier for the agent create call's resolved inputs. It is
// built once in CreateHandler and consumed by both buildAgentInvokeOptions (rootfs/code/opts
// sinking) and buildAgentCreateArgs (libruntime CreateInstance args). runtimeSpec carries the
// inline req.RuntimeSpec pointer (nil in registered mode); resKey is filled in after
// buildAgentInvokeOptions since it derives from the resolved invokeOpts.Cpu/Memory.
type agentCreateConfig struct {
	funcKey          string
	inline           bool
	spec             *types.FuncSpec
	platformExecutor bool
	preStopTimeout   int
	runtime          string
	runtimeSpec      *RuntimeSpec
}

type agentExecutorHTTPRequest struct {
	method        string
	path          string
	query         url.Values
	body          io.Reader
	contentLength int64
	headers       http.Header
}

const (
	defaultAgentCPU              = 1000
	defaultAgentMemory           = 2048
	agentCreateGrpcDeadlineSeconds = 70
	agentCreateBusinessTimeoutSeconds = 70
	agentInitTimeoutSeconds      = 305
	agentGracefulShutdownSeconds = 15
	agentPreStopTimeoutSeconds   = 10
	agentShutdownReserveSeconds  = 5
	defaultRecoverRetryTimes     = 3
	agentDirectoryQuotaMB        = 512
	agentInstanceType            = "reserved"
	agentDelegateDirectory       = "/tmp"
	agentConcurrency             = "1"
	agentKillInstanceSignal      = constant.KillSignalVal
	agentRunningPollTimeout      = 5 * time.Second
	agentRunningPollInterval     = 200 * time.Millisecond
	agentCreateTimeoutCode       = 3002
	agentStorageResourceName     = "storage"
	agentStorageBytesPerMiB      = 1024 * 1024
	sshEnableEnv                 = "YR_FRONTEND_SSH_ENABLE"
	sshPublicKeyDirectoryEnv     = "YR_SSH_BACKEND_PUBLIC_KEY_DIR"
	sshContainerMountDirectory   = "/run/openyuanrong/ssh"
)

// getAgentExecutorFuncKey maps the requested runtime to the faas system executor function
// funcKey (mirrors functionscaler.instancepool.getExecutorFuncKey). Agent reuses the faas
// executor function so function_proxy's funcMetaMap_ has it (loaded at startup) — avoids
// "invalid function (1015)" since agent does not register its user funcKey into proxy's
// /yr/functions watch.
func getAgentExecutorFuncKey(runtime string) string {
	r := strings.ToLower(runtime)
	switch {
	case strings.Contains(r, "python3.6"):
		return fmt.Sprintf(agentExecutorFormat, "Python3.6")
	case strings.Contains(r, "python3.7"):
		return fmt.Sprintf(agentExecutorFormat, "Python3.7")
	case strings.Contains(r, "python3.8"):
		return fmt.Sprintf(agentExecutorFormat, "Python3.8")
	case strings.Contains(r, "python3.9"):
		return fmt.Sprintf(agentExecutorFormat, "Python3.9")
	case strings.Contains(r, "python3.10"):
		return fmt.Sprintf(agentExecutorFormat, "Python3.10")
	case strings.Contains(r, "python3.11"):
		return fmt.Sprintf(agentExecutorFormat, "Python3.11")
	case strings.Contains(r, "go"), strings.Contains(r, "http"),
		strings.Contains(r, "custom image"):
		return fmt.Sprintf(agentExecutorFormat, "Go1.x")
	case strings.Contains(r, "java8"):
		return fmt.Sprintf(agentExecutorFormat, "Java8")
	case strings.Contains(r, "java11"):
		return fmt.Sprintf(agentExecutorFormat, "Java11")
	case strings.Contains(r, "java17"):
		return fmt.Sprintf(agentExecutorFormat, "Java17")
	case strings.Contains(r, "java21"):
		return fmt.Sprintf(agentExecutorFormat, "Java21")
	case strings.Contains(r, constant.PosixCustomRuntimeType):
		return fmt.Sprintf(agentExecutorFormat, "PosixCustom")
	default:
		return fmt.Sprintf(agentExecutorFormat, "PosixCustom")
	}
}

var selectAgentSchedulerID = func(funcKey string) (string, error) {
	schedulerInfo, err := schedulerproxy.Proxy.Get(funcKey, log.GetLogger())
	if err != nil {
		return "", err
	}
	if schedulerInfo == nil || schedulerInfo.InstanceInfo == nil || schedulerInfo.InstanceInfo.InstanceID == "" {
		return "", fmt.Errorf("failed to get valid scheduler for funcKey %s", funcKey)
	}
	return schedulerInfo.InstanceInfo.InstanceID, nil
}

var waitForAgentInstanceRunning = func(instanceID, functionID, resourceSpecNote string) bool {
	deadline := time.Now().Add(agentRunningPollTimeout)
	for time.Now().Before(deadline) {
		if isAgentInstanceRunning(instanceID, functionID, resourceSpecNote) {
			return true
		}
		time.Sleep(agentRunningPollInterval)
	}
	return isAgentInstanceRunning(instanceID, functionID, resourceSpecNote)
}

// CreateAgentRequest holds the parameters for POST /api/agent.
type CreateAgentRequest struct {
	Namespace   string            `json:"namespace" binding:"required"`
	Name        string            `json:"name" binding:"required"`
	Urn         string            `json:"urn,omitempty"`
	RuntimeSpec *RuntimeSpec      `json:"runtime_spec,omitempty"`
	Workspace   string            `json:"workspace,omitempty"`
	EnvVars     map[string]string `json:"env_vars,omitempty"`
	Mounts      []Mount           `json:"mounts,omitempty"`
}

// RuntimeSpec carries inline container config (inline mode).
// CodePath/Handler/ExtendedHandler supply the user code entry for inline+supervisor
// (where there is no funcSpec): CodePath→DELEGATE_DOWNLOAD (storage_type=local), the
// handler symbols→CodePaths. See design §4.5.1, §6 C4/C8.
type RuntimeSpec struct {
	Runtime         string          `json:"runtime,omitempty"`
	SandboxType     string          `json:"sandbox_type,omitempty"`
	CodePath        string          `json:"codePath,omitempty"`
	Handler         string          `json:"handler,omitempty"`
	ExtendedHandler *ExtendedHandler `json:"extendedHandler,omitempty"`
	Rootfs          *RootfsSpec     `json:"rootfs,omitempty"`
	Cmds            [][]string      `json:"cmds,omitempty"`
	CPU             int             `json:"cpu,omitempty"`
	Memory          int             `json:"memory,omitempty"`
}

// ExtendedHandler carries the optional init/preStop entry symbols for inline mode,
// mirroring meta_service's extendedHandler (registered: ExtendedMetaData.Initializer/PreStop).
type ExtendedHandler struct {
	Initializer string `json:"initializer,omitempty"`
	PreStop     string `json:"pre_stop,omitempty"`
}

// RootfsSpec carries the inline container rootfs config. ImageURL is optional here: it is
// validated per sandbox type in buildAgentInvokeOptions (required unless the sandbox_type is
// supervisor, which runs without a container image).
type RootfsSpec struct {
	ImageURL string   `json:"imageurl,omitempty"`
	User     string   `json:"user,omitempty"`
	Ports    []string `json:"ports,omitempty"`
}

// Mount defines a custom bind mount.
type Mount struct {
	Source   string `json:"source" binding:"required"`
	Target   string `json:"target" binding:"required"`
	ReadOnly bool   `json:"readonly,omitempty"`
}

// CreateHandler handles POST /api/agent.
// Inline mode (Runtime/Rootfs set): container config from the request, bypasses meta_service.
// Registered mode (Urn set): config looked up from funcSpecMap. Inline takes precedence.
func CreateHandler(ctx *gin.Context) {
	var req CreateAgentRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("invalid request body: %v", err))
		return
	}

	inline := isInlineMode(req)
	if !inline && req.Urn == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			fmt.Errorf("either runtime_spec (inline) or urn (registered) is required"))
		return
	}

	funcKey, runtime, ok := resolveAgentFuncKeyAndRuntime(ctx, req, inline)
	if !ok {
		return
	}
	var spec *types.FuncSpec
	if !inline {
		if loaded, found := functionmeta.LoadFuncSpec(funcKey); found {
			spec = loaded
		}
	}
	// platformExecutor decides whether the instance runs the platform Executor's lifecycle
	// entries (yr.agentexecutor.handler.*) instead of a user-supplied handler. The two modes
	// are evaluated separately so that inline's spec==nil does not leak into the registered
	// branch and vice versa:
	//   - inline: fall back to the platform Executor ONLY when no user handler was supplied
	//     in runtime_spec (the bootstrap-cmds use case). A non-empty runtime_spec.handler
	//     means the caller wants its own code entry run (design §4.5.1 UC2).
	//   - registered: fall back to the platform Executor when the watched funcSpec is missing
	//     or its handler is empty.
	inlineUserHandler := inline && req.RuntimeSpec != nil && strings.TrimSpace(req.RuntimeSpec.Handler) != ""
	platformExecutor := (inline && !inlineUserHandler) ||
		(!inline && (spec == nil || strings.TrimSpace(spec.FuncMetaData.Handler) == ""))
	preStopTimeout := resolveAgentPreStopTimeout(spec, platformExecutor)
	if platformExecutor && !isSupportedAgentPythonRuntime(runtime) {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			fmt.Errorf("agent executor requires a supported Python runtime "+
				"(python3.9, python3.10, or python3.11), got %q", runtime))
		return
	}
	funcMeta := api.FunctionMeta{
		FuncID:    getAgentExecutorFuncKey(runtime),
		Language:  api.Python,
		Api:       api.FaaSApi,
		Namespace: &req.Namespace,
	}
	createConfig := agentCreateConfig{
		funcKey:          funcKey,
		inline:           inline,
		spec:             spec,
		platformExecutor: platformExecutor,
		preStopTimeout:   preStopTimeout,
		runtime:          runtime,
		runtimeSpec:      req.RuntimeSpec,
	}
	invokeOpts, err := buildAgentInvokeOptions(ctx, req, createConfig)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	resKey := resspeckey.ResourceSpecification{
		CPU:         int64(invokeOpts.Cpu),
		Memory:      int64(invokeOpts.Memory),
		InvokeLabel: "",
	}
	args := buildAgentCreateArgs(createConfig, resKey)
	createAgentInstance(ctx, req, funcMeta, invokeOpts, args)
}

func isSupportedAgentPythonRuntime(runtime string) bool {
	runtime = strings.ToLower(strings.TrimSpace(runtime))
	return strings.Contains(runtime, "python3.9") || strings.Contains(runtime, "python3.10") ||
		strings.Contains(runtime, "python3.11")
}

func resolveAgentPreStopTimeout(spec *types.FuncSpec, platformExecutor bool) int {
	if spec != nil && spec.ExtendedMetaData.PreStop.Timeout > 0 {
		return spec.ExtendedMetaData.PreStop.Timeout
	}
	if platformExecutor {
		return agentPreStopTimeoutSeconds
	}
	return 0
}

// resolveAgentFuncKeyAndRuntime resolves the user function key and runtime for the agent.
// Inline mode composes funcKey from the tenant header + req.Name and takes runtime from
// RuntimeSpec; registered mode parses req.Urn and loads runtime from the watched funcSpecMap.
// Returns ok=false after writing a bad-request response on an unparseable URN.
func resolveAgentFuncKeyAndRuntime(ctx *gin.Context, req CreateAgentRequest, inline bool,
) (funcKey, runtime string, ok bool) {
	if inline {
		tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
		if tenantID == "" {
			tenantID = "default"
		}
		funcKey = urnutils.CombineFunctionKey(tenantID, req.Name, urnutils.DefaultURNVersion)
		runtime = req.RuntimeSpec.Runtime
		return funcKey, runtime, true
	}
	funcUrn := &urnutils.FunctionURN{}
	if err := funcUrn.ParseFrom(req.Urn); err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("invalid urn: %v", err))
		return "", "", false
	}
	funcKey = urnutils.CombineFunctionKey(funcUrn.TenantID, funcUrn.FuncName, funcUrn.FuncVersion)
	if spec, loaded := functionmeta.LoadFuncSpec(funcKey); loaded && spec != nil {
		runtime = spec.FuncMetaData.Runtime
	}
	return funcKey, runtime, true
}

// isInlineMode reports whether req carries inline container config (runtime_spec present).
// imageurl is validated downstream per sandbox type, not here.
func isInlineMode(req CreateAgentRequest) bool {
	return req.RuntimeSpec != nil && req.RuntimeSpec.Runtime != ""
}

// validateRootfsImageURL enforces rootfs.imageurl per sandbox type. imageurl is required for
// container-based sandboxes (e.g. docker) and optional only when sandbox_type is supervisor,
// which runs without a container image. Inline mode reads imageurl/sandbox_type from the request;
// registered mode reads them from the watched funcSpecMap. A missing funcSpec cache entry is
// tolerated (mirrors applyAgentFuncMeta) — the create call downstream surfaces the failure.
func validateRootfsImageURL(req CreateAgentRequest, funcKey string, inline bool) error {
	var imageurl, sandboxType string
	if inline {
		if req.RuntimeSpec == nil {
			return nil
		}
		if req.RuntimeSpec.Rootfs != nil {
			imageurl = req.RuntimeSpec.Rootfs.ImageURL
		}
		sandboxType = req.RuntimeSpec.SandboxType
	} else {
		spec, ok := functionmeta.LoadFuncSpec(funcKey)
		if !ok || spec == nil {
			return nil
		}
		imageurl = spec.RootfsSpecMeta.ImageURL
		sandboxType = spec.SandboxType
	}
	if imageurl == "" && !strings.EqualFold(sandboxType, agentSandboxTypeSupervisor) {
		return fmt.Errorf("rootfs.imageurl is required for sandbox_type %q", sandboxType)
	}
	return nil
}

// buildAgentInvokeOptions builds the invoke options for agent create.
// inline=true: container config from req; inline=false: from funcSpecMap by funcKey.
func buildAgentInvokeOptions(ctx *gin.Context, req CreateAgentRequest,
	config agentCreateConfig) (api.InvokeOptions, error) {
	cpu, memory := defaultAgentCPU, defaultAgentMemory
	if config.inline && req.RuntimeSpec != nil {
		if req.RuntimeSpec.CPU > 0 {
			cpu = req.RuntimeSpec.CPU
		}
		if req.RuntimeSpec.Memory > 0 {
			memory = req.RuntimeSpec.Memory
		}
	}
	invokeOpts := api.InvokeOptions{
		Cpu:              cpu,
		Memory:           memory,
		Timeout:          agentCreateGrpcDeadlineSeconds,
		CreateOpt:        map[string]string{"create_error_policy": "last_failure_on_timeout"},
		CustomExtensions: map[string]string{"lifecycle": "detached", "Concurrency": agentConcurrency},
	}

	if err := validateRootfsImageURL(req, config.funcKey, config.inline); err != nil {
		return api.InvokeOptions{}, err
	}

	if err := applyAgentRootfsMounts(&invokeOpts, req); err != nil {
		return api.InvokeOptions{}, err
	}
	applyAgentDynamicEnv(&invokeOpts, req)
	if config.inline {
		applyAgentInlineMeta(&invokeOpts, req)
	} else {
		applyAgentFuncMeta(&invokeOpts, config.funcKey)
	}
	if config.platformExecutor {
		applyAgentExecutorCode(&invokeOpts)
	} else {
		applyAgentCodePaths(&invokeOpts, config.spec)
	}
	invokeOpts.RecoverRetryTimes = defaultRecoverRetryTimes
	applyAgentCreateOpts(&invokeOpts, ctx, req, config)
	return invokeOpts, nil
}

// applyAgentFuncMeta looks up the watched funcMeta by funcKey and transparently passes
// rootfs (imageurl/user/ports), sandboxType, and cpu/memory into createOptions (registered mode).
func applyAgentFuncMeta(invokeOpts *api.InvokeOptions, funcKey string) {
	spec, ok := functionmeta.LoadFuncSpec(funcKey)
	if !ok || spec == nil {
		log.GetLogger().Warnf("agent funcMeta not found in cache for funcKey %s, skip funcMeta sinking", funcKey)
		return
	}
	if spec.ResourceMetaData.CPU > 0 {
		invokeOpts.Cpu = int(spec.ResourceMetaData.CPU)
	}
	if spec.ResourceMetaData.Memory > 0 {
		invokeOpts.Memory = int(spec.ResourceMetaData.Memory)
	}
	if spec.SandboxType != "" {
		invokeOpts.CreateOpt["sandbox_type"] = spec.SandboxType
	}
	if spec.RootfsSpecMeta.User != "" {
		invokeOpts.CreateOpt["host_user"] = spec.RootfsSpecMeta.User
	}
	replaceAgentUserPlaceholder(invokeOpts, spec.RootfsSpecMeta.User)
	if spec.RootfsSpecMeta.ImageURL != "" {
		rootfsJSON, err := json.Marshal(map[string]interface{}{
			"type": "image", "imageurl": spec.RootfsSpecMeta.ImageURL, "mounts": []interface{}{},
		})
		if err != nil {
			log.GetLogger().Warnf("failed to marshal agent rootfs imageurl: %v", err)
		} else if existing, exists := invokeOpts.CreateOpt["rootfs"]; exists && existing != "" {
			invokeOpts.CreateOpt["rootfs"] = mergeRootfsJSON(existing, string(rootfsJSON))
		} else {
			invokeOpts.CreateOpt["rootfs"] = string(rootfsJSON)
		}
	}
	if len(spec.RootfsSpecMeta.Ports) > 0 {
		applyAgentPorts(invokeOpts, spec.RootfsSpecMeta.Ports)
	}
	mergeAgentStaticEnv(invokeOpts, spec)
	applyAgentCodePath(invokeOpts, spec)
}

// applyAgentExecutorCode configures only the lifecycle entries of the platform-owned Executor.
// The package is preinstalled in the image's Python environment, so it must not consume
// DELEGATE_DOWNLOAD. That option remains available for an optional user process code package.
func applyAgentExecutorCode(invokeOpts *api.InvokeOptions) {
	invokeOpts.CodePaths = []string{agentExecutorInitEntry, agentExecutorCallEntry, agentExecutorPreStopEntry}
}

// applyAgentCodePath passes an optional local user code package to the runtime. The package may
// contain a traditional AgentHandler, or it may only contain files and executables started by
// the platform Executor through YR_RUNTIME_BOOTSTRAP_CMD.
func applyAgentCodePath(invokeOpts *api.InvokeOptions, spec *types.FuncSpec) {
	if spec == nil || spec.CodeMetaData.StorageType != constants.LocalStorageType ||
		spec.CodeMetaData.CodePath == "" {
		return
	}
	delegateDownloadValue := types.LocalMetaData{
		StorageType: constants.LocalStorageType,
		CodePath:    spec.CodeMetaData.CodePath,
	}
	data, err := json.Marshal(delegateDownloadValue)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent user code delegate download: %v", err)
		return
	}
	log.GetLogger().Infof("[AgentCodePath] set user code DELEGATE_DOWNLOAD=%s", string(data))
	invokeOpts.CreateOpt[constant.DelegateDownloadKey] = string(data)
}

func applyAgentCodePaths(invokeOpts *api.InvokeOptions, spec *types.FuncSpec) {
	if spec == nil {
		return
	}
	entries := []string{spec.ExtendedMetaData.Initializer.Handler, spec.FuncMetaData.Handler}
	if spec.ExtendedMetaData.PreStop.Handler != "" {
		entries = append(entries, spec.ExtendedMetaData.PreStop.Handler)
	}
	invokeOpts.CodePaths = entries
}

// buildAgentCreateArgs builds the libruntime CreateInstance args array for the agent create
// path. Mirrors function-scaler instance_operation_kernel.go:1170-1199, 1202-1235 for the
// non-HTTP / non-CustomContainer local-codePath path. It returns four args: funcSpecData carries
// the platform Executor lifecycle metadata; createParamsData
// carries instanceLabel plus the userInitEntry and userCallEntry; schedulerData is an empty JSON
// object because agent instances are not scheduler-managed; createEvent is an empty JSON object
// because agent has no event payload.
//
// Three mutually exclusive branches, selected by config.platformExecutor + spec + runtimeSpec:
//   - platformExecutor=true: platform Executor entries (inline without a user handler, or a
//     registered funcSpec whose handler is empty).
//   - spec != nil: registered mode, user entries from the watched funcSpec.
//   - runtimeSpec != nil with a non-empty handler: inline+supervisor user-code mode (design
//     §4.5.1 UC2). A user funcSpec is synthesized from runtime_spec so the runtime's faas
//     init/call path registers the user's handler/preStop the same way a registered funcSpec
//     would.
//
// resKey mirrors the ResourceSpecification built by buildAgentResourceSpecJSON (same CPU/Memory
// source, InvokeLabel empty since agent create does not set one).
func buildAgentCreateArgs(config agentCreateConfig, resKey resspeckey.ResourceSpecification) []api.Arg {
	runtime := config.runtime
	spec := config.spec
	rs := config.runtimeSpec
	preStopTimeout := config.preStopTimeout
	platformExecutor := config.platformExecutor
	funcSpecData := []byte("{}")
	userInitEntry, userCallEntry := "", ""
	if platformExecutor {
		executorSpec := types.FuncSpec{
			FuncMetaData: types.FuncMetaData{
				Handler: agentExecutorCallEntry,
				Runtime: runtime,
				Timeout: agentCreateBusinessTimeoutSeconds,
			},
			ResourceMetaData: types.ResourceMetaData{CPU: resKey.CPU, Memory: resKey.Memory},
			ExtendedMetaData: types.ExtendedMetaData{
				Initializer: types.Initializer{Handler: agentExecutorInitEntry, Timeout: agentInitTimeoutSeconds},
				PreStop: types.PreStop{
					Handler: agentExecutorPreStopEntry,
					Timeout: preStopTimeout,
				},
			},
		}
		var err error
		funcSpecData, err = json.Marshal(executorSpec)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal agent executor func spec: %v", err)
			funcSpecData = []byte("{}")
		}
		userInitEntry, userCallEntry = agentExecutorInitEntry, agentExecutorCallEntry
	} else if spec != nil {
		userSpec := *spec
		userSpec.ResourceMetaData.CPU = resKey.CPU
		userSpec.ResourceMetaData.Memory = resKey.Memory
		var err error
		funcSpecData, err = json.Marshal(userSpec)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal user agent func spec: %v", err)
			funcSpecData = []byte("{}")
		}
		userInitEntry = spec.ExtendedMetaData.Initializer.Handler
		userCallEntry = spec.FuncMetaData.Handler
	} else if rs != nil {
		// inline+supervisor user-code mode: synthesize the user funcSpec the runtime faas
		// init/call path expects (load_context_meta reads funcMetaData.handler,
		// extendedMetaData.initializer/pre_stop with the same snake_case tags as a registered
		// funcSpec). Entry symbols come from resolveInlineHandlerEntries, the single source of
		// truth for the [init?, handler, preStop?] layout shared with applyAgentInlineCode.
		var preStopEntry string
		userInitEntry, userCallEntry, preStopEntry = resolveInlineHandlerEntries(rs)
		if userCallEntry != "" {
			inlineSpec := types.FuncSpec{
				FuncMetaData: types.FuncMetaData{
					Handler: userCallEntry,
					Runtime: runtime,
					Timeout: agentCreateBusinessTimeoutSeconds,
				},
				ResourceMetaData: types.ResourceMetaData{CPU: resKey.CPU, Memory: resKey.Memory},
				ExtendedMetaData: types.ExtendedMetaData{
					Initializer: types.Initializer{Handler: userInitEntry, Timeout: agentInitTimeoutSeconds},
					PreStop: types.PreStop{
						Handler: preStopEntry,
						Timeout: preStopTimeout,
					},
				},
			}
			var err error
			funcSpecData, err = json.Marshal(inlineSpec)
			if err != nil {
				log.GetLogger().Warnf("failed to marshal inline agent user func spec: %v", err)
				funcSpecData = []byte("{}")
			}
		}
	}
	params := createParams{
		InstanceLabel: resKey.InvokeLabel,
		EventCreateParams: eventCreateParams{
			UserInitEntry: userInitEntry,
			UserCallEntry: userCallEntry,
		},
	}
	createParamsData, err := json.Marshal(params)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent create params: %v", err)
		createParamsData = []byte("{}")
	}
	return []api.Arg{
		{Type: api.Value, Data: funcSpecData},
		{Type: api.Value, Data: createParamsData},
		{Type: api.Value, Data: []byte("{}")},
		{Type: api.Value, Data: []byte("{}")},
	}
}

// resolveInlineHandlerEntries extracts the (init, call, preStop) user entry symbols from an
// inline runtime_spec, mirroring applyAgentInlineCode's CodePaths layout [init?, handler, preStop?]:
// init from extendedHandler.initializer, call from handler, preStop from extendedHandler.pre_stop
// (each "" when unset/absent). Shared by buildAgentCreateArgs (synthesized funcSpec + createParams)
// and applyAgentInlineCode (wire CodePaths) so the two never diverge on entry layout.
func resolveInlineHandlerEntries(rs *RuntimeSpec) (initEntry, callEntry, preStopEntry string) {
	if rs == nil {
		return "", "", ""
	}
	callEntry = strings.TrimSpace(rs.Handler)
	if rs.ExtendedHandler != nil {
		initEntry = strings.TrimSpace(rs.ExtendedHandler.Initializer)
		preStopEntry = strings.TrimSpace(rs.ExtendedHandler.PreStop)
	}
	return initEntry, callEntry, preStopEntry
}

// applyAgentInlineMeta passes container config from req.RuntimeSpec into createOptions (inline mode).
// It also sinks CodePath→DELEGATE_DOWNLOAD (storage_type=local) and Handler/ExtendedHandler
// →CodePaths, mirroring registered's applyAgentCodePath/applyAgentCodePaths so inline+supervisor
// instances reach the same runtime code-loading path. CodePath presence is not validated here;
// missing codePath/handler is surfaced at invoke time (design §6 C7/C8).
func applyAgentInlineMeta(invokeOpts *api.InvokeOptions, req CreateAgentRequest) {
	c := req.RuntimeSpec
	if c == nil {
		return
	}
	if c.SandboxType != "" {
		invokeOpts.CreateOpt["sandbox_type"] = c.SandboxType
	}
	if len(c.Cmds) > 0 {
		mergeAgentBootstrapCmds(invokeOpts, c.Cmds)
	}
	applyAgentInlineCode(invokeOpts, c)
	if c.Rootfs == nil {
		return
	}
	if c.Rootfs.User != "" {
		invokeOpts.CreateOpt["host_user"] = c.Rootfs.User
	}
	replaceAgentUserPlaceholder(invokeOpts, c.Rootfs.User)
	if c.Rootfs.ImageURL != "" {
		rootfsJSON, err := json.Marshal(map[string]interface{}{
			"type": "image", "imageurl": c.Rootfs.ImageURL, "mounts": []interface{}{},
		})
		if err != nil {
			log.GetLogger().Warnf("failed to marshal agent rootfs imageurl: %v", err)
		} else if existing, exists := invokeOpts.CreateOpt["rootfs"]; exists && existing != "" {
			invokeOpts.CreateOpt["rootfs"] = mergeRootfsJSON(existing, string(rootfsJSON))
		} else {
			invokeOpts.CreateOpt["rootfs"] = string(rootfsJSON)
		}
	}
	if len(c.Rootfs.Ports) > 0 {
		applyAgentPorts(invokeOpts, c.Rootfs.Ports)
	}
}

// applyAgentInlineCode sinks the inline user-code config from runtime_spec into
// createOptions: codePath→DELEGATE_DOWNLOAD (storage_type=local, per design §6 C4) so the
// function_agent's LocalDeployer resolves the code dir; handler/extendedHandler→CodePaths
// via resolveInlineHandlerEntries so CodeManager.load_functions registers the
// [init, handler, preStop?] entry layout (init empty when unset, keeping length>=2 so
// _are_faas_entries treats it as FaaS entries, design §6 C5). The same resolver feeds the
// synthesized funcSpec in buildAgentCreateArgs, so wire CodePaths and create args never diverge.
// codePath/handler presence is not validated here (§6 C7/C8: surfaced at invoke).
func applyAgentInlineCode(invokeOpts *api.InvokeOptions, c *RuntimeSpec) {
	if c.CodePath != "" {
		delegateDownloadValue := types.LocalMetaData{
			StorageType: constants.LocalStorageType,
			CodePath:    c.CodePath,
		}
		data, err := json.Marshal(delegateDownloadValue)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal inline agent delegate download: %v", err)
		} else {
			log.GetLogger().Infof("[AgentInlineCode] set DELEGATE_DOWNLOAD=%s", string(data))
			invokeOpts.CreateOpt[constant.DelegateDownloadKey] = string(data)
		}
	}
	if c.Handler != "" {
		initEntry, _, preStopEntry := resolveInlineHandlerEntries(c)
		codeEntrys := []string{initEntry, c.Handler}
		if preStopEntry != "" {
			codeEntrys = append(codeEntrys, preStopEntry)
		}
		invokeOpts.CodePaths = codeEntrys
	}
}

// mergeAgentBootstrapCmds serializes runtime_spec.cmds (a list of argv arrays) into the
// YR_RUNTIME_BOOTSTRAP_CMD key inside createOptions["DELEGATE_ENV_VAR"]. The runtime reads
// this env at startup and fork+execs each argv as a child process. It merges into the
// existing DELEGATE_ENV_VAR map without clobbering other env vars; a pre-existing
// YR_RUNTIME_BOOTSTRAP_CMD key (e.g. set explicitly via env_vars) wins and is left untouched.
func mergeAgentBootstrapCmds(invokeOpts *api.InvokeOptions, cmds [][]string) {
	cmdsJSON, err := json.Marshal(cmds)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent cmds: %v", err)
		return
	}
	env := map[string]string{}
	if existing, ok := invokeOpts.CreateOpt["DELEGATE_ENV_VAR"]; ok && existing != "" {
		if err := json.Unmarshal([]byte(existing), &env); err != nil {
			log.GetLogger().Warnf("failed to unmarshal agent DELEGATE_ENV_VAR: %v", err)
			env = map[string]string{}
		}
	}
	if _, exists := env[agentBootstrapCmdEnv]; exists {
		log.GetLogger().Warnf("agent %s already set, skip merging runtime_spec.cmds", agentBootstrapCmdEnv)
		return
	}
	env[agentBootstrapCmdEnv] = string(cmdsJSON)
	merged, err := json.Marshal(env)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent env with cmds: %v", err)
		return
	}
	invokeOpts.CreateOpt["DELEGATE_ENV_VAR"] = string(merged)
}

// replaceAgentUserPlaceholder replaces the workspace target placeholder in createOptions["rootfs"]
// with a real path. When user is non-empty, /home/__AGENT_USER__ → /home/<user>; when empty,
// the whole /home/__AGENT_USER__ falls back to agentDefaultWorkspaceTarget (e.g. /workspace),
// so the mount target is never a literal placeholder.
func replaceAgentUserPlaceholder(invokeOpts *api.InvokeOptions, user string) {
	rootfsStr, exists := invokeOpts.CreateOpt["rootfs"]
	if !exists || rootfsStr == "" {
		return
	}
	if user != "" {
		invokeOpts.CreateOpt["rootfs"] = strings.ReplaceAll(rootfsStr, agentUserPlaceholder, user)
		return
	}
	invokeOpts.CreateOpt["rootfs"] = strings.ReplaceAll(
		rootfsStr, "/home/"+agentUserPlaceholder, agentDefaultWorkspaceTarget)
}

// applyAgentPorts builds createOptions["network"] portForwardings from rootfs.ports.
func applyAgentPorts(invokeOpts *api.InvokeOptions, ports []string) {
	portForwardings := make([]map[string]interface{}, 0, len(ports))
	for _, p := range ports {
		protocol := "TCP"
		portStr := p
		if idx := strings.Index(p, ":"); idx >= 0 {
			protocol = strings.ToUpper(p[:idx])
			portStr = p[idx+1:]
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}
		portForwardings = append(portForwardings, map[string]interface{}{"port": port, "protocol": protocol})
	}
	networkJSON, err := json.Marshal(map[string]interface{}{"portForwardings": portForwardings})
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent network portForwardings: %v", err)
		return
	}
	invokeOpts.CreateOpt["network"] = string(networkJSON)
}

// mergeAgentStaticEnv merges the function's static environment (funcSpec.EnvMetaData.Environment,
// a JSON map registered with the function) into createOptions["DELEGATE_ENV_VAR"]. Since agent
// reuses the faasExecutor system function as FuncID, function_proxy sinks the (empty) faasExecutor
// envMetaData — the user function's static env never reaches the container unless frontend passes it.
// Dynamic env_vars (set earlier by applyAgentDynamicEnv) take precedence; static env only fills keys
// not already present. ParseDelegateEnv on the C++ side also inserts (non-overwriting), so the
// merged JSON yields: dynamic values win, static values fill the rest.
func mergeAgentStaticEnv(invokeOpts *api.InvokeOptions, spec *types.FuncSpec) {
	if spec == nil || spec.EnvMetaData.Environment == "" {
		return
	}
	var staticEnv map[string]string
	if err := json.Unmarshal([]byte(spec.EnvMetaData.Environment), &staticEnv); err != nil {
		log.GetLogger().Warnf("failed to unmarshal agent static environment: %v", err)
		return
	}
	if len(staticEnv) == 0 {
		return
	}
	merged := make(map[string]string, len(staticEnv))
	for k, v := range staticEnv {
		merged[k] = v
	}
	if existing, ok := invokeOpts.CreateOpt["DELEGATE_ENV_VAR"]; ok && existing != "" {
		var dynamicEnv map[string]string
		if err := json.Unmarshal([]byte(existing), &dynamicEnv); err == nil {
			for k, v := range dynamicEnv {
				merged[k] = v // dynamic wins
			}
		}
	}
	envJSON, err := json.Marshal(merged)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal merged agent env: %v", err)
		return
	}
	invokeOpts.CreateOpt["DELEGATE_ENV_VAR"] = string(envJSON)
}

// mergeRootfsJSON merges two rootfs JSON strings (image into existing mounts).
func mergeRootfsJSON(existing, imageJSON string) string {
	var existingMap, imageMap map[string]interface{}
	if err := json.Unmarshal([]byte(existing), &existingMap); err != nil {
		return imageJSON
	}
	if err := json.Unmarshal([]byte(imageJSON), &imageMap); err != nil {
		return existing
	}
	for k, v := range imageMap {
		if k == "mounts" {
			if _, ok := existingMap[k]; !ok {
				existingMap[k] = v
			}
			continue
		}
		existingMap[k] = v
	}
	merged, err := json.Marshal(existingMap)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal merged agent rootfs: %v", err)
		return existing
	}
	return string(merged)
}

// applyAgentRootfsMounts builds rootfs.mounts from the workspace and custom mounts and writes
// it into CreateOpt["rootfs"] (not CustomExtensions) so docker executor's BuildBindMounts can
// parse it. The workspace source is also mirrored into CreateOpt["workspace"] so the Get handler
// can surface it as rootfs.workspace without disambiguating it from a colliding user mount.
func applyAgentRootfsMounts(invokeOpts *api.InvokeOptions, req CreateAgentRequest) error {
	var rootfsMounts []map[string]interface{}
	if req.Workspace != "" {
		if err := validateBindSource(req.Workspace, "workspace"); err != nil {
			return err
		}
		rootfsMounts = append(rootfsMounts, map[string]interface{}{
			"source": req.Workspace, "target": "/home/" + agentUserPlaceholder, "readonly": false,
		})
		invokeOpts.CreateOpt["workspace"] = req.Workspace
	}
	for _, m := range req.Mounts {
		if err := validateBindSource(m.Source, "mount source"); err != nil {
			return err
		}
		rootfsMounts = append(rootfsMounts, map[string]interface{}{
			"source": m.Source, "target": m.Target, "readonly": m.ReadOnly,
		})
	}
	var err error
	rootfsMounts, err = appendPlatformSSHMount(rootfsMounts)
	if err != nil {
		return err
	}
	if len(rootfsMounts) == 0 {
		return nil
	}
	rootfsJSON, err := json.Marshal(map[string]interface{}{"mounts": rootfsMounts})
	if err != nil {
		return fmt.Errorf("failed to marshal rootfs mounts: %v", err)
	}
	invokeOpts.CreateOpt["rootfs"] = string(rootfsJSON)
	return nil
}

func appendPlatformSSHMount(mounts []map[string]interface{}) ([]map[string]interface{}, error) {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv(sshEnableEnv)), "true") {
		return mounts, nil
	}
	source := strings.TrimSpace(os.Getenv(sshPublicKeyDirectoryEnv))
	if err := validateBindSource(source, sshPublicKeyDirectoryEnv); err != nil {
		return nil, err
	}
	for _, mount := range mounts {
		target, _ := mount["target"].(string)
		if pathsOverlap(target, sshContainerMountDirectory) {
			return nil, fmt.Errorf("mount target %s overlaps reserved platform SSH path", target)
		}
	}
	return append(mounts, map[string]interface{}{
		"source": source, "target": sshContainerMountDirectory, "readonly": true,
	}), nil
}

func pathsOverlap(first, second string) bool {
	first = filepath.Clean(first)
	second = filepath.Clean(second)
	return first == second || strings.HasPrefix(first, second+string(filepath.Separator)) ||
		strings.HasPrefix(second, first+string(filepath.Separator))
}

// applyAgentDynamicEnv sinks dynamic env vars (incl. userid) via createOptions["DELEGATE_ENV_VAR"]
// (JSON), which the runtime merges into sandbox userenvs. Static env travels with the
// function meta (EnvMetaData), not here.
func applyAgentDynamicEnv(invokeOpts *api.InvokeOptions, req CreateAgentRequest) {
	if len(req.EnvVars) == 0 {
		return
	}
	envJSON, err := json.Marshal(req.EnvVars)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent env_vars: %v", err)
		return
	}
	invokeOpts.CreateOpt["DELEGATE_ENV_VAR"] = string(envJSON)
}

// applyAgentCreateOpts sets the common createOptions shared across agent instances.
// funcKeyNote: inline mode uses funcKey; registered mode uses urn.
func applyAgentCreateOpts(invokeOpts *api.InvokeOptions, ctx *gin.Context, req CreateAgentRequest,
	config agentCreateConfig) {
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	invokeOpts.CreateOpt["tenantId"] = tenantID
	if config.inline {
		invokeOpts.CreateOpt[constant.FunctionKeyNote] = config.funcKey
	} else {
		invokeOpts.CreateOpt[constant.FunctionKeyNote] = req.Urn
	}
	invokeOpts.CreateOpt[constant.InstanceTypeNote] = agentInstanceType
	invokeOpts.CreateOpt[constant.SchedulerManagedNote] = strconv.FormatBool(false)
	invokeOpts.CreateOpt["call_timeout"] = strconv.Itoa(agentCreateBusinessTimeoutSeconds)
	invokeOpts.CreateOpt["init_call_timeout"] = strconv.Itoa(agentInitTimeoutSeconds)
	gracefulShutdownSeconds := agentGracefulShutdownSeconds
	if config.preStopTimeout+agentShutdownReserveSeconds > gracefulShutdownSeconds {
		gracefulShutdownSeconds = config.preStopTimeout + agentShutdownReserveSeconds
	}
	invokeOpts.CreateOpt["GRACEFUL_SHUTDOWN_TIME"] = strconv.Itoa(gracefulShutdownSeconds)
	invokeOpts.CreateOpt["DELEGATE_DIRECTORY_INFO"] = agentDelegateDirectory
	invokeOpts.CreateOpt["DELEGATE_DIRECTORY_QUOTA"] = strconv.Itoa(agentDirectoryQuotaMB)
	invokeOpts.CreateOpt["ConcurrentNum"] = agentConcurrency
	if invokeOpts.RecoverRetryTimes > 0 {
		invokeOpts.CreateOpt["RecoverRetryTimes"] = strconv.Itoa(invokeOpts.RecoverRetryTimes)
	}
	if resSpecJSON, err := buildAgentResourceSpecJSON(invokeOpts.Cpu, invokeOpts.Memory); err == nil {
		invokeOpts.CreateOpt[constant.ResourceSpecNote] = resSpecJSON
	} else {
		log.GetLogger().Warnf("failed to marshal agent resource spec: %v", err)
	}
}

func validateBindSource(path, label string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%s must be an absolute path: %s", label, path)
	}
	if !isSafeBindSource(path) {
		return fmt.Errorf("unsafe %s: %s", label, path)
	}
	return nil
}

func createAgentInstance(
	ctx *gin.Context, req CreateAgentRequest, funcMeta api.FunctionMeta, invokeOpts api.InvokeOptions,
	args []api.Arg,
) {
	directReq, err := util.NewDirectCreateRequest(funcMeta, args, invokeOpts)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	instanceID, err := util.GetDirectProxyClient().CreateInstance(directReq)
	if err != nil {
		if shouldTreatCreateTimeoutAsSuccess(instanceID, err) {
			if waitForAgentInstanceRunning(instanceID, funcMeta.FuncID,
				invokeOpts.CreateOpt[constant.ResourceSpecNote]) {
				log.GetLogger().Infof(
					"agent instance reached running state after create timeout instanceID=%s name=%s ns=%s",
					instanceID, req.Name, req.Namespace)
			}
			log.GetLogger().Warnf(
				"agent create returned timeout after scheduling instanceID=%s name=%s ns=%s: %v",
				instanceID, req.Name, req.Namespace, err)
			ctx.JSON(http.StatusOK, gin.H{"code": 200, "instance_id": instanceID})
			return
		}
		log.GetLogger().Errorf("failed to create agent instance name=%s ns=%s: %v", req.Name, req.Namespace, err)
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError, fmt.Errorf("failed to create agent: %v", err))
		return
	}

	log.GetLogger().Infof("agent created: instanceID=%s name=%s ns=%s", instanceID, req.Name, req.Namespace)
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "instance_id": instanceID})
}

// DeleteHandler handles DELETE /api/agent/:instanceId.
// Input is the instance_id returned by create; kills the agent instance directly.
func DeleteHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	// libruntime.Kill is idempotent for missing IDs; check cache first so unknown or
	// already-deleted instances return 404 instead of 200.
	if instancemanager.GetGlobalInstanceScheduler().
		GetInstanceByIDAcrossFunctions(instanceID) == nil {
		ctx.JSON(http.StatusNotFound,
			gin.H{"code": 404, "message": fmt.Sprintf("instance not found: %s", instanceID)})
		return
	}
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	invokeOpts := api.InvokeOptions{
		TraceID: ctx.GetHeader(constant.HeaderTraceID),
	}
	if err := util.GetDirectProxyClient().KillInstance(util.NewDirectKillRequest(
		ctx.Request.Context(), instanceID, agentKillInstanceSignal, []byte("agent deleted"), tenantID, invokeOpts,
	)); err != nil {
		log.GetLogger().Errorf("failed to kill agent instance %s: %v", instanceID, err)
		ctx.JSON(http.StatusInternalServerError,
			gin.H{"code": 500, "message": fmt.Sprintf("failed to delete agent: %v", err)})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "status": "deleted"})
}

func shouldTreatCreateTimeoutAsSuccess(instanceID string, err error) bool {
	if instanceID == "" || err == nil {
		return false
	}
	var errInfo api.ErrorInfo
	if !errors.As(err, &errInfo) {
		return false
	}
	return errInfo.Code == agentCreateTimeoutCode
}

func buildAgentResourceSpecJSON(cpu, memory int) (string, error) {
	resourceSpec := resspeckey.ResourceSpecification{
		CPU:         int64(cpu),
		Memory:      int64(memory),
		InvokeLabel: "",
	}
	data, err := json.Marshal(resourceSpec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func isAgentInstanceRunning(instanceID, functionID, resourceSpecNote string) bool {
	resKey, err := resspeckey.GetResKeyFromStr(resourceSpecNote)
	if err != nil {
		log.GetLogger().Warnf("failed to parse agent resource spec while checking instance status: %v", err)
		return false
	}
	return instancemanager.GetGlobalInstanceScheduler().GetInstance(
		functionID, resKey.String(), instanceID) != nil
}

// isSafeBindSource mirrors docker_executor IsSafeBindSource: reject unsafe host paths
// ("/", "/etc", "/proc", "/sys", "/dev", "/boot", docker.sock, "..").
func isSafeBindSource(path string) bool {
	clean := filepath.Clean(path)
	switch clean {
	case "/", "/etc", "/proc", "/sys", "/dev", "/boot":
		return false
	}
	if strings.Contains(clean, "..") {
		return false
	}
	if strings.HasSuffix(clean, "docker.sock") {
		return false
	}
	return true
}

// --- instance status query (GET /api/agent, GET /api/agent/:instanceId) ---

// Package-level indirections so tests can stub the execendpoint cache and IP extraction.
var (
	lookupAgentInstanceSummaries = func(tenantID, instanceID string) []execendpoint.Summary {
		return execendpoint.Default().ListSummaries(tenantID, instanceID)
	}
	lookupAgentInstanceEndpoint = func(instanceID string) (execendpoint.Endpoint, bool) {
		return execendpoint.Default().Get(instanceID)
	}
	extractNodeIP = func(proxyGrpcAddress string) string { return route.ExtractIP(proxyGrpcAddress) }
)

// InstanceBrief is the minimal List output: identity + addresses only.
type InstanceBrief struct {
	InstanceID  string `json:"instance_id"`
	NodeIP      string `json:"node_ip,omitempty"`
	SandboxIP   string `json:"sandbox_ip,omitempty"`
	SandboxType string `json:"sandbox_type,omitempty"`
}

// InstanceDetail is the verbose Get output: brief fields plus create configuration from createOptions.
type InstanceDetail struct {
	InstanceID  string             `json:"instance_id"`
	NodeIP      string             `json:"node_ip,omitempty"`
	SandboxIP   string             `json:"sandbox_ip,omitempty"`
	SandboxType string             `json:"sandbox_type,omitempty"`
	SandboxID   string             `json:"sandbox_id,omitempty"`
	Rootfs      *RootfsInfo        `json:"rootfs,omitempty"`
	Ports       []string           `json:"ports,omitempty"`
	EnvVars     map[string]string  `json:"env_vars,omitempty"`
	Resources   map[string]float64 `json:"resources,omitempty"`
	StartTime   string             `json:"start_time,omitempty"`
}

// RootfsInfo mirrors createOptions["rootfs"]. User and workspace come from the sibling
// host_user/workspace keys, not the rootfs JSON.
type RootfsInfo struct {
	Type      string      `json:"type,omitempty"`
	ImageURL  string      `json:"imageurl,omitempty"`
	User      string      `json:"user,omitempty"`
	Workspace string      `json:"workspace,omitempty"`
	Mounts    []MountInfo `json:"mounts,omitempty"`
}

// MountInfo is one bind mount. ReadOnly has no omitempty so readonly:false is always printed.
type MountInfo struct {
	Source   string `json:"source,omitempty"`
	Target   string `json:"target,omitempty"`
	ReadOnly bool   `json:"readonly"`
}

// rootfsJSON mirrors the subset of createOptions["rootfs"] Get needs.
type rootfsJSON struct {
	Type     string            `json:"type"`
	ImageURL string            `json:"imageurl"`
	Mounts   []json.RawMessage `json:"mounts"`
}

// networkJSON mirrors createOptions["network"] (built by applyAgentPorts).
type networkJSON struct {
	PortForwardings []struct {
		Port     int    `json:"port"`
		Protocol string `json:"protocol"`
	} `json:"portForwardings"`
}

// ListHandler handles GET /api/agent: returns the brief view of every cached (RUNNING) instance.
func ListHandler(ctx *gin.Context) {
	summaries := lookupAgentInstanceSummaries("", "")
	instances := make([]InstanceBrief, 0, len(summaries))
	for _, s := range summaries {
		// Skip system drivers (driver-frontend/driver-scheduler, etc.): they register in
		// the instance cache without a sandbox_type, so they are not user agent instances.
		if s.SandboxType == "" {
			continue
		}
		b := InstanceBrief{InstanceID: s.InstanceID, SandboxType: s.SandboxType}
		if ep, ok := lookupAgentInstanceEndpoint(s.InstanceID); ok {
			b.NodeIP = extractNodeIP(ep.ProxyGrpcAddress)
		}
		b.SandboxIP = s.ContainerIP
		instances = append(instances, b)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "instances": instances})
}

// GetHandler handles GET /api/agent/:instanceId: returns the detailed view of one instance.
// GET /api/agent (no path segment) routes to ListHandler, so :instanceId is never empty here.
func GetHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	summaries := lookupAgentInstanceSummaries("", instanceID)
	if len(summaries) == 0 {
		ctx.JSON(http.StatusNotFound, gin.H{"code": http.StatusNotFound,
			"message": "instance not found or not running"})
		return
	}
	s := summaries[0]
	d := InstanceDetail{
		InstanceID:  s.InstanceID,
		SandboxType: s.SandboxType,
		SandboxID:   s.ContainerID,
		Resources:   flattenResources(s.Resources),
		StartTime:   s.StartTime,
	}
	if ep, ok := lookupAgentInstanceEndpoint(instanceID); ok {
		d.NodeIP = extractNodeIP(ep.ProxyGrpcAddress)
	}
	d.SandboxIP = s.ContainerIP
	applyDetailCreateOptions(&d, s.CreateOptions)
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "instance": d})
}

// flattenResources collapses the scalar-resource map (e.g. {"CPU":{"scalar":{"value":600,"limit":0}}})
// to {"CPU":600}, exposing only the scalar value. limit is dropped. Storage is
// carried internally in bytes and converted back to MiB for the public API.
func flattenResources(in map[string]execendpoint.Resource) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for name, r := range in {
		value := r.Scalar.Value
		if name == agentStorageResourceName {
			value /= agentStorageBytesPerMiB
		}
		out[name] = value
	}
	return out
}

// applyDetailCreateOptions parses rootfs/network/env_vars from createOptions into the detail.
// Malformed JSON is logged and skipped so a bad value never blanks the whole response.
func applyDetailCreateOptions(d *InstanceDetail, opts map[string]string) {
	if len(opts) == 0 {
		return
	}
	d.Rootfs = parseRootfs(opts["rootfs"], opts["host_user"], opts["workspace"], d.InstanceID)
	d.Ports = parsePorts(opts["network"])
	d.EnvVars = parseEnvVars(opts["DELEGATE_ENV_VAR"])
}

// parseRootfs decodes createOptions["rootfs"] into RootfsInfo. User and workspace come from
// the sibling host_user/workspace keys, not the rootfs JSON. Returns nil when every field is
// empty. Per-mount JSON errors are tolerated by skipping the bad mount (see parseRootfsMounts).
func parseRootfs(rootfsStr, hostUser, workspace, instanceID string) *RootfsInfo {
	info := &RootfsInfo{User: hostUser, Workspace: workspace}
	if rootfsStr != "" {
		var rf rootfsJSON
		if err := json.Unmarshal([]byte(rootfsStr), &rf); err != nil {
			log.GetLogger().Warnf("agent get: failed to unmarshal rootfs for %s: %v", instanceID, err)
		} else {
			info.Type = rf.Type
			info.ImageURL = rf.ImageURL
			info.Mounts = parseRootfsMounts(rf.Mounts)
		}
	}
	if info.Type != "" || info.ImageURL != "" || info.User != "" ||
		info.Workspace != "" || len(info.Mounts) > 0 {
		return info
	}
	return nil
}

// parseRootfsMounts decodes the raw mounts array, skipping entries that fail to unmarshal.
func parseRootfsMounts(rawMounts []json.RawMessage) []MountInfo {
	mounts := make([]MountInfo, 0, len(rawMounts))
	for _, raw := range rawMounts {
		var m MountInfo
		if err := json.Unmarshal(raw, &m); err == nil {
			mounts = append(mounts, m)
		}
	}
	return mounts
}

// parsePorts decodes createOptions["network"] into the list of port labels (e.g. "tcp:22").
func parsePorts(networkStr string) []string {
	if networkStr == "" {
		return nil
	}
	var netCfg networkJSON
	if err := json.Unmarshal([]byte(networkStr), &netCfg); err != nil {
		return nil
	}
	ports := make([]string, 0, len(netCfg.PortForwardings))
	for _, pf := range netCfg.PortForwardings {
		label := strconv.Itoa(pf.Port)
		if pf.Protocol != "" {
			label = strings.ToLower(pf.Protocol) + ":" + label
		}
		ports = append(ports, label)
	}
	return ports
}

// parseEnvVars decodes createOptions["DELEGATE_ENV_VAR"] (a JSON object) into the env map.
func parseEnvVars(envStr string) map[string]string {
	if envStr == "" {
		return nil
	}
	var env map[string]string
	if err := json.Unmarshal([]byte(envStr), &env); err == nil && len(env) > 0 {
		return env
	}
	return nil
}

// maxFileUploadSize is the per-file upload size cap (512MB). Keep it aligned
// with DEFAULT_MAX_FILE_SIZE in yuanrong-agentruntime's FileHandler. It is enforced
// both via Content-Length and by accumulating bytes read from the multipart
// file so a client lying about Content-Length cannot bypass it.
const maxFileUploadSize int64 = 512 * 1024 * 1024
const maxMultipartOverhead int64 = 1024 * 1024

// fileTransferWaitTimeout bounds how long the file-transfer handlers wait for
// an instance to appear in the watcher cache before returning 404.
const fileTransferWaitTimeout = 5 * time.Second

// Kept indirect so handler tests can exercise the complete HTTP adaptation
// without opening a real FunctionProxy tunnel.
var dialAgentSandboxTunnel = wsproxy.DialSandboxTunnel

// waitForAgentInstanceExist returns the instance spec for instanceID if it is
// present in the watcher cache within fileTransferWaitTimeout. It reuses the
// same WaitInstanceByID primitive the rest of the lifecycle path relies on.
func waitForAgentInstanceExist(instanceID string) (*types.InstanceSpecification, error) {
	if execendpoint.Default().IsPaused(instanceID) {
		return nil, execendpoint.NewInstancePausedError(instanceID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), fileTransferWaitTimeout)
	defer cancel()
	return instancemanager.WaitInstanceByID(ctx, instanceID)
}

func writeFileTransferInstanceError(ctx *gin.Context, instanceID string, err error) {
	statusCode := http.StatusNotFound
	message := fmt.Sprintf("instance %s not found", instanceID)
	if errors.Is(err, execendpoint.ErrInstancePaused) {
		statusCode = http.StatusConflict
		message = err.Error()
	}
	ctx.JSON(statusCode, gin.H{"code": statusCode, "message": message})
}

// FileUploadHandler handles POST /api/agent/:instanceId/files/upload.
// It retains the public multipart contract, then streams the file part over the
// existing HTTP-over-TCP tunnel to the Agent Executor HTTP server.
//
// The multipart form must include a "path" text field and a "file" file field.
// The optional "mode" (octal permission, e.g. "644") is a URL query parameter,
// not a multipart field, to avoid ordering constraints with the sequential
// multipart stream.
func FileUploadHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	// Validate Content-Length against the size cap before reading the body so
	// an oversized request is rejected without buffering it.
	maxRequestSize := maxFileUploadSize + maxMultipartOverhead
	if contentLength := ctx.Request.ContentLength; contentLength > maxRequestSize {
		ctx.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"code":    http.StatusRequestEntityTooLarge,
			"message": fmt.Sprintf("request size %d exceeds max %d", contentLength, maxRequestSize),
		})
		return
	}

	// Verify the instance exists and is known to the watcher cache. A missing
	// instance cannot receive files.
	if _, err := waitForAgentInstanceExist(instanceID); err != nil {
		log.GetLogger().Warnf("file upload instance not found %s: %v", instanceID, err)
		writeFileTransferInstanceError(ctx, instanceID, err)
		return
	}

	// "mode" is a URL query parameter (e.g. ?mode=644), not a multipart field.
	// This decouples it from multipart field ordering: the sequential multipart
	// stream cannot rewind to read a "mode" field placed after "file".
	fileMode := ctx.Query("mode")

	// Limit the multipart reader memory to the cap so a malicious payload is
	// rejected at the parse boundary instead of exhausting memory.
	ctx.Request.Body = http.MaxBytesReader(ctx.Writer, ctx.Request.Body, maxRequestSize)
	reader, err := ctx.Request.MultipartReader()
	if err != nil {
		log.GetLogger().Warnf("file upload multipart read failed instance %s: %v", instanceID, err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": fmt.Sprintf("invalid multipart request: %v", err),
		})
		return
	}

	var targetPath string
	pathSeen := false
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeMultipartReadError(ctx, instanceID, err)
			return
		}
		switch part.FormName() {
		case "path":
			pathSeen = true
			buf, err := io.ReadAll(part)
			if err != nil {
				log.GetLogger().Warnf("file upload path read failed instance %s: %v", instanceID, err)
				ctx.JSON(http.StatusBadRequest, gin.H{
					"code":    http.StatusBadRequest,
					"message": fmt.Sprintf("read path failed: %v", err),
				})
				return
			}
			targetPath = strings.TrimSpace(string(buf))
		case "file":
			// The "path" field must precede the "file" field so the upload
			// target is known before streaming begins.
			if !pathSeen || targetPath == "" {
				ctx.JSON(http.StatusBadRequest, gin.H{
					"code":    http.StatusBadRequest,
					"message": "'path' field must precede 'file' in the multipart form",
				})
				return
			}
			// Keep the public multipart parsing in Frontend. The Executor receives
			// only raw bytes and owns the actual filesystem operation.
			countingReader := &countingReader{reader: part}
			query := url.Values{"path": []string{targetPath}}
			if fileMode != "" {
				query.Set("mode", fileMode)
			}
			headers := http.Header{"Content-Type": []string{"application/octet-stream"}}
			request := agentExecutorHTTPRequest{
				method: http.MethodPut, path: "/v1/files/upload", query: query,
				body: countingReader, contentLength: -1, headers: headers,
			}
			if uploadErr := forwardAgentExecutorHTTP(ctx, instanceID, tenantID, request); uploadErr != nil {
				log.GetLogger().Errorf("file upload failed instance %s path %s: %v",
					instanceID, targetPath, uploadErr)
				writeFileTransferError(ctx, uploadErr)
			}
			return
		default:
			// Skip unknown parts; only path and file are consumed.
			_, _ = io.Copy(io.Discard, part)
		}
	}
	ctx.JSON(http.StatusBadRequest, gin.H{
		"code":    http.StatusBadRequest,
		"message": "multipart form must include a 'file' field",
	})
}

// writeMultipartReadError maps a multipart read failure (including the
// MaxBytesReader overflow) to the right HTTP status.
func writeMultipartReadError(ctx *gin.Context, instanceID string, err error) {
	if err == nil {
		return
	}
	if err.Error() == "http: request body too large" {
		ctx.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"code":    http.StatusRequestEntityTooLarge,
			"message": fmt.Sprintf("upload size exceeds max %d", maxFileUploadSize),
		})
		return
	}
	log.GetLogger().Warnf("file upload part read failed instance %s: %v", instanceID, err)
	ctx.JSON(http.StatusBadRequest, gin.H{
		"code":    http.StatusBadRequest,
		"message": fmt.Sprintf("multipart read failed: %v", err),
	})
}

// countingReader wraps a reader and counts the bytes read so the upload path
// can enforce the size cap even when Content-Length is absent or inaccurate.
// It is also used to short-circuit an oversized read before it completes.
type countingReader struct {
	reader io.Reader
	count  int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.count += int64(n)
	if c.count > maxFileUploadSize {
		return n, fmt.Errorf("upload size exceeds max %d: %w", maxFileUploadSize, err)
	}
	return n, err
}

// FileListHandler handles GET /api/agent/:instanceId/files/list.
// It returns a JSON array of files and directories at the given path
// inside the instance's filesystem.
func FileListHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	targetPath := strings.TrimSpace(ctx.Query("path"))
	if targetPath == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "path query parameter is required",
		})
		return
	}
	if _, err := waitForAgentInstanceExist(instanceID); err != nil {
		log.GetLogger().Warnf("file list instance not found %s: %v", instanceID, err)
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": fmt.Sprintf("instance %s not found", instanceID),
		})
		return
	}
	query := url.Values{"path": []string{targetPath}}
	if recursive := ctx.Query("recursive"); recursive != "" {
		query.Set("recursive", recursive)
	}
	if rawMaxDepth := ctx.Query("max_depth"); rawMaxDepth != "" {
		maxDepth, err := strconv.Atoi(rawMaxDepth)
		if err != nil || maxDepth < 0 {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"code":    http.StatusBadRequest,
				"message": "max_depth must be a non-negative integer",
			})
			return
		}
		query.Set("max_depth", strconv.Itoa(maxDepth))
	}
	request := agentExecutorHTTPRequest{
		method: http.MethodGet, path: "/v1/files/list", query: query,
		headers: http.Header{"Accept": []string{"application/json"}},
	}
	if err := forwardAgentExecutorHTTP(ctx, instanceID, tenantID, request); err != nil {
		writeFileTransferError(ctx, err)
	}
}

// FileMkdirHandler handles POST /api/agent/:instanceId/files/mkdir.
// It forwards a directory creation request through the existing TCP tunnel,
// optionally setting the directory mode.
func FileMkdirHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	targetPath := strings.TrimSpace(ctx.Query("path"))
	if targetPath == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "path query parameter is required",
		})
		return
	}
	if _, err := waitForAgentInstanceExist(instanceID); err != nil {
		log.GetLogger().Warnf("file mkdir instance not found %s: %v", instanceID, err)
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": fmt.Sprintf("instance %s not found", instanceID),
		})
		return
	}
	query := url.Values{"path": []string{targetPath}}
	if mode := ctx.Query("mode"); mode != "" {
		query.Set("mode", mode)
	}
	if recursive := ctx.Query("recursive"); recursive != "" {
		query.Set("recursive", recursive)
	}
	request := agentExecutorHTTPRequest{
		method: http.MethodPost, path: "/v1/files/mkdir", query: query,
		headers: http.Header{"Accept": []string{"application/json"}},
	}
	if err := forwardAgentExecutorHTTP(ctx, instanceID, tenantID, request); err != nil {
		writeFileTransferError(ctx, err)
	}
}

// writeFileTransferError maps a file transfer error to the most appropriate
// HTTP status. Executor HTTP statuses are forwarded directly; errors reaching
// this helper are local streaming failures, where size overflow maps to 413
// and everything else maps to 500.
func writeFileTransferError(ctx *gin.Context, err error) {
	if err == nil {
		return
	}
	if ctx.Writer.Written() {
		log.GetLogger().Warnf("file transfer failed after response started: %v", err)
		return
	}
	if strings.Contains(err.Error(), "exceeds max") {
		ctx.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"code":    http.StatusRequestEntityTooLarge,
			"message": err.Error(),
		})
		return
	}
	ctx.JSON(http.StatusInternalServerError, gin.H{
		"code":    http.StatusInternalServerError,
		"message": err.Error(),
	})
}

// FileDownloadHandler handles GET /api/agent/:instanceId/files/download.
// It forwards the request through the existing TCP tunnel. The Executor owns
// Range handling and streams the HTTP response directly back through Frontend.
func FileDownloadHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}

	targetPath := strings.TrimSpace(ctx.Query("path"))
	if targetPath == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
			"message": "path query parameter is required",
		})
		return
	}

	// Verify the instance exists before resolving the download route.
	if _, err := waitForAgentInstanceExist(instanceID); err != nil {
		log.GetLogger().Warnf("file download instance not found %s: %v", instanceID, err)
		writeFileTransferInstanceError(ctx, instanceID, err)
		return
	}

	headers := http.Header{}
	if rangeHeader := ctx.GetHeader("Range"); rangeHeader != "" {
		headers.Set("Range", rangeHeader)
	}
	query := url.Values{"path": []string{targetPath}}
	request := agentExecutorHTTPRequest{
		method: http.MethodGet, path: "/v1/files/download", query: query, headers: headers,
	}
	if err := forwardAgentExecutorHTTP(ctx, instanceID, tenantID, request); err != nil {
		writeFileTransferError(ctx, err)
	}
}

// forwardAgentExecutorHTTP adapts the public Agent file API to the Executor's
// internal HTTP API while reusing the same authenticated TCP tunnel as SSH,
// WebSocket, and generic HTTP passthrough.
func forwardAgentExecutorHTTP(ctx *gin.Context, instanceID, tenantID string,
	executorRequest agentExecutorHTTPRequest) error {
	tunnelRequest := ctx.Request.Clone(ctx.Request.Context())
	tunnelURL := *ctx.Request.URL
	tunnelRequest.URL = &tunnelURL
	routeQuery := tunnelRequest.URL.Query()
	routeQuery.Set("instance", instanceID)
	routeQuery.Set("port", strconv.Itoa(agentExecutorHTTPPort))
	if routeQuery.Get("tenant_id") == "" {
		routeQuery.Set("tenant_id", tenantID)
	}
	tunnelRequest.URL.RawQuery = routeQuery.Encode()
	tunnel, ok := dialAgentSandboxTunnel(ctx.Writer, tunnelRequest)
	if !ok {
		return nil
	}
	defer tunnel.Close()

	targetURL := "http://agent-executor" + executorRequest.path
	if encoded := executorRequest.query.Encode(); encoded != "" {
		targetURL += "?" + encoded
	}
	request, err := http.NewRequestWithContext(
		ctx.Request.Context(), executorRequest.method, targetURL, executorRequest.body)
	if err != nil {
		return fmt.Errorf("build executor request: %w", err)
	}
	request.ContentLength = executorRequest.contentLength
	request.Header = executorRequest.headers.Clone()
	if traceID := ctx.GetHeader("X-Trace-ID"); traceID != "" {
		request.Header.Set("X-Trace-ID", traceID)
	}
	if err := request.Write(tunnel); err != nil {
		return fmt.Errorf("write executor request: %w", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(tunnel), request)
	if err != nil {
		return fmt.Errorf("read executor response: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		return writeAgentExecutorErrorResponse(ctx, response)
	}
	copyResponseHeaders(ctx.Writer.Header(), response.Header)
	ctx.Status(response.StatusCode)
	if _, err := io.Copy(ctx.Writer, response.Body); err != nil {
		return fmt.Errorf("stream executor response: %w", err)
	}
	return nil
}

// writeAgentExecutorErrorResponse preserves the public Agent file API error
// envelope while keeping the Executor HTTP API internal. Executor errors use a
// small {"message":"..."} body; Frontend adds the HTTP status as "code", as
// the previous file-transfer implementation did.
func writeAgentExecutorErrorResponse(ctx *gin.Context, response *http.Response) error {
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read executor error response: %w", err)
	}
	var executorError struct {
		Message string `json:"message"`
	}
	message := ""
	if err := json.Unmarshal(data, &executorError); err == nil {
		message = strings.TrimSpace(executorError.Message)
	}
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	copyResponseHeaders(ctx.Writer.Header(), response.Header)
	ctx.Writer.Header().Del("Content-Length")
	ctx.Writer.Header().Del("Content-Type")
	ctx.JSON(response.StatusCode, gin.H{
		"code":    response.StatusCode,
		"message": message,
	})
	return nil
}

func copyResponseHeaders(destination, source http.Header) {
	for key, values := range source {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

// --- agent instance invocation (POST /api/agent/:instanceId/invoke) ---

// InvokeHandler drives one synchronous execution of an already-created agent instance.
// It looks up the instance + executor funcKey from the execendpoint cache, builds the
// faasCallHandler args contract ([traceID, CallReqJSON]), calls the raw gRPC invoke
// channel (util.InvokeInstanceRawWithContext), and decodes the returned NotifyRequest
// into the user handler's return value. Mirrors sandbox.InvokeV1Handler's raw path but
// uses the FaaS args contract instead of the sandbox single-Arg envelope.
func InvokeHandler(ctx *gin.Context) {
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	summaries := lookupAgentInstanceSummaries("", instanceID)
	if len(summaries) == 0 {
		ctx.JSON(http.StatusNotFound,
			gin.H{"code": http.StatusNotFound, "message": fmt.Sprintf("instance not found: %s", instanceID)})
		return
	}
	funcKey := summaries[0].Function
	traceID := httputil.InitTraceID(ctx)

	callReqJSON, err := buildAgentCallReqJSON(ctx, traceID)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("failed to build invoke request: %v", err))
		return
	}
	parent := ctx.Request.Context()
	if agentInvokeTimeout > 0 {
		var cancel context.CancelFunc
		parent, cancel = context.WithTimeout(parent, agentInvokeTimeout)
		defer cancel()
	}
	// Payloads are [traceID, callReqJSON]; InvokeAgentInstanceRaw prepends the MetaData
	// protobuf (args[0]) and 16-byte META_PREFIX on each VALUE arg so the FaaS executor
	// (non-Posix) ParseMetaData succeeds -- mirroring the kernel FaaS invoke arg layout
	// produced by convertSimpleRuntimeInvokeArgsForRPC. See faas_executor.py faasCallHandler
	// (_INDEX_META_DATA=0, _INDEX_CALL_USER_EVENT=1).
	respRaw, err := util.InvokeAgentInstanceRaw(parent, util.NewClient(), funcKey, instanceID, traceID,
		[][]byte{[]byte(traceID), callReqJSON}, api.RawRequestOption{TraceParent: ctx.GetHeader(constant.HeaderTraceParent)})
	if err != nil {
		if errInfo, ok := err.(api.ErrorInfo); ok {
			app.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
				fmt.Errorf("invoke failed: code=%d err=%s", errInfo.Code, errInfo.Err))
			return
		}
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError, fmt.Errorf("invoke failed: %v", err))
		return
	}
	result, err := parseAgentInvokeResponse(respRaw)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "data": result})
}

// buildAgentCallReqJSON mirrors httputil.TranslateInvokeMsgToCallReq's CallReq shape
// (body + header + path + method + query) but reads directly from gin.Context instead of
// InvokeProcessContext. faas_call_handler (faas_executor.py:123) parses args[1] as a
// CallReq and takes event from its "body" field; header feeds trace/context into the
// user handler. Minimal header set (X-Trace-Id); callers needing more can extend.
func buildAgentCallReqJSON(ctx *gin.Context, traceID string) ([]byte, error) {
	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	if len(body) == 0 {
		body = []byte("{}")
	}
	header := map[string]string{"X-Trace-Id": traceID}
	for _, h := range []string{"X-Request-Id", "X-Invoke-Alias"} {
		if v := ctx.GetHeader(h); v != "" {
			header[h] = v
		}
	}
	req := map[string]interface{}{
		"body":   json.RawMessage(body),
		"header": header,
		"path":   ctx.Request.URL.Path,
		"method": ctx.Request.Method,
		"query":  ctx.Request.URL.RawQuery,
	}
	return json.Marshal(req)
}

// parseAgentInvokeResponse decodes the []byte returned by InvokeInstanceRawWithContext
// into the user handler's return value. The bytes are a runtime.NotifyRequest protobuf
// (the client bridges the gRPC InvokeInstanceResponse.callResult into NotifyRequest via
// marshalRuntimeNotifyFromCallResultWithRequestID). code!=0 means the runtime reported
// an execution error; smallObjects[0].value holds the FaaS call result.
//
// The FaaS executor encodes its return as META_PREFIX(16 bytes of '0') +
// transform_call_response_to_str JSON (faas_executor.py:289-322): {"body":<user return>,
// "innerCode":"0", "traceId":..., "billingDuration":..., "logResult":..., "invokerSummary":...}.
// The user handler's return value is the "body" field. This is NOT the libruntime inline-value
// (msgpack+length-header) format that sandbox/posix returns, so decodeAgentYRValue (which
// assumes that format and decodes the '0' prefix bytes as msgpack fixint 48) must not be used
// here. Strip the 16-byte META_PREFIX, JSON-unmarshal, and return the "body" field.
func parseAgentInvokeResponse(raw []byte) (interface{}, error) {
	var notify runtime.NotifyRequest
	if err := proto.Unmarshal(raw, &notify); err != nil {
		return nil, fmt.Errorf("failed to unmarshal agent invoke response: %w", err)
	}
	code := int(notify.GetCode())
	if code != 0 {
		message := notify.GetMessage()
		if message == "" {
			message = fmt.Sprintf("agent invoke failed with code %d", code)
		}
		return nil, api.ErrorInfo{Code: code, Err: errors.New(message)}
	}
	if len(notify.GetSmallObjects()) == 0 {
		return nil, fmt.Errorf("agent invoke response contains no result")
	}
	smallVal := notify.GetSmallObjects()[0].GetValue()
	// Strip the 16-byte FaaS META_PREFIX (ASCII '0' x16) that wraps the JSON result.
	payload := smallVal
	if len(payload) >= constant.LibruntimeHeaderSize {
		payload = payload[constant.LibruntimeHeaderSize:]
	}
	var resp struct {
		Body        interface{} `json:"body"`
		InnerCode   string      `json:"innerCode,omitempty"`
		TraceID     string      `json:"traceId,omitempty"`
		BillingDur  string      `json:"billingDuration,omitempty"`
		LogResult   string      `json:"logResult,omitempty"`
		InvokerSum  string      `json:"invokerSummary,omitempty"`
	}
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal faas call result JSON: %w (payload head=%x)", err,
		    smallVal[:min(constant.LibruntimeHeaderSize, len(smallVal))])
	}
	if resp.InnerCode != "" && resp.InnerCode != "0" {
		return nil, api.ErrorInfo{Code: atoiSafe(resp.InnerCode), Err: fmt.Errorf("faas inner error code %s", resp.InnerCode)}
	}
	return resp.Body, nil
}

func atoiSafe(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// agentInvokeTimeout caps a single invoke call. Agent handlers are user code so the
// default mirrors agent create timeout; can be tuned via flag if needed.
const agentInvokeTimeout = 0 // 0 = no client-side deadline; runtime/proxy enforce theirs
