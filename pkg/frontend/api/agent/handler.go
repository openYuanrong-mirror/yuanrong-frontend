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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/constants"
	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
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

const (
	defaultAgentCPU              = 1000
	defaultAgentMemory           = 2048
	agentCreateTimeoutSeconds    = 60
	agentInitTimeoutSeconds      = 305
	agentGracefulShutdownSeconds = 0
	agentDirectoryQuotaMB        = 512
	agentInstanceType            = "reserved"
	agentDelegateDirectory       = "/tmp"
	agentConcurrency             = "1"
	agentKillInstanceSignal      = constant.KillSignalVal
	agentRunningPollTimeout      = 5 * time.Second
	agentRunningPollInterval     = 200 * time.Millisecond
	agentCreateTimeoutCode       = 3002
	sshEnableEnv                 = "YR_FRONTEND_SSH_ENABLE"
	sshPublicKeyDirectoryEnv     = "YR_SSH_BACKEND_PUBLIC_KEY_DIR"
	sshContainerMountDirectory   = "/run/openyuanrong/ssh"
)

// getAgentExecutorFuncKey maps the user function runtime to the faas system executor function
// funcKey (mirrors functionscaler.instancepool.getExecutorFuncKey). agent reuses the faas
// executor function so function_proxy's funcMetaMap_ has it (loaded at startup) — avoids
// "invalid function (1015)" since agent does not register its user funcKey into proxy's
// /yr/functions watch. Falls back to PosixCustom for unknown runtimes.
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
type RuntimeSpec struct {
	Runtime     string      `json:"runtime,omitempty"`
	SandboxType string      `json:"sandbox_type,omitempty"`
	Rootfs      *RootfsSpec `json:"rootfs,omitempty"`
	Cmds        [][]string  `json:"cmds,omitempty"`
	CPU         int         `json:"cpu,omitempty"`
	Memory      int         `json:"memory,omitempty"`
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
	funcMeta := api.FunctionMeta{
		FuncID:    getAgentExecutorFuncKey(runtime),
		Language:  api.Python,
		Api:       api.ActorApi,
		Namespace: &req.Namespace,
	}
	invokeOpts, err := buildAgentInvokeOptions(ctx, req, funcKey, inline)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	var spec *types.FuncSpec
	if !inline {
		if loaded, ok := functionmeta.LoadFuncSpec(funcKey); ok && loaded != nil {
			spec = loaded
		}
	}
	resKey := resspeckey.ResourceSpecification{
		CPU:         int64(invokeOpts.Cpu),
		Memory:      int64(invokeOpts.Memory),
		InvokeLabel: "",
	}
	args := buildAgentCreateArgs(spec, resKey)
	createAgentInstance(ctx, req, funcMeta, invokeOpts, args)
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
func buildAgentInvokeOptions(ctx *gin.Context, req CreateAgentRequest, funcKey string, inline bool,
) (api.InvokeOptions, error) {
	cpu, memory := defaultAgentCPU, defaultAgentMemory
	if inline && req.RuntimeSpec != nil {
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
		Timeout:          agentCreateTimeoutSeconds,
		CreateOpt:        map[string]string{},
		CustomExtensions: map[string]string{"lifecycle": "detached", "Concurrency": agentConcurrency},
	}

	if err := validateRootfsImageURL(req, funcKey, inline); err != nil {
		return api.InvokeOptions{}, err
	}

	if err := applyAgentRootfsMounts(&invokeOpts, req); err != nil {
		return api.InvokeOptions{}, err
	}
	applyAgentDynamicEnv(&invokeOpts, req)
	if inline {
		applyAgentInlineMeta(&invokeOpts, req)
	} else {
		applyAgentFuncMeta(&invokeOpts, funcKey)
		if spec, ok := functionmeta.LoadFuncSpec(funcKey); ok && spec != nil {
			applyAgentCodePaths(&invokeOpts, spec)
		}
	}
	applyAgentCreateOpts(&invokeOpts, ctx, req, inline, funcKey)
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

// applyAgentCodePath sinks the user function codePath for local-storage registered functions
// into createOptions["DELEGATE_DOWNLOAD"] as a LocalMetaData JSON. Mirrors function-scaler's
// instance_operation_kernel.go: prepareDelegateDownload for storageType=="local". When the
// storageType is not local or codePath is empty, no DELEGATE_DOWNLOAD is written (the agent
// fallback is to skip code loading — the container starts but cannot run user code). The
// function_agent's LocalDeployer parses this and returns deployDir without downloading.
func applyAgentCodePath(invokeOpts *api.InvokeOptions, spec *types.FuncSpec) {
	if spec == nil {
		log.GetLogger().Infof("[AgentCodePath] spec is nil, skip")
		return
	}
	log.GetLogger().Infof("[AgentCodePath] storageType=%s, codePath=%s, sandboxType=%s",
		spec.CodeMetaData.StorageType, spec.CodeMetaData.CodePath, spec.SandboxType)
	if spec.CodeMetaData.StorageType != constants.LocalStorageType {
		return
	}
	if spec.CodeMetaData.CodePath == "" {
		return
	}
	delegateDownloadValue := types.LocalMetaData{
		StorageType: constants.LocalStorageType,
		CodePath:    spec.CodeMetaData.CodePath,
	}
	data, err := json.Marshal(delegateDownloadValue)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal agent delegate download: %v", err)
		return
	}
	log.GetLogger().Infof("[AgentCodePath] set DELEGATE_DOWNLOAD=%s", string(data))
	invokeOpts.CreateOpt[constant.DelegateDownloadKey] = string(data)
}

// applyAgentCodePaths builds invokeOpts.CodePaths from the spec's init/handler/preStop entries.
// Mirrors function-scaler instance_operation_kernel.go:309-312. The executor process reads
// CodePaths to know which entry symbols to load and run for each lifecycle hook.
func applyAgentCodePaths(invokeOpts *api.InvokeOptions, spec *types.FuncSpec) {
	if spec == nil {
		return
	}
	codeEntrys := []string{
		spec.ExtendedMetaData.Initializer.Handler,
		spec.FuncMetaData.Handler,
	}
	if spec.ExtendedMetaData.PreStop.Handler != "" {
		codeEntrys = append(codeEntrys, spec.ExtendedMetaData.PreStop.Handler)
	}
	invokeOpts.CodePaths = codeEntrys
}

// buildAgentCreateArgs builds the libruntime CreateInstance args array for the agent create
// path. Mirrors function-scaler instance_operation_kernel.go:1170-1199, 1202-1235 for the
// non-HTTP / non-CustomContainer local-codePath path. It returns four args: funcSpecData is an
// empty JSON object because agent has no funcSpec payload (the executor is the system
// faasExecutor function, and user-code config travels via createOptions); createParamsData
// carries instanceLabel plus the userInitEntry and userCallEntry; schedulerData is an empty JSON
// object because agent instances are not scheduler-managed; createEvent is an empty JSON object
// because agent has no event payload.
//
// When spec is nil (inline mode or uncached registered funcKey), createParamsData falls back to
// empty init/call entries. resKey mirrors the ResourceSpecification built by
// buildAgentResourceSpecJSON (same CPU/Memory source, InvokeLabel empty since agent create does
// not set one).
func buildAgentCreateArgs(spec *types.FuncSpec, resKey resspeckey.ResourceSpecification) []api.Arg {
	userInitEntry := ""
	userCallEntry := ""
	if spec != nil {
		userInitEntry = spec.ExtendedMetaData.Initializer.Handler
		userCallEntry = spec.FuncMetaData.Handler
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
		{Type: api.Value, Data: []byte("{}")},
		{Type: api.Value, Data: createParamsData},
		{Type: api.Value, Data: []byte("{}")},
		{Type: api.Value, Data: []byte("{}")},
	}
}

// applyAgentInlineMeta passes container config from req.RuntimeSpec into createOptions (inline mode).
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

// applyAgentRootfsMounts builds rootfs.mounts from the workspace and custom mounts,
// writes it into CreateOpt["rootfs"]. rootfs goes into CreateOpt (not CustomExtensions)
// so docker executor's BuildBindMounts (which reads deployOptions["rootfs"]) can parse it.
func applyAgentRootfsMounts(invokeOpts *api.InvokeOptions, req CreateAgentRequest) error {
	var rootfsMounts []map[string]interface{}
	if req.Workspace != "" {
		if err := validateBindSource(req.Workspace, "workspace"); err != nil {
			return err
		}
		rootfsMounts = append(rootfsMounts, map[string]interface{}{
			"source": req.Workspace, "target": "/home/" + agentUserPlaceholder, "readonly": false,
		})
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
	inline bool, funcKey string) {
	tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	invokeOpts.CreateOpt["tenantId"] = tenantID
	if inline {
		invokeOpts.CreateOpt[constant.FunctionKeyNote] = funcKey
	} else {
		invokeOpts.CreateOpt[constant.FunctionKeyNote] = req.Urn
	}
	invokeOpts.CreateOpt[constant.InstanceTypeNote] = agentInstanceType
	invokeOpts.CreateOpt[constant.SchedulerManagedNote] = strconv.FormatBool(false)
	invokeOpts.CreateOpt["call_timeout"] = strconv.Itoa(agentCreateTimeoutSeconds)
	invokeOpts.CreateOpt["init_call_timeout"] = strconv.Itoa(agentInitTimeoutSeconds)
	invokeOpts.CreateOpt["GRACEFUL_SHUTDOWN_TIME"] = strconv.Itoa(agentGracefulShutdownSeconds)
	invokeOpts.CreateOpt["DELEGATE_DIRECTORY_INFO"] = agentDelegateDirectory
	invokeOpts.CreateOpt["DELEGATE_DIRECTORY_QUOTA"] = strconv.Itoa(agentDirectoryQuotaMB)
	invokeOpts.CreateOpt["ConcurrentNum"] = agentConcurrency
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
	HostUser    string             `json:"host_user,omitempty"`
	Ports       []string           `json:"ports,omitempty"`
	EnvVars     map[string]string  `json:"env_vars,omitempty"`
	Resources   map[string]float64 `json:"resources,omitempty"`
	StartTime   string             `json:"start_time,omitempty"`
}

// RootfsInfo mirrors createOptions["rootfs"] (image identity + nested bind mounts).
type RootfsInfo struct {
	Type     string      `json:"type,omitempty"`
	ImageURL string      `json:"imageurl,omitempty"`
	Mounts   []MountInfo `json:"mounts,omitempty"`
}

// MountInfo is one bind mount from rootfs.mounts.
type MountInfo struct {
	Source   string `json:"source,omitempty"`
	Target   string `json:"target,omitempty"`
	ReadOnly bool   `json:"readonly,omitempty"`
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
		b.SandboxIP = resolveSandboxIP(s.ContainerIP, s.SandboxType, b.NodeIP)
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
	d.SandboxIP = resolveSandboxIP(s.ContainerIP, s.SandboxType, d.NodeIP)
	applyDetailCreateOptions(&d, s.CreateOptions)
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK, "instance": d})
}

// resolveSandboxIP returns the container/sandbox internal IP. docker's comes from
// etcd ContainerIP (inspect); supervisor is host-networked, so it falls back to nodeIP.
func resolveSandboxIP(containerIP, sandboxType, nodeIP string) string {
	if containerIP != "" {
		return containerIP
	}
	if sandboxType == agentSandboxTypeSupervisor {
		return nodeIP
	}
	return ""
}

// flattenResources collapses the scalar-resource map (e.g. {"CPU":{"scalar":{"value":600,"limit":0}}})
// to {"CPU":600}, exposing only the scalar value. limit is dropped.
func flattenResources(in map[string]execendpoint.Resource) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for name, r := range in {
		out[name] = r.Scalar.Value
	}
	return out
}

// applyDetailCreateOptions parses rootfs/host_user/network/env_vars from createOptions into the detail.
// Malformed JSON is logged and skipped so a bad value never blanks the whole response.
func applyDetailCreateOptions(d *InstanceDetail, opts map[string]string) {
	if len(opts) == 0 {
		return
	}
	d.HostUser = opts["host_user"]
	d.Rootfs = parseRootfs(opts["rootfs"], d.InstanceID)
	d.Ports = parsePorts(opts["network"])
	d.EnvVars = parseEnvVars(opts["DELEGATE_ENV_VAR"])
}

// parseRootfs decodes createOptions["rootfs"] into RootfsInfo, tolerating per-mount
// JSON errors by skipping the bad mount. Returns nil when the field is empty or invalid.
func parseRootfs(rootfsStr, instanceID string) *RootfsInfo {
	if rootfsStr == "" {
		return nil
	}
	var rf rootfsJSON
	if err := json.Unmarshal([]byte(rootfsStr), &rf); err != nil {
		log.GetLogger().Warnf("agent get: failed to unmarshal rootfs for %s: %v", instanceID, err)
		return nil
	}
	info := &RootfsInfo{Type: rf.Type, ImageURL: rf.ImageURL}
	info.Mounts = make([]MountInfo, 0, len(rf.Mounts))
	for _, raw := range rf.Mounts {
		var m MountInfo
		if err := json.Unmarshal(raw, &m); err == nil {
			info.Mounts = append(info.Mounts, m)
		}
	}
	if info.Type != "" || info.ImageURL != "" || len(info.Mounts) > 0 {
		return info
	}
	return nil
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

// maxFileUploadSize is the per-request upload size cap (512MB). It is enforced
// both via Content-Length and by accumulating bytes read from the multipart
// file so a client lying about Content-Length cannot bypass it.
const maxFileUploadSize int64 = 512 * 1024 * 1024

// fileTransferWaitTimeout bounds how long the file-transfer handlers wait for
// an instance to appear in the watcher cache before returning 404.
const fileTransferWaitTimeout = 5 * time.Second

// waitForAgentInstanceExist returns the instance spec for instanceID if it is
// present in the watcher cache within fileTransferWaitTimeout. It reuses the
// same WaitInstanceByID primitive the rest of the lifecycle path relies on.
func waitForAgentInstanceExist(instanceID string) (*types.InstanceSpecification, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fileTransferWaitTimeout)
	defer cancel()
	return instancemanager.WaitInstanceByID(ctx, instanceID)
}

// fileTransferClient returns the fixed direct-proxy file transfer client.
func fileTransferClient() (util.FileTransferClient, error) {
	return util.GetDirectProxyClient(), nil
}

// isFileNotFoundError reports whether err indicates the file/path does not
// exist on the owning proxy. It delegates to util.IsFileTransferNotFoundError,
// which inspects both the grpc status code (NotFound) and the frontend proxy
// business/status error envelopes so download handlers can map the failure to
// 404 without depending on the unexported error types.
func isFileNotFoundError(err error) bool {
	return util.IsFileTransferNotFoundError(err)
}

// FileUploadHandler handles POST /api/agent/:instanceId/files/upload.
// It streams the uploaded file to the owning proxy of the instance, validating
// the file extension whitelist and the 512MB size cap before dispatching.
//
// The multipart form must include a "path" text field and a "file" file field.
// The "path" field should precede "file" so the target is known before the
// stream is opened; if "file" arrives before "path", the request is rejected
// with 400 to keep streaming single-pass.
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
	if contentLength := ctx.Request.ContentLength; contentLength > maxFileUploadSize {
		ctx.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"code":    http.StatusRequestEntityTooLarge,
			"message": fmt.Sprintf("upload size %d exceeds max %d", contentLength, maxFileUploadSize),
		})
		return
	}

	// Verify the instance exists and is known to the watcher cache. A missing
	// instance cannot receive files.
	if _, err := waitForAgentInstanceExist(instanceID); err != nil {
		log.GetLogger().Warnf("file upload instance not found %s: %v", instanceID, err)
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": fmt.Sprintf("instance %s not found", instanceID),
		})
		return
	}

	// Limit the multipart reader memory to the cap so a malicious payload is
	// rejected at the parse boundary instead of exhausting memory.
	ctx.Request.Body = http.MaxBytesReader(ctx.Writer, ctx.Request.Body, maxFileUploadSize)
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
			// Wrap the part so cumulative size is checked against the cap while
			// bytes are streamed straight to the owning proxy.
			countingReader := &countingReader{reader: part}
			resp, uploadErr := uploadInstanceFile(ctx, instanceID, targetPath, countingReader, tenantID)
			if uploadErr != nil {
				log.GetLogger().Errorf("file upload failed instance %s path %s: %v",
					instanceID, targetPath, uploadErr)
				writeFileTransferError(ctx, uploadErr)
				return
			}
			if resp != nil {
				ctx.JSON(http.StatusOK, gin.H{
					"success": true,
					"path":    resp.GetPath(),
					"size":    resp.GetSize(),
				})
				return
			}
			ctx.JSON(http.StatusOK, gin.H{
				"success": true,
				"path":    targetPath,
				"size":    countingReader.count,
			})
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

// uploadInstanceFile resolves the file transfer client and dispatches the
// upload. It centralizes the client resolution so the handler stays focused
// on HTTP concerns.
func uploadInstanceFile(ctx *gin.Context, instanceID, path string,
	reader io.Reader, tenantID string,
) (*frontend_proxy.FileTransferResponse, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}
	transferClient, err := fileTransferClient()
	if err != nil {
		return nil, err
	}
	return transferClient.UploadFile(ctx.Request.Context(), instanceID, path, reader, tenantID)
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
	recursive := ctx.Query("recursive") == "true"
	maxDepth, err := strconv.Atoi(ctx.Query("max_depth"))
	if err != nil || maxDepth < 0 {
		maxDepth = 0
	}
	if _, err := waitForAgentInstanceExist(instanceID); err != nil {
		log.GetLogger().Warnf("file list instance not found %s: %v", instanceID, err)
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": fmt.Sprintf("instance %s not found", instanceID),
		})
		return
	}
	transferClient, err := fileTransferClient()
	if err != nil {
		log.GetLogger().Errorf("file list client unavailable instance %s: %v", instanceID, err)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{
			"code":    http.StatusServiceUnavailable,
			"message": err.Error(),
		})
		return
	}
	resp, err := transferClient.ListFile(ctx.Request.Context(), instanceID, targetPath, recursive, maxDepth, tenantID)
	if err != nil {
		writeFileTransferError(ctx, err)
		return
	}
	if resp == nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code":    http.StatusInternalServerError,
			"message": "file list response is nil",
		})
		return
	}
	if resp.GetStatus().GetCode() != common.ErrorCode_ERR_NONE {
		writeFileTransferError(ctx, fmt.Errorf("list failed: %s", resp.GetStatus().GetMessage()))
		return
	}
	ctx.Header("Content-Type", "application/json")
	ctx.String(http.StatusOK, resp.GetItemsJson())
}

// writeFileTransferError maps a file transfer error to the most appropriate
// HTTP status. Business/instance-not-found errors map to 404, path-not-found
// errors map to 404, size overflow to 413, invalid input to 400, everything
// else to 500.
func writeFileTransferError(ctx *gin.Context, err error) {
	if err == nil {
		return
	}
	if isFileNotFoundError(err) {
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": err.Error(),
		})
		return
	}
	if strings.Contains(err.Error(), "path not found") || strings.Contains(err.Error(), "No such file or directory") {
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": err.Error(),
		})
		return
	}
	if strings.Contains(err.Error(), "exceeds max") {
		ctx.JSON(http.StatusRequestEntityTooLarge, gin.H{
			"code":    http.StatusRequestEntityTooLarge,
			"message": err.Error(),
		})
		return
	}
	if strings.Contains(err.Error(), "is required") {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"code":    http.StatusBadRequest,
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
// It streams the file from the owning proxy to the HTTP response, honoring the
// Range header for partial downloads. The file is never buffered in memory in
// full; each gRPC chunk is flushed to the response writer.
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
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": fmt.Sprintf("instance %s not found", instanceID),
		})
		return
	}

	// Parse the Range header for byte-offset downloads. Only a single byte
	// range of the form "bytes=<start>-" is supported, mirroring the proxy's
	// offset-based download contract.
	var offset int64
	hasRange := false
	if rangeHeader := ctx.GetHeader("Range"); rangeHeader != "" {
		if parsed, ok := parseSingleRange(rangeHeader); ok {
			offset = parsed
			hasRange = true
		} else {
			ctx.Header("Content-Range", "bytes=*/0")
			ctx.JSON(http.StatusRequestedRangeNotSatisfiable, gin.H{
				"code":    http.StatusRequestedRangeNotSatisfiable,
				"message": "unsupported range request",
			})
			return
		}
	}

	transferClient, err := fileTransferClient()
	if err != nil {
		log.GetLogger().Errorf("file download client unavailable instance %s: %v", instanceID, err)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{
			"code":    http.StatusServiceUnavailable,
			"message": err.Error(),
		})
		return
	}

	stream, err := transferClient.DownloadFile(ctx.Request.Context(), instanceID, targetPath, offset, tenantID)
	if err != nil {
		log.GetLogger().Errorf("file download open stream failed instance %s path %s: %v",
			instanceID, targetPath, err)
		writeFileTransferError(ctx, err)
		return
	}
	// Server-streaming clients (grpc.ServerStreamingClient) have no Close
	// method; the underlying connection is managed by the pooled gRPC client.
	// Reading until io.EOF signals the end of the stream.

	// Set response headers before the first byte is written. Once Write is
	// called the status defaults to 200, so for Range requests we emit the
	// 206 status up front; the proxy does not pre-declare total size, so the
	// Content-Range uses the unknown-length form "bytes <start>-*".
	ctx.Writer.Header().Set("Content-Type", "application/octet-stream")
	ctx.Writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`,
		filepath.Base(targetPath)))
	ctx.Writer.Header().Set("Accept-Ranges", "bytes")
	if hasRange {
		ctx.Writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-*/*", offset))
		ctx.Writer.WriteHeader(http.StatusPartialContent)
	}

	// Stream each FileChunk directly to the response writer, flushing after
	// every chunk so large files do not accumulate in memory or buffers.
	var bytesWritten int64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if bytesWritten == 0 && !hasRange {
				// No bytes have been written to the client yet; we can still
				// return a clean status code. A not-found error from the proxy
				// before any data was sent maps to 404.
				writeFileTransferError(ctx, err)
				return
			}
			// The response is already committed (status/headers sent). Log and
			// abort the connection rather than sending a misleading status.
			log.GetLogger().Errorf("file download stream interrupted instance %s path %s after %d bytes: %v",
				instanceID, targetPath, bytesWritten, err)
			return
		}
		if chunk == nil {
			continue
		}
		data := chunk.GetData()
		if len(data) == 0 {
			continue
		}
		n, err := ctx.Writer.Write(data)
		bytesWritten += int64(n)
		if err != nil {
			log.GetLogger().Warnf("file download client write failed instance %s path %s: %v",
				instanceID, targetPath, err)
			return
		}
		ctx.Writer.Flush()
	}

	// If nothing was streamed at all and the status is still uncommitted, the
	// path did not exist on the proxy but the stream opened and closed cleanly
	// (Recv returned EOF immediately). Surface that as 404 so clients can
	// distinguish empty/missing files. For ranged requests the 206 status was
	// already committed before streaming, so we cannot rewrite it to 404.
	if bytesWritten == 0 && !hasRange {
		ctx.JSON(http.StatusNotFound, gin.H{
			"code":    http.StatusNotFound,
			"message": fmt.Sprintf("file %s not found in instance %s", targetPath, instanceID),
		})
		return
	}
}

// parseSingleRange parses a Range header of the form "bytes=<start>-" and
// returns the start offset. Only single-range open-ended requests are
// supported because the underlying proxy download is offset-based.
func parseSingleRange(rangeHeader string) (int64, bool) {
	const (
		prefix       = "bytes="
		decimalBase  = 10
		int64BitSize = 64
	)
	trimmed := strings.ToLower(strings.TrimSpace(rangeHeader))
	if !strings.HasPrefix(trimmed, prefix) {
		return 0, false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	dash := strings.Index(rest, "-")
	if dash < 0 {
		return 0, false
	}
	startStr := strings.TrimSpace(rest[:dash])
	if startStr == "" {
		// Suffix range "bytes=-N" is not supported by the offset-only proxy.
		return 0, false
	}
	start, err := strconv.ParseInt(startStr, decimalBase, int64BitSize)
	if err != nil || start < 0 {
		return 0, false
	}
	return start, true
}
