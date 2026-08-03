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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
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
	agentDefaultWorkspaceTarget = "/workspace"
)

// agentExecutorFormat is the system executor function funcKey pattern. agent reuses the
// faas system executor function (loaded into function_proxy's funcMetaMap_ at startup from
// executor-meta/*_meta.json) as the CreateInstance FuncID so proxy's GetFuncMeta hits and
// does not return "invalid function (1015)". agent's real config (imageurl/user/ports/
// sandboxType) is passed via createOptions — proxy sinks them as-is, docker executor reads.
const agentExecutorFormat = "default/0-system-faasExecutor%s/$latest"

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
	Workspace   string            `json:"workspace" binding:"required"`
	EnvVars     map[string]string `json:"env_vars,omitempty"`
	Mounts      []Mount           `json:"mounts,omitempty"`
}

// RuntimeSpec carries inline container config (inline mode).
type RuntimeSpec struct {
	Runtime     string      `json:"runtime,omitempty"`
	SandboxType string      `json:"sandbox_type,omitempty"`
	Rootfs      *RootfsSpec `json:"rootfs,omitempty"`
	CPU         int         `json:"cpu,omitempty"`
	Memory      int         `json:"memory,omitempty"`
}

// RootfsSpec carries the inline container rootfs config.
type RootfsSpec struct {
	ImageURL string   `json:"imageurl" binding:"required"`
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

	var funcKey, executorFuncKey, runtime string
	if inline {
		tenantID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId")
		if tenantID == "" {
			tenantID = "default"
		}
		funcKey = urnutils.CombineFunctionKey(tenantID, req.Name, urnutils.DefaultURNVersion)
		runtime = req.RuntimeSpec.Runtime
	} else {
		funcUrn := &urnutils.FunctionURN{}
		if err := funcUrn.ParseFrom(req.Urn); err != nil {
			app.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("invalid urn: %v", err))
			return
		}
		funcKey = urnutils.CombineFunctionKey(funcUrn.TenantID, funcUrn.FuncName, funcUrn.FuncVersion)
		if spec, ok := functionmeta.LoadFuncSpec(funcKey); ok && spec != nil {
			runtime = spec.FuncMetaData.Runtime
		}
	}
	executorFuncKey = getAgentExecutorFuncKey(runtime)

	funcMeta := api.FunctionMeta{
		FuncID:    executorFuncKey,
		Language:  api.Python,
		Api:       api.ActorApi,
		Namespace: &req.Namespace,
	}
	invokeOpts, err := buildAgentInvokeOptions(ctx, req, funcKey, inline)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	createAgentInstance(ctx, req, funcMeta, invokeOpts)
}

// isInlineMode reports whether req carries inline container config (runtime + rootfs.imageurl).
func isInlineMode(req CreateAgentRequest) bool {
	return req.RuntimeSpec != nil && req.RuntimeSpec.Runtime != "" &&
		req.RuntimeSpec.Rootfs != nil && req.RuntimeSpec.Rootfs.ImageURL != ""
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

	if err := applyAgentRootfsMounts(&invokeOpts, req); err != nil {
		return api.InvokeOptions{}, err
	}
	applyAgentDynamicEnv(&invokeOpts, req)
	if inline {
		applyAgentInlineMeta(&invokeOpts, req)
	} else {
		applyAgentFuncMeta(&invokeOpts, funcKey)
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
	if tenantID != "" {
		invokeOpts.CreateOpt["tenantId"] = tenantID
	}
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
) {
	instanceID, err := util.NewClient().CreateInstanceByLibRt(funcMeta, []api.Arg{}, invokeOpts)
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
	if err := util.NewClient().KillByLibRt(instanceID, agentKillInstanceSignal,
		[]byte("agent deleted")); err != nil {
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
	InstanceID  string `json:"instance_id"`
	NodeIP      string `json:"node_ip,omitempty"`
	SandboxIP   string `json:"sandbox_ip,omitempty"`
	SandboxType string `json:"sandbox_type,omitempty"`
	SandboxID   string `json:"sandbox_id,omitempty"`
	Rootfs      *RootfsInfo `json:"rootfs,omitempty"`
	HostUser    string      `json:"host_user,omitempty"`
	Ports       []string    `json:"ports,omitempty"`
	EnvVars     map[string]string `json:"env_vars,omitempty"`
	Resources   map[string]float64 `json:"resources,omitempty"`
	StartTime   string `json:"start_time,omitempty"`
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
	Type     string             `json:"type"`
	ImageURL string             `json:"imageurl"`
	Mounts   []json.RawMessage  `json:"mounts"`
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
	if sandboxType == "supervisor" {
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
