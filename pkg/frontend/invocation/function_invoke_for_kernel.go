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

// Package invocation -
package invocation

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/resspeckey"
	"frontend/pkg/common/faas_common/snerror"
	"frontend/pkg/common/faas_common/statuscode"
	commontype "frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/common/uuid"
	"frontend/pkg/frontend/common/httpconstant"
	"frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/leaseadaptor"
	"frontend/pkg/frontend/responsehandler"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/types"
	"frontend/pkg/frontend/upgradecompatible"
	"frontend/pkg/frontend/wisecloud"
)

var (
	// 参考libruntime写法, runtime\src\libruntime\invokeadaptor\task_submitter.cpp NeedRetry
	needRetryCodeMap = map[int]struct{}{
		statuscode.ErrInstanceNotFound:    {}, // 1003
		statuscode.ErrInstanceExitedCode:  {}, // 1007
		statuscode.ErrInstanceCircuitCode: {}, // 1009
		statuscode.ErrInstanceEvicted:     {}, // 1013

		statuscode.ErrRequestBetweenRuntimeBusCode:      {}, // 3001
		statuscode.ErrInnerCommunication:                {}, // 3002
		statuscode.ErrRequestBetweenRuntimeFrontendCode: {}, // 3008

		statuscode.ErrSharedMemoryLimited:   {}, // 4202
		statuscode.ErrOperateDiskFailed:     {}, // 4203
		statuscode.ErrInsufficientDiskSpace: {}, // 4204
		statuscode.ErrFinalized:             {}, // 9000
	}

	instanceFatalCodeMap = map[int]struct{}{
		statuscode.ErrInstanceNotFound:      {}, // 1003
		statuscode.ErrInstanceExitedCode:    {}, // 1007
		statuscode.ErrInstanceEvicted:       {}, // 1013
		statuscode.ErrUserFunctionException: {}, // 2002
	}
)

func computeTimeout(originTimeout int64, beginTime time.Time) int64 {
	costTime := time.Now().Sub(beginTime)
	costTimeSecond := int64(math.Trunc(costTime.Seconds()))
	return originTimeout - costTimeSecond
}

type kernelRequestHandler struct {
	ctx                    *types.InvokeProcessContext
	funcSpec               *commontype.FuncSpec
	funcKey                string
	resSpecKeyStr          string
	resSpecKey             *resspeckey.ResSpecKey
	logger                 api.FormatLogger
	instanceAllocationInfo *commontype.InstanceAllocationInfo

	startTime time.Time
	downgrade bool
	timeout   int64

	unexpectedInstances []string

	legacyCurrentSchedulerInfo *commontype.InstanceInfo

	ringName string
	grayNum  uint32
	session  *commontype.InstanceSessionConfig
}

func createKernelRequestHandler(ctx *types.InvokeProcessContext, funcSpec *commontype.FuncSpec) (
	*kernelRequestHandler, error) {
	if ctx.InvokeTimeout == 0 {
		ctx.InvokeTimeout = funcSpec.FuncMetaData.Timeout
	}
	resSpecKey := convertResSpecKey(ctx, funcSpec)
	session := &commontype.InstanceSessionConfig{}
	instanceSession := ctx.ReqHeader[httpconstant.HeaderInstanceSession]
	if instanceSession != "" {
		if err := json.Unmarshal([]byte(instanceSession), session); err != nil {
			return nil, fmt.Errorf("unmarshal instance session failed, err: %s", err.Error())
		}
	}
	ringName, grayNum := getRingName(ctx, session)
	ctx.RingName = ringName
	return &kernelRequestHandler{
		ctx:           ctx,
		funcSpec:      funcSpec,
		funcKey:       funcSpec.FunctionKey,
		resSpecKey:    &resSpecKey,
		resSpecKeyStr: resSpecKey.String(),
		logger: log.GetLogger().With(zap.Any("traceId", ctx.TraceID), zap.Any("function", funcSpec.FunctionKey),
			zap.Any("timeout", ctx.InvokeTimeout), zap.Any("acquireTimeout", ctx.AcquireTimeout),
			zap.Any("ringName", ringName), zap.Any("sessionId", session.SessionID)),
		startTime:           time.Now(),
		downgrade:           ctx.InvokeWithoutScheduler,
		unexpectedInstances: make([]string, 0),
		timeout:             ctx.InvokeTimeout,
		ringName:            ringName,
		grayNum:             grayNum,
		session:             session,
	}, nil
}

func newKernelRequestHandler(ctx *types.InvokeProcessContext, funcSpec *commontype.FuncSpec) *kernelRequestHandler {
	handler, err := createKernelRequestHandler(ctx, funcSpec)
	if err != nil {
		log.GetLogger().Warnf("failed to create kernel request handler, err: %s", err.Error())
		return &kernelRequestHandler{
			ctx:                 ctx,
			funcSpec:            funcSpec,
			funcKey:             funcSpec.FunctionKey,
			logger:              log.GetLogger(),
			startTime:           time.Now(),
			downgrade:           ctx.InvokeWithoutScheduler,
			unexpectedInstances: make([]string, 0),
			timeout:             ctx.InvokeTimeout,
			ringName:            constant.GreenRing,
		}
	}
	return handler
}

func (k *kernelRequestHandler) getProxyManager() *schedulerproxy.ProxyManager {
	switch k.ringName {
	case constant.BlueRing:
		return schedulerproxy.BlueProxy
	default:
		return schedulerproxy.Proxy
	}
}

func (k *kernelRequestHandler) legacyMakeReq(logger api.FormatLogger) (*util.InvokeRequest, error) {
	k.legacyCurrentSchedulerInfo = nil
	if !k.downgrade {
		option := &commontype.AcquireOption{
			EnableSessionCtx: k.funcSpec.ExtendedMetaData.EnableSessionCtx,
			SessionCtxID:     k.ctx.SessionCtxID,
			InstanceSession:  k.session,
		}
		schedulerNodeInfo, err := k.getProxyManager().GetByAcquireOption(k.funcKey, option, logger)
		if err != nil {
			logger.Warnf("failed to get scheduler, err: %s", err.Error())
		} else if schedulerNodeInfo != nil {
			k.legacyCurrentSchedulerInfo = schedulerNodeInfo.InstanceInfo
		}
	}
	var err error
	var instanceId string
	if k.downgrade || k.needDownGrade(k.legacyCurrentSchedulerInfo) {
		k.downgrade = true
		instanceId, err = k.chooseInstance(logger)
		if err != nil {
			return nil, err
		}
	}
	req, err := convert(k.ctx, k.funcSpec, instanceId, false, nil, k.legacyCurrentSchedulerInfo)
	if err != nil {
		logger.Errorf("failed to convert request, err: %s", err.Error())
		return nil, err
	}
	return req, nil
}

func (k *kernelRequestHandler) makeReqCompatible(logger api.FormatLogger) (*util.InvokeRequest, error) {
	if k.accessFaaSSchedulerWithLibRuntime() {
		// legacy
		return k.legacyMakeReq(logger)
	}
	return k.makeReq(logger)
}

func (k *kernelRequestHandler) makeReq(logger api.FormatLogger) (*util.InvokeRequest, error) {
	var instanceId string
	var forceInvoke bool
	if !k.downgrade {
		var instanceAllocationInfo *commontype.InstanceAllocationInfo
		var err snerror.SNError
		if k.ringName == constant.BlueRing {
			instanceAllocationInfo, err = leaseadaptor.GetBlueInstanceManager().AcquireInstance(k.ctx, k.funcSpec,
				k.logger)
		} else {
			instanceAllocationInfo, err = leaseadaptor.GetInstanceManager().AcquireInstance(k.ctx, k.funcSpec, k.logger)
		}
		if err != nil {
			if err.Code() == statuscode.ErrAllSchedulerUnavailable {
				k.logger.Warnf("acquire lease failed, err: %s, do downgrade", err.Error())
				k.downgrade = true
			} else {
				k.logger.Errorf("acquire lease failed, err: %s", err.Error())
				return nil, err
			}
		} else if instanceAllocationInfo != nil {
			k.instanceAllocationInfo = instanceAllocationInfo
			instanceId = instanceAllocationInfo.InstanceID
			forceInvoke = instanceAllocationInfo.ForceInvoke
		}
	}

	if k.downgrade {
		var err error
		instanceId, err = k.chooseInstance(logger)
		if err != nil {
			return nil, err
		}
	}

	req, err := convert(k.ctx, k.funcSpec, instanceId, forceInvoke, k.instanceAllocationInfo, nil)
	if err != nil {
		logger.Errorf("failed to convert request, err: %s", err.Error())
		return nil, err
	}
	return req, nil
}

func (k *kernelRequestHandler) chooseInstance(logger api.FormatLogger) (string, error) {
	instance := instancemanager.GetGlobalInstanceScheduler().GetRandomInstanceWithoutUnexpectedInstance(
		k.funcKey, k.resSpecKeyStr, k.unexpectedInstances, logger)

	if instance == nil {
		pendingRequest := &wisecloud.PendingRequest{
			CreatedTime:     time.Now(),
			ScheduleTimeout: time.Duration(k.ctx.AcquireTimeout) * time.Second,
			ResultChan:      make(chan *wisecloud.PendingResponse, 1),
		}
		wisecloud.GetQueueManager().AddPendingRequest(k.funcKey, k.resSpecKey, pendingRequest)
		pendingResponse := <-pendingRequest.ResultChan
		if pendingResponse.Error != nil {
			return "", pendingResponse.Error
		}
		if pendingResponse.Instance == nil {
			return "", fmt.Errorf("no available instance, no available scheduler")
		}
		instance = pendingResponse.Instance
	}
	return instance.InstanceID, nil
}

func (k *kernelRequestHandler) accessFaaSSchedulerWithLibRuntime() bool {
	accessType := upgradecompatible.GetAccessFaaSSchedulerType()
	k.logger.Debugf("access scheduler type: %s", accessType)
	return accessType == upgradecompatible.AccessSchedulerByLibruntime
}

func getRingName(ctx *types.InvokeProcessContext, session *commontype.InstanceSessionConfig) (string, uint32) {
	grayNum := utils.ConvertHashToPercent(ctx.RequestID)
	if session != nil && session.SessionID != "" {
		grayNum = utils.ConvertHashToPercent(session.SessionID)
	}

	if leaseadaptor.ShouldUseBlueRing(grayNum) {
		return constant.BlueRing, grayNum
	}
	return constant.GreenRing, grayNum
}

func (k *kernelRequestHandler) setRingName() error {
	if k.session == nil {
		k.session = &commontype.InstanceSessionConfig{}
		instanceSession := k.ctx.ReqHeader[httpconstant.HeaderInstanceSession]
		if instanceSession != "" {
			if err := json.Unmarshal([]byte(instanceSession), k.session); err != nil {
				return err
			}
		}
	}
	ringName, grayNum := getRingName(k.ctx, k.session)
	k.ringName = ringName
	k.grayNum = grayNum
	k.ctx.RingName = k.ringName
	return nil
}

func (k *kernelRequestHandler) getRingInstanceManager() *leaseadaptor.Manager {
	switch k.ringName {
	case constant.BlueRing:
		return leaseadaptor.GetBlueInstanceManager()
	default:
		return leaseadaptor.GetInstanceManager()
	}
}

func (k *kernelRequestHandler) invoke() error {
	defer resetSchedulerProxy(k.ctx)
	count := 0
	for {
		count++
		k.ctx.RequestID = uuid.New().String()
		k.ctx.InvokeTimeout = computeTimeout(k.ctx.InvokeTimeout, k.startTime)
		if k.ctx.InvokeTimeout <= 0 {
			return fmt.Errorf("do invoke failed, timeout")
		}

		if err := k.setRingName(); err != nil {
			k.logger.Errorf("set ring name failed: %s", err.Error())
			httputil.HandleInvokeError(k.ctx, err)
			return err
		}
		logger := k.logger.With(zap.Any("requestId", k.ctx.RequestID), zap.Any("timeLeft", k.ctx.InvokeTimeout),
			zap.Any("count", count), zap.Any("ringName", k.ringName), zap.Any("grayNum", k.grayNum))
		req, err := k.makeReqCompatible(logger)
		if err != nil {
			logger.Errorf("make req failed: %s", err.Error())
			httputil.HandleInvokeError(k.ctx, err)
			return err
		}

		if k.downgrade {
			wisecloud.GetMetricsManager().InvokeStart(k.funcKey, k.resSpecKeyStr, req.InstanceID)
		}

		snError := invokeFunctionWithLibRuntime(k.ctx, *req, logger)
		if k.downgrade {
			wisecloud.GetMetricsManager().InvokeEnd(k.funcKey, k.resSpecKeyStr, req.InstanceID)
		}
		if k.instanceAllocationInfo != nil {
			if snError != nil && instanceIsAbnormal(snError.Code()) {
				k.getRingInstanceManager().ReleaseInstanceAllocation(k.instanceAllocationInfo, true,
					k.ctx.TraceID)
			} else {
				k.getRingInstanceManager().ReleaseInstanceAllocation(k.instanceAllocationInfo, false,
					k.ctx.TraceID)
			}
			k.instanceAllocationInfo = nil
		}
		if snError != nil {
			retry, err := k.handleInvokeError(snError, req.InstanceID, logger)
			if err != nil {
				return err
			}
			if retry {
				continue
			}
		}
		return nil
	}
}

func (k *kernelRequestHandler) handleInvokeError(snError snerror.SNError, instanceId string, logger api.FormatLogger) (
	bool, error) {
	if snError == nil {
		return false, nil
	}
	if snError.Code() == constant.AcquireLeaseTrafficLimitErrorCode && k.legacyCurrentSchedulerInfo != nil {
		k.ctx.TrafficLimited = true
		return true, nil
	} else if snError.Code() == statuscode.FrontendStatusInternalError { // 这个分支不太理解，后续考虑优化
		logger.Errorf("failed to invoke name by client, err: %s", snError.Error())
		responsehandler.SetErrorInContext(k.ctx, statuscode.FrontendStatusInternalError, snError.Error())
		return false, snError
	} else if snError.Code() == statuscode.ErrAllSchedulerUnavailable {
		logger.Warnf("all schedulers are unavailable")
		k.downgrade = true // 这里要处理的情况是，当无可用scheduler时，该请求后续都不走租约机制，直接选择实例调用
		return true, nil
	} else if needRetryCode(snError.Code()) {
		logger.Warnf("do invokeByInstanceId failed, retry, code: %d, message: %s",
			snError.Code(), snError.Error())
		if instanceId != "" {
			k.unexpectedInstances = append(k.unexpectedInstances, instanceId)
		}
		return true, nil
	} else {
		httputil.HandleInvokeError(k.ctx, snError)
		return false, snError
	}
}

func (k *kernelRequestHandler) needDownGrade(schedulerInfo *commontype.InstanceInfo) bool {
	if schedulerInfo == nil || k.getProxyManager().IsEmpty() {
		return true
	}
	return false
}

func needRetryCode(code int) bool {
	// 暂时不考虑区分 同实例重试和不同实例重试的错误码
	_, ok := needRetryCodeMap[code]
	return ok
}

func instanceIsAbnormal(code int) bool {
	_, ok := instanceFatalCodeMap[code]
	return ok
}

func getAcquireReqCPUAndMemory(ctx *types.InvokeProcessContext, funcSpec *commontype.FuncSpec) (int64, int64) {
	cpu := funcSpec.ResourceMetaData.CPU
	memory := funcSpec.ResourceMetaData.Memory
	if ctx == nil || ctx.ReqHeader == nil {
		return cpu, memory
	}
	if cpuString := util.PeekIgnoreCase(ctx.ReqHeader, constant.HeaderCPUSize); cpuString != "" {
		cpuInt, err := strconv.Atoi(cpuString)
		if err != nil || cpuInt <= 0 {
			log.GetLogger().Warnf("invalid value %s from request header", constant.HeaderCPUSize)
		} else {
			cpu = int64(cpuInt)
		}
	}

	if memoryString := util.PeekIgnoreCase(ctx.ReqHeader, constant.HeaderMemorySize); memoryString != "" {
		memoryInt, err := strconv.Atoi(memoryString)
		if err != nil || memoryInt <= 0 {
			log.GetLogger().Warnf("invalid value %s from request header", constant.ResourceMemoryName)
		} else {
			memory = int64(memoryInt)
		}
	}
	return cpu, memory
}

func convertResSpecKey(ctx *types.InvokeProcessContext, funcSpec *commontype.FuncSpec) resspeckey.ResSpecKey {
	invokeLabel := util.PeekIgnoreCase(ctx.ReqHeader, httpconstant.HeaderInstanceLabel)
	cpu, memory := getAcquireReqCPUAndMemory(ctx, funcSpec)

	resSpec := resspeckey.ConvertResourceMetaDataToResSpec(funcSpec.ResourceMetaData)
	resSpec.InvokeLabel = invokeLabel
	resSpec.CPU = cpu
	resSpec.Memory = memory

	return resspeckey.ConvertToResSpecKey(resSpec)
}

func invokeFunctionWithLibRuntime(ctx *types.InvokeProcessContext, request util.InvokeRequest,
	logger api.FormatLogger) snerror.SNError {
	logger.Infof("send request %v to grpc", request)

	invokeStart := time.Now()
	var notifyMsg []byte
	var err error
	if request.InstanceID != "" {
		notifyMsg, err = util.NewClient().Invoke(request)
	} else {
		// legacy
		notifyMsg, err = util.NewClient().InvokeByName(request)
	}

	invokeTotalTime := time.Since(invokeStart)
	logger.Debugf("get response %s, err: %v", string(notifyMsg), err)

	if err != nil {
		if rtErr, ok := err.(api.ErrorInfo); ok {
			logger.Errorf("invoke request, errCode: %d, error: %s, totalTime: %v",
				rtErr.Code, rtErr.Error(), invokeTotalTime.Seconds())
			if snErr := checkErrorMsg(rtErr.Error()); snErr != nil {
				return snErr
			}
			return snerror.New(rtErr.Code, rtErr.Error())
		}
		if snError := checkInstanceResp(notifyMsg); snError != nil {
			return snError
		}
		logger.Errorf("invoke GRPC request error: %s, totalTime: %v", err.Error(), invokeTotalTime.Seconds())
		errMsg := fmt.Sprintf("invoke GRPC request error: %s", err.Error())
		// todo 暂时保存，后续可以考虑取消JudgeRetry
		httputil.JudgeRetry(err, ctx)
		return snerror.New(statuscode.FrontendStatusInternalError, errMsg)
	}
	respMsg, snErr := responsehandler.SetResponseInContext(ctx, notifyMsg)
	if snErr != nil {
		return snErr
	}
	if ctx.RequestTraceInfo != nil {
		ctx.RequestTraceInfo.FrontendCost = invokeTotalTime
		ctx.RequestTraceInfo.WorkerCost = httputil.GetTimeFromResp(respMsg.UserFuncTime)
	}
	logger.Infof("invoke end, totalTime: %f, executorTime: %f, userTime: %f", invokeTotalTime.Seconds(),
		httputil.GetTimeFromResp(respMsg.ExecutorTime).Seconds(), httputil.GetTimeFromResp(respMsg.UserFuncTime).Seconds())
	return nil
}

func convert(ctx *types.InvokeProcessContext, funcSpec *commontype.FuncSpec,
	instanceId string, forceInvoke bool, leaseInfo *commontype.InstanceAllocationInfo,
	legacySchedulerInfo *commontype.InstanceInfo,
) (*util.InvokeRequest, error) {
	resourceSpecs, err := util.ConvertResourceSpecs(ctx, funcSpec)
	if err != nil {
		return nil, err
	}
	req := &util.InvokeRequest{
		Function:         ctx.FuncKey,
		TraceID:          ctx.TraceID,
		TraceParent:      util.PeekIgnoreCase(ctx.ReqHeader, constant.HeaderTraceParent),
		RequestID:        ctx.RequestID,
		ReturnObjectIDs:  []string{},
		ResourceSpecs:    resourceSpecs,
		PoolLabel:        util.PeekIgnoreCase(ctx.ReqHeader, httpconstant.HeaderPoolLabel),
		InvokeTag:        convertInvokeTag(ctx),
		InstanceLabel:    util.PeekIgnoreCase(ctx.ReqHeader, httpconstant.HeaderInstanceLabel),
		AcquireTimeout:   getTimeout(util.GetAcquireTimeout(funcSpec), ctx.AcquireTimeout),
		InvokeTimeout:    ctx.InvokeTimeout,
		FuncSig:          funcSpec.FuncMetaSignature,
		TrafficLimited:   ctx.TrafficLimited,
		BusinessType:     funcSpec.FuncMetaData.BusinessType,
		TenantID:         funcSpec.FuncMetaData.TenantID,
		InstanceID:       instanceId,
		ForceInvoke:      forceInvoke,
		IsInterrupted:    ctx.IsInterrupted,
		BypassDataSystem: strings.EqualFold(util.PeekIgnoreCase(ctx.ReqHeader, httpconstant.HeaderBypassDataSystem), "true"),
	}
	if leaseInfo != nil && leaseInfo.FunctionProxyID != "" {
		req.RouteAddress = leaseInfo.FunctionProxyID
	}
	if funcSpec.ExtendedMetaData.EnableSessionCtx {
		req.SessionCtxID = ctx.SessionCtxID
	}

	// legacy
	if legacySchedulerInfo != nil {
		req.SchedulerID = legacySchedulerInfo.InstanceID
		req.SchedulerFuncKey = legacySchedulerInfo.FunctionName
	}

	instanceSession := util.PeekIgnoreCase(ctx.ReqHeader, httpconstant.HeaderInstanceSession)
	if instanceSession != "" {
		session := &commontype.InstanceSessionConfig{}
		err = json.Unmarshal([]byte(instanceSession), &session)
		if err != nil {
			return nil, err
		}
		req.InstanceSession = session
	}
	body, err := httputil.TranslateInvokeMsgToCallReq(ctx)
	if err != nil {
		responsehandler.SetErrorInContext(ctx, statuscode.FrontendStatusInternalError, err.Error())
		return req, err
	}
	req, err = convertResourceSpecs(ctx, req)
	if err != nil {
		return req, err
	}
	req.Args = newArgList([]byte(ctx.TraceID), body)
	req.AcceptHeader = util.PeekIgnoreCase(ctx.ReqHeader, "Accept")
	req.ResponseWriter = ctx.ResponseWriter
	return req, nil
}

func convertResourceSpecs(ctx *types.InvokeProcessContext, req *util.InvokeRequest) (*util.InvokeRequest, error) {
	dynamicResourceSpecs, err := prepareDynamicResource(ctx)
	if err != nil {
		responsehandler.SetErrorInContext(ctx, statuscode.FrontendStatusInternalError, err.Error())
		return req, err
	}
	if dynamicResourceSpecs[constant.ResourceCPUName] > 0 && dynamicResourceSpecs[constant.ResourceMemoryName] > 0 {
		req.ResourceSpecs = dynamicResourceSpecs
	}
	return req, nil
}

func getTimeout(funcSpecTimeout int64, ctxTimeout int64) int64 {
	if ctxTimeout != 0 {
		return ctxTimeout
	}
	return funcSpecTimeout
}

func checkErrorMsg(msg string) snerror.SNError {
	if len(msg) != 0 {
		var errInfo struct {
			ErrorCode    int    `json:"code"`
			ErrorMessage string `json:"message"`
		}
		if unMarshalErr := json.Unmarshal([]byte(msg), &errInfo); unMarshalErr != nil {
			log.GetLogger().Debugf("unmarshal notifyMsg error : %s", unMarshalErr.Error())
			return nil
		}
		if errInfo.ErrorCode != 0 && errInfo.ErrorMessage != "" {
			// current faasscheduler has reached instance limit, should retry and chose another faasscheduler
			return snerror.New(errInfo.ErrorCode, errInfo.ErrorMessage)
		}
	}
	return nil
}

func checkInstanceResp(notifyMsg []byte) snerror.SNError {
	if notifyMsg != nil && len(notifyMsg) != 0 {
		var insResponse struct {
			ErrorCode    int    `json:"errorCode"`
			ErrorMessage string `json:"errorMessage"`
		}
		if unMarshalErr := json.Unmarshal(notifyMsg, &insResponse); unMarshalErr != nil {
			log.GetLogger().Errorf("unmarshal notifyMsg error : %s", unMarshalErr.Error())
		}
		if insResponse.ErrorCode != 0 && insResponse.ErrorMessage != "" {
			// current faasscheduler has reached instance limit, should retry and chose another faasscheduler
			return snerror.New(insResponse.ErrorCode, insResponse.ErrorMessage)
		}
	}
	return nil
}

func newArgList(payloads ...[]byte) []*api.Arg {
	var result []*api.Arg
	for _, p := range payloads {
		result = append(result, &api.Arg{Type: api.Value, Data: p})
	}
	return result
}

func convertInvokeTag(ctx *types.InvokeProcessContext) map[string]string {
	m := make(map[string]string)
	headerValue, ok := ctx.ReqHeader[httpconstant.HeaderInvokeTag]
	if !ok || headerValue == "" {
		return m
	}
	err := json.Unmarshal([]byte(headerValue), &m)
	if err != nil {
		log.GetLogger().Errorf("convert invoke tag failed, traceId: %s, err: %s", ctx.TraceID, err.Error())
		return m
	}
	return m
}

func prepareDynamicResource(ctx *types.InvokeProcessContext) (map[string]int64, error) {
	dynamicResourcesRoute := make(map[string]int64)
	cpuBytes := ctx.ReqHeader[httpconstant.HeaderCPUSize]
	memoryBytes := ctx.ReqHeader[httpconstant.HeaderMemorySize]
	customResourcesString := httputil.GetCompatibleHeader(ctx.ReqHeader, constant.HeaderCustomResourceNew,
		constant.HeaderCustomResource)

	logger := log.GetLogger().With(zap.Any("traceId", ctx.TraceID), zap.Any("funcKey", ctx.FuncKey))
	if cpuBytes != "" && memoryBytes != "" {
		cpu, err := strconv.ParseInt(cpuBytes, baseTen, bitSize)
		if err != nil {
			return dynamicResourcesRoute, err
		}
		memory, err := strconv.ParseInt(memoryBytes, baseTen, bitSize)
		if err != nil {
			return dynamicResourcesRoute, err
		}
		dynamicResourcesRoute[constant.ResourceCPUName] = cpu
		dynamicResourcesRoute[constant.ResourceMemoryName] = memory
	}
	if customResourcesString != "" {
		var customResources map[string]int64
		if err := json.Unmarshal([]byte(customResourcesString), &customResources); err != nil {
			logger.Errorf("failed to unmarshal custom resources %s", err.Error())
			return dynamicResourcesRoute, err
		}
		for resourceType, resource := range customResources {
			if resource > constant.MinCustomResourcesSize {
				dynamicResourcesRoute[resourceType] = resource
			} else {
				logger.Warnf("ignore invalid value %f of custom resource %s", resource, resourceType)
			}
		}
	}
	if len(dynamicResourcesRoute) != 0 {
		logger.Infof("dynamicResourcesRoute is %v", dynamicResourcesRoute)
	}
	return dynamicResourcesRoute, nil
}
