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

// Package leaseadaptor -
package leaseadaptor

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/queue"
	"frontend/pkg/common/faas_common/snerror"
	"frontend/pkg/common/faas_common/statuscode"
	commontypes "frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/common/uuid"
	"frontend/pkg/frontend/common/httpconstant"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/types"
)

const (
	renewAction = "retain"

	idleHoldTime               = 100 // millisecond
	defaultAcquireLeaseTimeout = 120 // second
	defaultRetainLeaseTime     = 200
	beforeRetainTime           = 100 // millisecond
	defaultMapSize             = 16
	callSchedulerPath          = "/invoke"

	maxBatchSize = 1000
)

var (
	instanceManager     *Manager
	blueInstanceManager *Manager
	once                sync.Once
	blueOnce            sync.Once
)

// Manager manges
type Manager struct {
	globalFuncKeyLeasePools map[string]*FuncKeyLeasePools
	ringName                string
	sync.RWMutex
}

// GetInstanceManager creates Manager
func GetInstanceManager() *Manager {
	once.Do(func() {
		instanceManager = &Manager{
			globalFuncKeyLeasePools: make(map[string]*FuncKeyLeasePools, defaultMapSize),
			ringName:                constant.GreenRing,
		}
	})
	return instanceManager
}

// GetBlueInstanceManager creates blue Manager
func GetBlueInstanceManager() *Manager {
	blueOnce.Do(func() {
		blueInstanceManager = &Manager{
			globalFuncKeyLeasePools: make(map[string]*FuncKeyLeasePools, defaultMapSize),
			ringName:                constant.BlueRing,
		}
	})
	return blueInstanceManager
}

// ClearFuncLeasePools -
func (im *Manager) ClearFuncLeasePools(funcKey string) {
	logger := log.GetLogger().With(zap.Any("funcKey", funcKey))
	logger.Infof("function is delete,clean lease pools")
	im.Lock()
	defer im.Unlock()
	funckeyLeasePools, ok := im.globalFuncKeyLeasePools[funcKey]
	if !ok {
		logger.Infof("function leasePool is not exist,no need to delete")
		return
	}
	funckeyLeasePools.Lock()
	utils.SafeCloseChannel(funckeyLeasePools.stopCh)
	for _, leasePool := range funckeyLeasePools.leasePools {
		utils.SafeCloseChannel(leasePool.stopCh)
	}
	delete(im.globalFuncKeyLeasePools, funcKey)
	funckeyLeasePools.Unlock()
}

// AcquireInstance -
func (im *Manager) AcquireInstance(ctx *types.InvokeProcessContext, funcSpec *commontypes.FuncSpec,
	logger api.FormatLogger) (*commontypes.InstanceAllocationInfo, snerror.SNError) {
	if funcSpec.InstanceMetaData.MaxInstance == 0 {
		return nil, snerror.New(statuscode.ReachMaxInstancesCode, fmt.Sprintf("%s,%d",
			statuscode.ReachMaxInstancesErrMsg, funcSpec.InstanceMetaData.MaxInstance))
	}
	im.Lock()
	funcKeyLeasePools, ok := im.globalFuncKeyLeasePools[ctx.FuncKey]
	if !ok {
		funcKeyLeasePools = im.newFuncKeyLeasePools(ctx.FuncKey)
		im.globalFuncKeyLeasePools[ctx.FuncKey] = funcKeyLeasePools
	}
	im.Unlock()
	acquireOption, err := makeAcquireOption(ctx, funcSpec)
	if err != nil {
		return nil, err
	}
	return funcKeyLeasePools.acquireInstance(acquireOption)
}

// ReleaseInstanceAllocation -
func (im *Manager) ReleaseInstanceAllocation(allocation *commontypes.InstanceAllocationInfo, abnormal bool,
	traceID string) {
	if allocation == nil || allocation.ThreadID == "" {
		return
	}
	logger := log.GetLogger().With(zap.Any("funcKey", allocation.FuncKey), zap.Any("traceId", traceID),
		zap.Any("leaseId", allocation.ThreadID), zap.Any("abnormal", abnormal))
	logger.Debugf("release instance lease")
	im.RLock()
	funckeyLeasePools, ok := im.globalFuncKeyLeasePools[allocation.FuncKey]
	if !ok {
		logger.Warnf("funcKey is not in lease pools!")
		im.RUnlock()
		return
	}
	im.RUnlock()
	funckeyLeasePools.releaseInstanceLease(allocation.ThreadID, abnormal)
}

// FuncKeyLeasePools -
type FuncKeyLeasePools struct {
	leasePools map[string]*LeasePool
	sync.RWMutex
	funcKey            string
	funcSpec           *commontypes.FuncSpec
	logger             api.FormatLogger
	stopCh             chan struct{}
	globalLeaseList    map[string]*InstanceLease
	leaseIdToLeasePool map[string]*LeasePool

	interval atomic.Int64
	ringName *types.RingName
}

func (im *Manager) newFuncKeyLeasePools(funckey string) *FuncKeyLeasePools {
	funcKeyLeasePools := &FuncKeyLeasePools{
		leasePools:         make(map[string]*LeasePool),
		RWMutex:            sync.RWMutex{},
		funcKey:            funckey,
		funcSpec:           nil,
		logger:             log.GetLogger().With(zap.Any("funcKey", funckey)),
		stopCh:             make(chan struct{}),
		globalLeaseList:    make(map[string]*InstanceLease, defaultMapSize),
		leaseIdToLeasePool: make(map[string]*LeasePool, defaultMapSize),
		interval:           atomic.Int64{},
		ringName:           &types.RingName{Name: im.ringName},
	}
	funcKeyLeasePools.interval.Store(int64(defaultRetainLeaseTime * time.Millisecond))
	go funcKeyLeasePools.loop()
	return funcKeyLeasePools
}

func getSortedKeys(m map[string]any) []string {
	if m == nil || len(m) == 0 {
		return []string{}
	}

	// 提取所有 key
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	// 按字典序排序
	sort.Strings(keys)

	return keys
}

func getMapStr(m map[string]any) string {
	str := ""
	keys := getSortedKeys(m)
	for _, k := range keys {
		v := m[k]
		str += fmt.Sprintf("%s:%v,", k, v)
	}
	return str
}

func getPoolKey(funckey string, option *commontypes.AcquireOption) string {
	poolKey := funckey
	poolKey += "|"

	splits := make([]string, 0)
	splits = append(splits, funckey)
	m := make(map[string]any, len(option.ResourceSpecs))
	for k, v := range option.ResourceSpecs {
		m[k] = v
	}
	splits = append(splits, getMapStr(m))
	splits = append(splits, option.PoolLabel)
	m = make(map[string]any, len(option.InvokeTag))
	for k, v := range option.InvokeTag {
		m[k] = v
	}
	splits = append(splits, getMapStr(m))
	splits = append(splits, option.InstanceLabel)
	sessionStr := ""
	if option.InstanceSession != nil {
		sessionStr = option.InstanceSession.ToString()
	}
	splits = append(splits, sessionStr)
	splits = append(splits, option.RingName)
	return strings.Join(splits, "|")
}

func (flps *FuncKeyLeasePools) acquireInstance(option *commontypes.AcquireOption) (*commontypes.InstanceAllocationInfo,
	snerror.SNError) {
	poolKey := getPoolKey(flps.funcKey, option)
	flps.RLock()
	leasePool, ok := flps.leasePools[poolKey]
	if !ok {
		flps.RUnlock()
		flps.Lock()
		leasePool, ok = flps.leasePools[poolKey]
		if !ok {
			leasePool = newInstanceLeasePool(flps.funcKey, option, flps.ringName)
			flps.leasePools[poolKey] = leasePool
		}
		flps.Unlock()
		flps.RLock()
	}
	leasePool.IncPendingReqNum()
	defer leasePool.DecPendingReqNum()
	flps.RUnlock()
	lease, err := leasePool.acquireInstanceLease(option)
	if err != nil {
		return nil, err
	}
	flps.Lock()

	flps.globalLeaseList[lease.ThreadID] = lease
	flps.leaseIdToLeasePool[lease.ThreadID] = leasePool
	flps.Unlock()
	return lease.InstanceAllocationInfo, nil
}

func (flps *FuncKeyLeasePools) releaseInstanceLease(leaseID string, abnormal bool) {
	flps.RLock()
	if _, ok := flps.globalLeaseList[leaseID]; !ok {
		flps.RUnlock()
		return
	}
	leasePool, ok := flps.leaseIdToLeasePool[leaseID]
	if !ok {
		flps.RUnlock()
		flps.Lock()
		delete(flps.globalLeaseList, leaseID)
		flps.Unlock()
		return
	}
	flps.RUnlock()
	leasePool.releaseInstanceLease(leaseID, abnormal)
}

func (flps *FuncKeyLeasePools) loop() {
	ticker := time.NewTicker(normalizeRetainInterval(time.Duration(flps.interval.Load())))
	defer ticker.Stop()
	for {
		select {
		case <-flps.stopCh:
			flps.logger.Infof("end leasepools loop")
			flps.Lock()
			for _, leasePool := range flps.leasePools {
				utils.SafeCloseChannel(leasePool.stopCh)
			}

			flps.Unlock()
			return
		case <-ticker.C:
			flps.clearEmptyLeasePool()
			flps.doBatchRetain()
			ticker.Reset(normalizeRetainInterval(time.Duration(flps.interval.Load())))
		}
	}
}

func normalizeRetainInterval(interval time.Duration) time.Duration {
	if interval < time.Millisecond {
		return defaultRetainLeaseTime * time.Millisecond
	}
	return interval
}

func (flps *FuncKeyLeasePools) clearEmptyLeasePool() {
	flps.Lock()
	for key, leasePool := range flps.leasePools {
		if leasePool.empty() && leasePool.pendingNum.Load() <= 0 {
			utils.SafeCloseChannel(leasePool.stopCh)
			delete(flps.leasePools, key)
		}
	}
	flps.Unlock()
}

func (flps *FuncKeyLeasePools) getSchedulerNodeInfo(lease *InstanceLease) *schedulerproxy.SchedulerNodeInfo {
	schedulerId := lease.schedulerInstanceId
	schedulerInfo := getProxyManagerByRing(lease.ringName).GetSchedulerByInstanceId(schedulerId)
	logger := log.GetLogger().With(zap.Any("leaseId", lease.ThreadID), zap.Any("ringName", lease.ringName))
	if schedulerInfo == nil {
		var err error
		schedulerInfo, err = getProxyManagerByRing(lease.ringName).Get(lease.FuncKey, logger)
		if err != nil {
			logger.Warnf("can not get scheduler")
			return nil
		}
	}
	return schedulerInfo
}

func assembleBatchRetainLeaseInfo(funcKey string, pool *LeasePool, lease *InstanceLease,
	logger api.FormatLogger) *BatchRetainLeaseInfo {
	if pool == nil || lease == nil {
		return nil
	}
	report := lease.reportRecord.report(false)
	info := &BatchRetainLeaseInfo{
		ProcReqNum:  report.ProcReqNum,
		AvgProcTime: report.AvgProcTime,
		MaxProcTime: report.MaxProcTime,
		IsAbnormal:  report.IsAbnormal,
		PoolKey:     getPoolKey(funcKey, lease.acquireOption),
		FunctionKey: funcKey,
	}

	if lease.reacquire {
		extraReacquireDataInfo := make(map[string][]byte)
		extraReacquireDataInfo["resourcesData"] = []byte(pool.resSpecStr)
		if pool.session != nil {
			bytes, err := json.Marshal(pool.session)
			if err != nil {
				logger.Warnf("marshal pool.session failed, err: %s", err.Error())
			} else {
				extraReacquireDataInfo["instanceSessionConfig"] = bytes
			}
		}
		if pool.poolLabel != "" {
			invokeLabel := map[string]string{
				httpconstant.HeaderInstanceLabel: pool.poolLabel,
			}
			bytes, e := json.Marshal(invokeLabel)
			if e != nil {
				logger.Warnf("marshal poollabel failed, e: %s", e.Error())
			} else {
				extraReacquireDataInfo["instanceInvokeLabel"] = bytes
			}
		}
		extraReacquireDataInfo["poolLabel"] = []byte(pool.poolLabel)
		if pool.enableSessionCtx {
			extraReacquireDataInfo[constant.SessionCtxID] = []byte(pool.sessionCtxID)
		}
		bytes, err := json.Marshal(extraReacquireDataInfo)
		if err != nil {
			logger.Warnf("marshal extraReacquireDataInfo failed, err: %s", err.Error())
		}
		info.ReacquireData = bytes
		logger.Infof("need reacquire")
	}
	return info
}

// BatchRetainLeaseInfosArr -
type BatchRetainLeaseInfosArr struct {
	arr []*BatchRetainLeaseInfos
}

func (flps *FuncKeyLeasePools) getProxyManager() *schedulerproxy.ProxyManager {
	if flps.ringName == nil {
		return schedulerproxy.Proxy
	}
	switch flps.ringName.Name {
	case constant.BlueRing:
		return schedulerproxy.BlueProxy
	default:
		return schedulerproxy.Proxy
	}
}

func getProxyManagerByRing(ringName string) *schedulerproxy.ProxyManager {
	switch ringName {
	case constant.BlueRing:
		return schedulerproxy.BlueProxy
	default:
		return schedulerproxy.Proxy
	}
}

func (flps *FuncKeyLeasePools) setRingName(leaseId string, lease *InstanceLease) {
	var ringName string
	leasePool := flps.leaseIdToLeasePool[leaseId]
	if leasePool != nil && leasePool.session != nil {
		if ShouldUseBlueRing(utils.ConvertHashToPercent(leasePool.session.SessionID)) {
			ringName = constant.BlueRing
		} else {
			ringName = constant.GreenRing
		}
		lease.Lock()
		defer lease.Unlock()
		if lease.ringName != ringName {
			lease.ringName = ringName
			lease.acquireOption.RingName = ringName
			lease.reacquire = true
		}
	}
}

type leaseRetainSnapshot struct {
	leaseId string
	lease   *InstanceLease
	pool    *LeasePool
}

func (flps *FuncKeyLeasePools) doBatchRetain() {
	snapshots, exitLeases := flps.collectRetainSnapshots()
	funcKeyAllBatches := flps.assembleRetainBatches(snapshots)
	flps.processBatchLease(exitLeases, funcKeyAllBatches)
}

func (flps *FuncKeyLeasePools) collectRetainSnapshots() ([]leaseRetainSnapshot, []*InstanceLease) {
	snapshots := make([]leaseRetainSnapshot, 0, defaultMapSize)
	exitLeases := make([]*InstanceLease, 0, defaultMapSize)
	flps.Lock()
	for leaseId, lease := range flps.globalLeaseList {
		lease.RLock()
		logger := log.GetLogger().With(zap.Any("batchRetain", true), zap.Any("leaseId", leaseId),
			zap.Any("ringName", lease.ringName))
		lease.RUnlock()
		if !leaseCanReuse(lease) || lease.exited.Load() {
			exitLeases = append(exitLeases, lease)
			logger.Debugf("lease exited")
			continue
		}
		logger.Debugf("doBatchRetain begin")
		flps.setRingName(leaseId, lease)
		snapshots = append(snapshots, leaseRetainSnapshot{
			leaseId: leaseId,
			lease:   lease,
			pool:    flps.leaseIdToLeasePool[leaseId],
		})
	}
	flps.Unlock()
	return snapshots, exitLeases
}

func (flps *FuncKeyLeasePools) assembleRetainBatches(
	snapshots []leaseRetainSnapshot) map[string]*BatchRetainLeaseInfosArr {
	funcKeyAllBatches := make(map[string]*BatchRetainLeaseInfosArr)
	for _, snap := range snapshots {
		lease := snap.lease
		lease.RLock()
		logger := log.GetLogger().With(zap.Any("batchRetain", true), zap.Any("leaseId", snap.leaseId),
			zap.Any("ringName", lease.ringName))
		schedulerInfo := flps.getSchedulerNodeInfo(lease)
		if schedulerInfo == nil {
			lease.RUnlock()
			continue
		}
		logger = logger.With(zap.Any("scheduler", schedulerInfo.InstanceInfo.InstanceName))
		info := assembleBatchRetainLeaseInfo(flps.funcKey, snap.pool, lease, logger)
		lease.RUnlock()
		appendRetainInfo(funcKeyAllBatches, schedulerInfo.InstanceInfo.InstanceID,
			schedulerInfo.InstanceInfo.Address, snap.leaseId, info)
	}
	return funcKeyAllBatches
}

func appendRetainInfo(funcKeyAllBatches map[string]*BatchRetainLeaseInfosArr,
	schedulerInstanceID, schedulerAddress, leaseId string, info *BatchRetainLeaseInfo) {
	batchInfoArr, ok := funcKeyAllBatches[schedulerInstanceID]
	lastIndex := -1
	if ok {
		lastIndex = len(batchInfoArr.arr) - 1
	} else {
		batchInfoArr = &BatchRetainLeaseInfosArr{arr: make([]*BatchRetainLeaseInfos, 0)}
	}
	if !ok || len(batchInfoArr.arr[lastIndex].infos) >= maxBatchSize {
		batchInfoArr.arr = append(batchInfoArr.arr, &BatchRetainLeaseInfos{
			infos:            make(map[string]*BatchRetainLeaseInfo, defaultMapSize),
			SchedulerAddress: schedulerAddress,
		})
		lastIndex = len(batchInfoArr.arr) - 1
		if lastIndex == 0 {
			funcKeyAllBatches[schedulerInstanceID] = batchInfoArr
		}
	}
	batchInfoArr.arr[lastIndex].targetName = assembleTargetName(batchInfoArr.arr[lastIndex].targetName, leaseId)
	if info != nil {
		batchInfoArr.arr[lastIndex].infos[leaseId] = info
	}
}

func assembleTargetName(targetName string, leaseId string) string {
	if targetName == "" {
		targetName = leaseId
	} else {
		targetName += "," + leaseId
	}
	return targetName
}

func (flps *FuncKeyLeasePools) processBatchLease(exitLeases []*InstanceLease,
	funcKeyAllBatches map[string]*BatchRetainLeaseInfosArr) {
	flps.Lock()
	for _, lease := range exitLeases {
		delete(flps.globalLeaseList, lease.ThreadID)
		pool, ok := flps.leaseIdToLeasePool[lease.ThreadID]
		if ok {
			pool.removeLease(lease.ThreadID)
		}
		delete(flps.leaseIdToLeasePool, lease.ThreadID)
	}
	flps.Unlock()

	for _, batches := range funcKeyAllBatches {
		for _, batch := range batches.arr {
			go func(batch *BatchRetainLeaseInfos) {
				traceId := uuid.New().String()
				if resp, err := doBatchRetainInvoke(batch, traceId); err != nil {
					flps.processErrBatchResponse(batch)
				} else {
					flps.processBatchResponse(batch, resp)
				}
			}(batch)
		}
	}
}

func (flps *FuncKeyLeasePools) processSucceedRetainLeaseIdsBare(batch *BatchRetainLeaseInfos,
	infos map[string]commontypes.InstanceAllocationSucceedInfo) {
	for leaseId, _ := range infos {
		info := batch.infos[leaseId]
		pool, ok := flps.leasePools[info.PoolKey]
		if !ok {
			continue
		}
		pool.Lock()
		lease, ok := pool.leaseMap[leaseId]
		if !ok {
			pool.Unlock()
			continue
		}
		lease.Lock()
		lease.reacquire = false
		lease.Unlock()
		pool.Unlock()
	}
}

func (flps *FuncKeyLeasePools) processBatchResponse(batch *BatchRetainLeaseInfos,
	resp *commontypes.BatchInstanceResponse) {
	interval := time.Duration(resp.LeaseInterval/2) * time.Millisecond // half of leaseInterval
	flps.interval.Store(int64(normalizeRetainInterval(interval)))
	reacquireLeaseIds, decreaseLeaseIds := flps.parseReacquireAndDecreaseIds(resp)

	flps.Lock()
	defer flps.Unlock()
	flps.processSucceedRetainLeaseIdsBare(batch, resp.InstanceAllocSucceed)
	for leaseId, schedulerInstanceId := range reacquireLeaseIds {
		info := batch.infos[leaseId]
		pool, ok := flps.leasePools[info.PoolKey]
		if !ok {
			continue
		}
		pool.Lock()
		lease, ok := pool.leaseMap[leaseId]
		if !ok {
			pool.Unlock()
			continue
		}
		lease.Lock()
		lease.reacquire = true
		lease.schedulerInstanceId = schedulerInstanceId
		lease.Unlock()
		pool.Unlock()
	}

	for _, leaseId := range decreaseLeaseIds {
		info := batch.infos[leaseId]
		pool, ok := flps.leasePools[info.PoolKey]
		if !ok {
			continue
		}
		pool.Lock()
		lease, ok := pool.leaseMap[leaseId]
		if !ok {
			pool.Unlock()
			continue
		}
		lease.destroy()
		delete(flps.globalLeaseList, leaseId)
		pool.idleLeaseList.DelByID(leaseId)
		delete(pool.leaseMap, leaseId)
		pool.Unlock()
	}
}

func (flps *FuncKeyLeasePools) parseReacquireAndDecreaseIds(resp *commontypes.BatchInstanceResponse) (
	map[string]string, []string) {
	reacquireLeaseIds := make(map[string]string)
	decreaseLeaseIds := make([]string, 0)
	for leaseId, errInfo := range resp.InstanceAllocFailed {
		if errInfo.ErrorCode == statuscode.LeaseIDNotFoundCode {
			reacquireLeaseIds[leaseId] = ""
		} else if errInfo.ErrorCode == statuscode.AcquireNonOwnerSchedulerErrorCode {
			reacquireLeaseIds[leaseId] = errInfo.ErrorMessage
		} else {
			decreaseLeaseIds = append(decreaseLeaseIds, leaseId)
		}
	}
	return reacquireLeaseIds, decreaseLeaseIds
}

func (flps *FuncKeyLeasePools) processErrBatchResponse(batch *BatchRetainLeaseInfos) {
	flps.Lock()
	for leaseId, info := range batch.infos {
		pool, ok := flps.leasePools[info.PoolKey]
		if !ok {
			continue
		}
		pool.Lock()
		lease, ok := pool.leaseMap[leaseId]
		if ok {
			lease.Lock()
			lease.reacquire = true
			lease.Unlock()
		}
		pool.Unlock()
	}
	flps.Unlock()
}

// LeasePool stores instance leases
type LeasePool struct {
	funcKey          string
	invokeLabel      string
	poolLabel        string
	invokeTag        map[string]string
	session          *commontypes.InstanceSessionConfig
	sessionCtxID     string
	enableSessionCtx bool
	idleLeaseList    *queue.FifoQueue
	leaseMap         map[string]*InstanceLease
	resSpecStr       string
	stopCh           chan struct{}
	sync.RWMutex
	logger        api.FormatLogger
	leasePoolKey  string
	inFlightCount atomic.Int32
	pendingNum    atomic.Int32
	ringName      *types.RingName
}

func identityFunc(obj interface{}) string {
	lease, ok := obj.(*InstanceLease)
	if ok && lease != nil {
		return lease.ThreadID
	}
	return ""
}

func newInstanceLeasePool(funcKey string, option *commontypes.AcquireOption, ringName *types.RingName) *LeasePool {
	return &LeasePool{
		funcKey:          funcKey,
		invokeLabel:      option.InstanceLabel,
		poolLabel:        option.PoolLabel,
		invokeTag:        option.InvokeTag,
		session:          option.InstanceSession,
		sessionCtxID:     option.SessionCtxID,
		enableSessionCtx: option.EnableSessionCtx,
		idleLeaseList:    queue.NewFifoQueue(identityFunc),
		leaseMap:         make(map[string]*InstanceLease, defaultMapSize),
		stopCh:           make(chan struct{}),
		logger:           log.GetLogger().With(zap.Any("poolKey", getPoolKey(funcKey, option))),
		inFlightCount:    atomic.Int32{},
		ringName:         ringName,
	}
}

func (lp *LeasePool) getProxyManager() *schedulerproxy.ProxyManager {
	lp.RLock()
	defer lp.RUnlock()
	if lp.ringName == nil {
		return schedulerproxy.Proxy
	}
	switch lp.ringName.Name {
	case constant.BlueRing:
		return schedulerproxy.BlueProxy
	default:
		return schedulerproxy.Proxy
	}
}

// IncPendingReqNum -
func (lp *LeasePool) IncPendingReqNum() {
	lp.pendingNum.Add(1)
}

// DecPendingReqNum -
func (lp *LeasePool) DecPendingReqNum() {
	lp.pendingNum.Add(-1)
}

func (lp *LeasePool) empty() bool {
	lp.RLock()
	defer lp.RUnlock()
	return len(lp.leaseMap) == 0 && lp.inFlightCount.Load() == 0
}

func (ip *LeasePool) acquireHandler(funcKey string, option *commontypes.AcquireOption) (*InstanceLease,
	snerror.SNError) {
	logger := ip.logger.With(zap.Any("traceID", option.TraceID))

	unavailableSchedulerNodeInfos := make([]*schedulerproxy.SchedulerNodeInfo, 0)
	hashRetry := true
	var acquireResponse *commontypes.InstanceResponse
	var acquireResponseErr error
	acquireDependOnHash := func() error {
		var schedulerNodeInfo *schedulerproxy.SchedulerNodeInfo
		var getSchedulerNodeInfoErr error
		if option.EnableSessionCtx {
			logger.Debugf("acquire with sessionCtx routing, funcKey=%s, sessionCtxID=%q",
				funcKey, option.SessionCtxID)
			schedulerNodeInfo, getSchedulerNodeInfoErr = getProxyManagerByRing(
				option.RingName).GetWithoutUnexpectedSchedulerInfosWithCtx(
				funcKey, option.SessionCtxID, unavailableSchedulerNodeInfos, logger)
		} else {
			schedulerNodeInfo, getSchedulerNodeInfoErr = getProxyManagerByRing(
				option.RingName).GetWithoutUnexpectedSchedulerInfos(
				funcKey, unavailableSchedulerNodeInfos, logger)
		}
		if getSchedulerNodeInfoErr != nil {
			hashRetry = false
			return getSchedulerNodeInfoErr
		}
		acquireResponse, acquireResponseErr = doAcquireInvoke(option, schedulerNodeInfo.InstanceInfo.Address, funcKey,
			option.Timeout)
		if acquireResponseErr != nil {
			logger.Infof("acquire response err: %s", acquireResponseErr.Error())
		}
		if acquireResponseErr != nil || acquireResponse.ErrorCode == statuscode.ErrFinalized {
			unavailableSchedulerNodeInfos = append(unavailableSchedulerNodeInfos, schedulerNodeInfo)
			return acquireResponseErr
		}
		if acquireResponse != nil && acquireResponse.ErrorCode == statuscode.AcquireNonOwnerSchedulerErrorCode {
			acquireResponse, getSchedulerNodeInfoErr = ip.acquireWithSameSchedulerIdRetry(funcKey, option,
				acquireResponse.ErrorMessage)
		}
		return acquireResponseErr
	}

	isRetryDependOnHash := func() bool {
		return hashRetry
	}
	acquireResponseErr = util.Retry(acquireDependOnHash, isRetryDependOnHash, 10, 10*time.Millisecond) // magic number
	if acquireResponseErr != nil {
		if acquireResponseErr.Error() == constant.AllSchedulerUnavailableErrorMessage {
			return nil, snerror.New(statuscode.ErrAllSchedulerUnavailable, acquireResponseErr.Error())
		} else {
			return nil, snerror.New(statuscode.ErrInnerCommunication, acquireResponseErr.Error())
		}
	}
	if acquireResponse == nil {
		return nil, snerror.New(statuscode.ErrInnerCommunication, "get acquire response failed")
	}
	if acquireResponse.ErrorCode != constant.InsReqSuccessCode {
		return nil, snerror.New(acquireResponse.ErrorCode, acquireResponse.ErrorMessage)
	}
	lease := newInstanceLease(&acquireResponse.InstanceAllocationInfo, option)
	return lease, nil
}

func (ip *LeasePool) acquireWithSameSchedulerIdRetry(funcKey string, option *commontypes.AcquireOption,
	schedulerId string) (*commontypes.InstanceResponse, error) {
	schedulerNodeInfo := getProxyManagerByRing(option.RingName).GetSchedulerByInstanceId(schedulerId)
	if schedulerNodeInfo == nil {
		return nil, fmt.Errorf("not found scheduler: %s info", schedulerId)
	}
	var acquireResponse *commontypes.InstanceResponse
	acquireDependOnSame := func() error {
		var acquireResponseErr error
		acquireResponse, acquireResponseErr = doAcquireInvoke(option, schedulerNodeInfo.InstanceInfo.Address,
			funcKey, option.Timeout)
		if acquireResponseErr != nil {
			log.GetLogger().Errorf("acquire response err: %s", acquireResponseErr.Error())
			return acquireResponseErr
		}
		return nil
	}
	err := util.Retry(acquireDependOnSame, func() bool {
		return true
	}, 3, 200*time.Millisecond) // magic number
	if err != nil {
		return nil, err
	}
	return acquireResponse, nil
}

func (ip *LeasePool) removeLease(leaseID string) {
	ip.Lock()
	lease, exist := ip.leaseMap[leaseID]
	if !exist {
		ip.Unlock()
		return
	} else {
		lease.destroy()
	}
	delete(ip.leaseMap, leaseID)
	ip.idleLeaseList.DelByID(leaseID)
	ip.Unlock()
}

func (ip *LeasePool) handleLeaseExpiredLoop(lease *InstanceLease) {
	intervalTicker := time.NewTicker(10 * time.Millisecond) // check interval time duration
	defer intervalTicker.Stop()
	releaseTimer := time.NewTimer(idleHoldTime * time.Millisecond)
	defer releaseTimer.Stop()
	logger := ip.logger.With(zap.Any("leaseId", lease.ThreadID))
	release := false
	for {
		select {
		case _, ok := <-intervalTicker.C:
			if !ok {
				logger.Infof("intervalTicker closed")
				return
			}
			checkLeaseIdle(lease, releaseTimer, logger)
		case _, ok := <-releaseTimer.C:
			if !ok {
				logger.Infof("releaseTimer closed")
				release = true
				break
			}
			if lease.claim() {
				release = true
			}
		case _, ok := <-lease.stopCh:
			if !ok {
				logger.Infof("lease stopCh closed")
			}
			release = true
		case _, ok := <-ip.stopCh:
			if !ok {

			}
			logger.Infof("end handle lease release lifecyle")
			release = true
		}
		if release {
			ip.removeLease(lease.ThreadID)
			ip.doReleaseInvoke(ip.funcKey, lease.ThreadID, lease.acquireOption, lease.report(true))
			logger.Infof("release lease")
			return
		}
	}
}

func checkLeaseIdle(lease *InstanceLease, releaseTimer *time.Timer, logger api.FormatLogger) {
	if !lease.beginRelease.Load() {
		releaseTimer.Stop()
	}
	if lease.available.Load() && !lease.beginRelease.Load() {
		lease.beginRelease.Store(true)
		releaseTimer.Reset(idleHoldTime * time.Millisecond)
		logger.Debugf("reset releaseTimer")
	}
	if !lease.available.Load() {
		beginRelease := lease.beginRelease.Load()
		lease.beginRelease.Store(false)
		releaseTimer.Stop()
		if beginRelease {
			logger.Debugf("stop releaseTimer")
		}
	}
}

func (ip *LeasePool) traverseIdleLeaseList(option *commontypes.AcquireOption) *InstanceLease {
	ip.Lock()
	defer ip.Unlock()
	var lease *InstanceLease
	for leaseRaw := ip.idleLeaseList.Front(); leaseRaw != nil; leaseRaw = ip.idleLeaseList.Front() {
		l, ok := leaseRaw.(*InstanceLease)
		if !ok {
			ip.idleLeaseList.PopFront()
			continue
		}
		// If the function signature has changed, the lease is not reused.
		if l.acquireOption.FuncSig != option.FuncSig {
			log.GetLogger().Warnf("lease %s has a different signature %s which should be %s for function %s,"+
				"traceID %s", l.ThreadID, l.acquireOption.FuncSig, option.FuncSig, ip.funcKey, option.TraceID)

			for ip.idleLeaseList.Len() != 0 {
				ip.idleLeaseList.PopFront()
			}
			break
		}
		ip.idleLeaseList.PopFront()
		if l.claim() {
			lease = l
			break
		}
	}
	return lease
}

func (ip *LeasePool) acquireInstanceLease(option *commontypes.AcquireOption) (*InstanceLease, snerror.SNError) {
	lease := ip.traverseIdleLeaseList(option)
	if lease != nil {
		return lease, nil
	}

	ip.inFlightCount.Add(1)
	lease, snError := ip.acquireHandler(ip.funcKey, option)

	if snError != nil {
		ip.inFlightCount.Add(-1)
		return nil, snError
	}
	lease.claim()
	ip.Lock()
	if _, exist := ip.leaseMap[lease.ThreadID]; exist {
		ip.inFlightCount.Add(-1)
		ip.Unlock()
		log.GetLogger().Errorf("acquired lease %s already exist for function %s traceID %s", lease.ThreadID,
			ip.funcKey, option.TraceID)
		// acquired a repeated lease, should acquire a new lease
		return nil, snerror.New(constant.InsAcquireLeaseExistErrorCode, "lease already exist")
	}
	ip.leaseMap[lease.ThreadID] = lease
	ip.inFlightCount.Add(-1)
	ip.Unlock()
	if leaseCanReuse(lease) {
		go ip.handleLeaseExpiredLoop(lease)
	}
	log.GetLogger().Infof("succeed to acquire lease %s for function %s from scheduler %s",
		lease.ThreadID, ip.funcKey, option.SchedulerName)
	return lease, nil
}

func (ip *LeasePool) releaseInstanceLease(leaseID string, abnormal bool) {
	ip.Lock()
	lease, ok := ip.leaseMap[leaseID]
	if !ok {
		ip.Unlock()
		return
	}
	lease.free(abnormal, true)
	if !abnormal && leaseCanReuse(lease) {
		_ = ip.idleLeaseList.PushBack(lease)
	} else {
		lease.destroy()
		delete(ip.leaseMap, leaseID)
		ip.idleLeaseList.DelByID(leaseID)
	}
	ip.Unlock()
}

func leaseCanReuse(lease *InstanceLease) bool {
	lease.RLock()
	defer lease.RUnlock()
	if lease.LeaseInterval <= 0 {
		return false
	}
	return true
}
