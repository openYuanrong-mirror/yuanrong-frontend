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

// Package schedulerproxy -
package schedulerproxy

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/loadbalance"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/upgradecompatible"
)

const (
	etcdPathElementsLen = 14
	tenantIndex         = 6
	functionNameIndex   = 8
	versionIndex        = 10
	instanceNameIndex   = 13
	funcKeyElementsLen  = 3
)

const (
	addSchedulerInfoOption    = "ADD"
	removeSchedulerInfoOption = "REMOVE"
)

// Proxy is the singleton proxy
var (
	Proxy     *ProxyManager
	BlueProxy *ProxyManager
)

func init() {
	Proxy = newSchedulerProxy(
		loadbalance.LBFactory(loadbalance.SimpleHashGeneric),
	)
	BlueProxy = newSchedulerProxy(
		loadbalance.LBFactory(loadbalance.SimpleHashGeneric),
	)
}

// ProxyManager is used to get instances from FaaSScheduler via a grpc stream
type ProxyManager struct {
	faasSchedulers sync.Map
	// key is tenantID, value is instanceID
	exclusivitySchedulers sync.Map
	// used to select a FaaSScheduler by the func info Concurrent Consistent Hash
	loadBalance loadbalance.LoadBalance
	RTAPI       api.LibruntimeAPI
}

// SchedulerNodeInfo -
type SchedulerNodeInfo struct {
	InstanceInfo *types.InstanceInfo
	UpdateTime   time.Time
}

// Add an FaaSScheduler
func (im *ProxyManager) Add(scheduleInfo *SchedulerNodeInfo, logger api.FormatLogger) {
	if im.RTAPI != nil {
		switch scheduleInfo.InstanceInfo.InstanceID != "" {
		case true:
			im.RTAPI.UpdateSchdulerInfo(scheduleInfo.InstanceInfo.InstanceName, scheduleInfo.InstanceInfo.InstanceID,
				addSchedulerInfoOption)
		case false:
			im.RTAPI.UpdateSchdulerInfo(scheduleInfo.InstanceInfo.InstanceName, scheduleInfo.InstanceInfo.InstanceID,
				removeSchedulerInfoOption)
		default:

		}
	}
	im.faasSchedulers.Store(scheduleInfo.InstanceInfo.InstanceName, scheduleInfo)
	im.exclusivitySchedulers.Store(scheduleInfo.InstanceInfo.Exclusivity, scheduleInfo.InstanceInfo.InstanceName)
	if scheduleInfo.InstanceInfo.Exclusivity != "" {
		logger.Infof("no need to add scheduler to load balance for exclusivity %s",
			scheduleInfo.InstanceInfo.Exclusivity)
		return
	}
	im.loadBalance.Add(scheduleInfo.InstanceInfo.InstanceName, 0)
	logger.Infof("add scheduler to load balance")
}

// Exist -
func (im *ProxyManager) Exist(instanceName string, instanceId string) bool {
	value, ok := im.faasSchedulers.Load(instanceName)
	if !ok {
		return false
	}
	info, _ := value.(*SchedulerNodeInfo) // no need judge
	if info == nil {
		return false
	}
	return info.InstanceInfo.InstanceID == instanceId
}

// ExistInstanceName -
func (im *ProxyManager) ExistInstanceName(instanceName string) bool {
	_, ok := im.faasSchedulers.Load(instanceName)
	return ok
}

// Remove a FaaSScheduler
func (im *ProxyManager) Remove(schedulerInfo *types.InstanceInfo, logger api.FormatLogger) {
	if _, ok := im.faasSchedulers.Load(schedulerInfo.InstanceName); !ok {
		logger.Infof("no need delete unexist scheduler")
		return
	}
	if im.RTAPI != nil {
		im.RTAPI.UpdateSchdulerInfo(schedulerInfo.InstanceName, schedulerInfo.InstanceID, removeSchedulerInfoOption)
	}
	im.faasSchedulers.Delete(schedulerInfo.InstanceName)
	im.exclusivitySchedulers.Range(func(key, value interface{}) bool {
		instanceID, ok := value.(string)
		if !ok {
			return true
		}
		if instanceID == schedulerInfo.InstanceName {
			im.exclusivitySchedulers.Delete(key)
		}
		return true
	})
	im.loadBalance.Remove(schedulerInfo.InstanceName)
	logger.Infof("deleted from load balance")
}

// Get an instance for this request
func (im *ProxyManager) Get(funcKey string, logger api.FormatLogger) (*SchedulerNodeInfo, error) {
	return im.getSchedulerByHashKey(funcKey, logger)
}

// GetByAcquireOption selects an owner with the same routing precedence as LiteScheduler:
// SessionContext, InstanceSession, then function owner.
func (im *ProxyManager) GetByAcquireOption(funcKey string, option *types.AcquireOption,
	logger api.FormatLogger) (*SchedulerNodeInfo, error) {
	return im.getSchedulerByHashKey(acquireOwnerKey(funcKey, option), logger)
}

// GetWithoutUnexpectedSchedulerInfosByAcquireOption is the retry variant of GetByAcquireOption.
func (im *ProxyManager) GetWithoutUnexpectedSchedulerInfosByAcquireOption(funcKey string,
	option *types.AcquireOption, unexpected []*SchedulerNodeInfo, logger api.FormatLogger) (*SchedulerNodeInfo, error) {
	return im.getWithoutUnexpectedSchedulerInfosByKey(acquireOwnerKey(funcKey, option), unexpected, logger)
}

func sessionContextOwnerKey(funcKey, sessionCtxID string) string {
	tenantID := strings.SplitN(funcKey, constant.KeySeparator, 2)[0]
	return tenantID + constant.KeySeparator + funcKey + constant.KeySeparator + sessionCtxID
}

func acquireOwnerKey(funcKey string, option *types.AcquireOption) string {
	if !upgradecompatible.IsLiteSchedulerEnabled() || option == nil {
		return funcKey
	}
	if option.EnableSessionCtx && option.SessionCtxID != "" {
		return sessionContextOwnerKey(funcKey, option.SessionCtxID)
	}
	if option.InstanceSession != nil && option.InstanceSession.SessionID != "" {
		tenantID := strings.SplitN(funcKey, constant.KeySeparator, 2)[0]
		return tenantID + constant.KeySeparator + option.InstanceSession.SessionID
	}
	return funcKey
}

func (im *ProxyManager) getSchedulerByHashKey(hashKey string, logger api.FormatLogger) (*SchedulerNodeInfo, error) {
	logger.Debugf("begin to get scheduler for hashKey: %s", hashKey)
	next, err := im.getNextScheduler(hashKey, logger)
	if err != nil {
		return nil, err
	}
	faasSchedulerName, ok := next.(string)
	if !ok {
		return nil, fmt.Errorf("failed to parse the result of loadbanlance: %+v", next)
	}
	if faasSchedulerName == "" {
		return nil, fmt.Errorf(constant.AllSchedulerUnavailableErrorMessage)
	}
	faaSScheduler := im.instanceNameToSchedulerNodeInfo(faasSchedulerName)
	if faaSScheduler == nil {
		return nil, fmt.Errorf("failed to get the faas scheduler named %s", faasSchedulerName)
	}
	logger.Infof("succeed to get scheduler instanceID: %s for hashKey: %s", faasSchedulerName, hashKey)
	return faaSScheduler, nil
}

func (im *ProxyManager) getFromExclusivityScheduler(funcKey string) string {
	elements := strings.Split(funcKey, constant.KeySeparator)
	if len(elements) != funcKeyElementsLen {
		return ""
	}
	var ok bool
	tenantID := elements[0]
	next, ok := im.exclusivitySchedulers.Load(tenantID)
	if ok && next != nil {
		instanceName, ok := next.(string)
		if ok {
			return instanceName
		}
		return ""
	}
	return ""
}

func (im *ProxyManager) instanceNameToSchedulerNodeInfo(instanceName string) *SchedulerNodeInfo {
	if strings.TrimSpace(instanceName) == "" {
		return nil
	}
	faaSSchedulerData, ok := im.faasSchedulers.Load(instanceName)
	if !ok {
		return nil
	}
	faaSScheduler, ok := faaSSchedulerData.(*SchedulerNodeInfo)
	if !ok {
		return nil
	}

	return faaSScheduler
}

// GetByInstanceID resolves an owner redirect returned by a scheduler.
func (im *ProxyManager) GetByInstanceID(instanceID string) *SchedulerNodeInfo {
	return im.instanceNameToSchedulerNodeInfo(instanceID)
}

// GetWithoutUnexpectedSchedulerInfos -
func (im *ProxyManager) GetWithoutUnexpectedSchedulerInfos(funcKey string,
	unexpectedSchedulerNodeInfos []*SchedulerNodeInfo, logger api.FormatLogger) (*SchedulerNodeInfo, error) {
	return im.getWithoutUnexpectedSchedulerInfosByKey(funcKey, unexpectedSchedulerNodeInfos, logger)
}

func (im *ProxyManager) getWithoutUnexpectedSchedulerInfosByKey(hashKey string,
	unexpectedSchedulerNodeInfos []*SchedulerNodeInfo, logger api.FormatLogger) (*SchedulerNodeInfo, error) {
	logger.Debugf("begin to get scheduler for hashKey: %s", hashKey)
	if len(unexpectedSchedulerNodeInfos) == 0 {
		return im.getSchedulerByHashKey(hashKey, logger)
	}
	allSchedulers := make(map[string]*SchedulerNodeInfo, 0)
	im.faasSchedulers.Range(func(key, value any) bool {
		info, ok := value.(*SchedulerNodeInfo)
		if !ok {
			return true
		}
		allSchedulers[info.InstanceInfo.InstanceName] = info
		return true
	})
	tmpHash := loadbalance.LBFactory(loadbalance.SimpleHashGeneric)

	for _, v := range allSchedulers {
		flag := true
		for _, unexpectedSchedulerNodeInfo := range unexpectedSchedulerNodeInfos {
			if v.InstanceInfo.InstanceID == unexpectedSchedulerNodeInfo.InstanceInfo.InstanceID &&
				v.UpdateTime.Sub(unexpectedSchedulerNodeInfo.UpdateTime) <= 0 {
				flag = false
				break
			}
		}
		if flag {
			tmpHash.Add(v.InstanceInfo.InstanceName, 0)

		}
	}

	next := tmpHash.Next(hashKey, false)

	faasSchedulerName, ok := next.(string)
	if !ok {
		return nil, fmt.Errorf("failed to parse the result of loadbalance: %+v", next)
	}
	if faasSchedulerName == "" {
		return nil, fmt.Errorf(constant.AllSchedulerUnavailableErrorMessage)
	}
	schedulerNodeInfo := im.instanceNameToSchedulerNodeInfo(faasSchedulerName)
	if schedulerNodeInfo == nil {
		return nil, fmt.Errorf("failed to get the faas scheduler named %s", faasSchedulerName)
	}

	return schedulerNodeInfo, nil
}

// IsEmpty -
func (im *ProxyManager) IsEmpty() bool {
	flag := false
	im.faasSchedulers.Range(func(k, v any) bool {
		instance, ok := v.(*SchedulerNodeInfo)
		if !ok {
			return true
		}

		ok = instancemanager.GetFaaSSchedulerInstanceManager().IsExist(instance.InstanceInfo.InstanceID)
		if ok {
			flag = true
			return false
		}
		return true
	})
	return !flag
}

// GetSchedulerByInstanceId -
func (im *ProxyManager) GetSchedulerByInstanceId(instanceId string) *SchedulerNodeInfo {
	if instanceId == "" {
		return nil
	}
	var instanceInfo *SchedulerNodeInfo
	im.faasSchedulers.Range(func(k, v any) bool {
		schedulerNodeInfo, ok := v.(*SchedulerNodeInfo)
		if !ok {
			return true
		}
		if schedulerNodeInfo.InstanceInfo.InstanceID == instanceId {
			ok = instancemanager.GetFaaSSchedulerInstanceManager().IsExist(schedulerNodeInfo.InstanceInfo.InstanceID)
			if ok {
				instanceInfo = schedulerNodeInfo
				return false
			}
		}
		return true
	})
	return instanceInfo
}

// GetSchedulerByInstanceName -
func (im *ProxyManager) GetSchedulerByInstanceName(instanceName string, traceID string) (*SchedulerNodeInfo, error) {
	faaSSchedulerData, ok := im.faasSchedulers.Load(instanceName)
	if !ok {
		return nil, fmt.Errorf("failed to get the faas scheduler named %s,traceID %s", instanceName, traceID)
	}
	faaSScheduler, ok := faaSSchedulerData.(*SchedulerNodeInfo)
	if !ok {
		return nil, fmt.Errorf("invalid faas scheduler named %s: %#v, traceID: %s",
			instanceName, faaSSchedulerData, traceID)
	}
	return faaSScheduler, nil
}

func (im *ProxyManager) getNextScheduler(funcKey string, logger api.FormatLogger) (any, error) {
	var next interface{}

	// select one FaaSScheduler by the func key
	next = im.loadBalance.Next(funcKey, false)
	if next == nil {
		logger.Errorf("failed to get faaSScheduler instance, function: %s", funcKey)
		return nil, fmt.Errorf("failed to get faaSScheduler instance")
	}
	logger.Debugf("show next type is %+v", next)
	return next, nil
}

// DeleteBalancer -
func (im *ProxyManager) DeleteBalancer(funcKey string) {
	im.loadBalance.DeleteBalancer(funcKey)
}

// newSchedulerProxy return an instance pool which get the instance from the remote FaaSScheduler
func newSchedulerProxy(lb loadbalance.LoadBalance) *ProxyManager {
	return &ProxyManager{
		loadBalance: lb,
	}
}
