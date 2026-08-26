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

// Package instancemanager -
package instancemanager

import (
	"strings"

	"go.uber.org/zap"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/instance"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/utils"
)

const (
	funcKeyDelimiter       = "/"
	validFuncKeyLen        = 3
	innerFuncKeyDelimiter  = "-"
	funcNameIndexInFuncKey = 2
	faasManagerFuncName    = "faasmanager"

	functionKeyNote = "FUNCTION_KEY_NOTE"
)

// IsFaaSManager checks if a funcKey is t
func IsFaaSManager(funcKey string) bool {
	items := strings.Split(funcKey, innerFuncKeyDelimiter)
	if len(items) != validFuncKeyLen {
		return false
	}
	return items[funcNameIndexInFuncKey] == faasManagerFuncName
}

// isFaaSScheduler used to filter the etcd event which stands for a faas scheduler
func isFaaSScheduler(etcdPath string) bool {
	info, err := utils.GetFunctionInstanceInfoFromEtcdKey(etcdPath)
	if err != nil {
		return false
	}
	return strings.Contains(info.FunctionName, "faasscheduler")
}

// ProcessInstanceUpdate -
func ProcessInstanceUpdate(event *etcd3.Event) {
	logger := log.GetLogger().With(zap.Any("etcdType", event.Type),
		zap.Any("revisionId", event.Rev))
	instanceId := instance.GetInstanceIDFromEtcdKey(event.Key)
	insSpec := instance.GetInsSpecFromEtcdValue(event.Key, event.Value)
	removeEvictingInstance(instanceId)
	if len(instanceId) == 0 || insSpec == nil {
		logger.Warnf("ignoring invalid etcd key, key: %s", event.Key)
		return
	}
	logger = logger.With(zap.Any("instanceId", instanceId))
	ProcessAppInfoUpdate(event)

	items := strings.Split(insSpec.Function, funcKeyDelimiter)
	if len(items) != validFuncKeyLen {
		return
	}
	insSpec.InstanceID = instanceId

	if IsFaaSManager(items[1]) {
		return
	}

	if isFaaSScheduler(event.Key) {
		if insSpec.InstanceStatus.Code == int32(constant.KernelInstanceStatusRunning) {
			GetFaaSSchedulerInstanceManager().addInstance(instanceId, insSpec, logger)
		} else {
			GetFaaSSchedulerInstanceManager().delInstance(instanceId, logger)
		}
		return
	}

	functionKey := insSpec.CreateOptions[functionKeyNote]
	if functionKey == "" {
		logger.Warnf("ignoring invalid instance meta data, function is empty, eventKey: %s", event.Key)
		return
	}

	if insSpec.InstanceStatus.Code != int32(constant.KernelInstanceStatusRunning) {
		GetGlobalInstanceScheduler().delInstance(functionKey, insSpec, logger)
		if insSpec.InstanceStatus.Code == int32(constant.KernelInstanceStatusEvicting) {
			recordEvictingInstance(insSpec)
		}
	} else {
		GetGlobalInstanceScheduler().addInstance(functionKey, insSpec, logger)
		// Now that the full instance is reachable in instancesMap, the
		// create-path routeOnlyInstance shim is no longer needed.
		RemoveRouteOnlyInstance(instanceId)
	}
}

// ProcessInstanceDelete -
func ProcessInstanceDelete(event *etcd3.Event) {
	logger := log.GetLogger().With(zap.Any("etcdKey", event.Key), zap.Any("etcdType", event.Type),
		zap.Any("revisionId", event.Rev))
	instanceId := instance.GetInstanceIDFromEtcdKey(event.Key)
	insSpec := instance.GetInsSpecFromEtcdValue(event.Key, event.PrevValue)
	RemoveRouteOnlyInstance(instanceId)
	removeEvictingInstance(instanceId)
	if len(instanceId) == 0 || insSpec == nil {
		logger.Warnf("ignoring invalid etcd key")
		return
	}
	logger = logger.With(zap.Any("instanceId", instanceId))
	ProcessAppInfoDelete(event)
	items := strings.Split(insSpec.Function, funcKeyDelimiter)
	if len(items) != validFuncKeyLen {
		return
	}
	if IsFaaSManager(items[1]) {
		return
	}

	if isFaaSScheduler(event.Key) {
		GetFaaSSchedulerInstanceManager().delInstance(instanceId, logger)
		return
	}

	functionKey := insSpec.CreateOptions[functionKeyNote]
	if functionKey == "" {
		logger.Warnf("ignoring invalid instance meta data, function is IsEmpty")
		return
	}
	insSpec.InstanceID = instanceId
	GetGlobalInstanceScheduler().delInstance(functionKey, insSpec, logger)
}

// ProcessInstanceSync -
func ProcessInstanceSync(event *etcd3.Event) {
	logger := log.GetLogger().With(zap.Any("etcdKey", event.Key), zap.Any("etcdType", event.Type),
		zap.Any("revisionId", event.Rev))
	GetFaaSSchedulerInstanceManager().sync(logger)
}
