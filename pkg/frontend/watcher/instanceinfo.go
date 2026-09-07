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

package watcher

import (
	"strings"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/instancemanager"
)

const (
	instanceEtcdKeyLen = 14
)

func instanceWatchEventLabel(eventType int) string {
	switch eventType {
	case etcd3.PUT:
		return "put"
	case etcd3.DELETE:
		return "delete"
	case etcd3.HISTORYDELETE:
		return "history_delete"
	case etcd3.HISTORYUPDATE:
		return "history_update"
	case etcd3.SYNCED:
		return "synced"
	case etcd3.ERROR:
		return "error"
	default:
		return "unknown"
	}
}

func startWatchInstanceInfo(stopCh <-chan struct{}) {
	etcdClient := etcd3.GetRouterEtcdClient()
	watcher := etcd3.NewEtcdWatcher(constant.InstancePathPrefix, instanceInfoFilter,
		instanceInfoHandler, stopCh, etcdClient)
	watcher.StartWatch()
}

func instanceInfoHandler(event *etcd3.Event) {
	recordInstanceWatchEvent(event.Type)
	switch event.Type {
	case etcd3.PUT:
		log.GetLogger().Debugf("handling instance info PUT event, key:%s", event.Key)
		instancemanager.ProcessInstanceUpdate(event)
	case etcd3.DELETE:
		log.GetLogger().Debugf("handling instance info DELETE event, key:%s", event.Key)
		instancemanager.ProcessInstanceDelete(event)
	case etcd3.SYNCED:
		log.GetLogger().Debugf("handling instance info SYNCED event")
		instancemanager.ProcessInstanceSync(event)
	case etcd3.ERROR:
		log.GetLogger().Warnf("etcd error event: %s", event.Value)
	default:
		log.GetLogger().Warnf("unsupported event, key: %s", event.Key)
	}
}

// key: /sn/aliases/business/<businessID>/tenant/<tenantID>/function/<functionName>/<aliasName> // todo 有问题
func instanceInfoFilter(event *etcd3.Event) bool {
	etcdKey := event.Key
	keyParts := strings.Split(etcdKey, constant.ETCDEventKeySeparator)
	if len(keyParts) != instanceEtcdKeyLen {
		return true
	}
	return false
}
