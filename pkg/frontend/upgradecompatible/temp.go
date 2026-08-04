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

// Package upgradecompatible -
package upgradecompatible

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"

	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/monitor"
)

// AccessSchedulerType -
const (
	DefaultAccessSchedulerType  = "frontend"
	AccessSchedulerByLibruntime = "libruntime"
	// LiteSchedulerEnableEnv is injected by yr start from values.lite_scheduler.enable.
	LiteSchedulerEnableEnv = "YR_LITE_SCHEDULER_ENABLE"
)

var (
	lock                    sync.RWMutex
	accessFaaSSchedulerType string
)

// Temp -
type Temp struct {
	AccessFaaSSchedulerType string `json:"accessFaaSSchedulerType"  valid:",optional"`
}

// GetAccessFaaSSchedulerType -
func GetAccessFaaSSchedulerType() string {
	lock.RLock()
	defer lock.RUnlock()
	if accessFaaSSchedulerType != "" {
		return accessFaaSSchedulerType
	}
	return DefaultAccessSchedulerType
}

// SetAccessFaaSSchedulerType -
func SetAccessFaaSSchedulerType(accessType string) {
	lock.Lock()
	defer lock.Unlock()
	accessFaaSSchedulerType = accessType
}

// IsLiteSchedulerEnabled reports whether Frontend should use session-based
// Scheduler ownership. Invalid and missing values preserve legacy function ownership.
func IsLiteSchedulerEnabled() bool {
	enabled, err := strconv.ParseBool(os.Getenv(LiteSchedulerEnableEnv))
	return err == nil && enabled
}

func loadAccessFaaSSchedulerType(fileName string, opType monitor.OpType) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		log.GetLogger().Warnf("read file error, filePath: %s, err: %s", fileName, err.Error())
		return
	}
	temp := &Temp{}

	err = json.Unmarshal(data, temp)
	if err != nil {
		log.GetLogger().Errorf("failed to parse the config data: %s", string(data))
		return
	}
	SetAccessFaaSSchedulerType(temp.AccessFaaSSchedulerType)
}

// WatchConfig -
func WatchConfig(configPath string, stopCh <-chan struct{}) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
		return err
	}
	watcher.RegisterCallback(configPath, loadAccessFaaSSchedulerType)
	return nil
}
