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

// Package config is used to keep the config used by the faas frontend function
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/asaskevich/govalidator/v11"

	"frontend/pkg/common/faas_common/alarm"
	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/crypto"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/redisclient"
	"frontend/pkg/common/faas_common/sts"
	commonType "frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/frontend/proxyrouting"
	"frontend/pkg/frontend/types"
	"frontend/pkg/frontend/upgradecompatible"
)

const (
	// DefaultTimeout defines the timeout of invoke
	DefaultTimeout                    = 1200
	defaultE2EMaxDelay                = 50
	defaultRPCClientConcurrentNum     = 1
	defaultStreamLengthLimitMb        = 1024
	defaultDataSystemPayloadLimitByte = 32
)

const (
	defaultLowerMemoryPercent        = 0.6
	defaultHighMemoryPercent         = 0.8
	defaultStatefulHighMemoryPercent = 0.85
	defaultMemoryRefreshInterval     = 20
	defaultBodyThreshold             = 40000
	defaultRequestMemoryEvaluator    = 2
	defaultTenantLimitQuota          = 1800
	// heartbeat default config
	defaultHeartbeatTimeout          = 2
	defaultHeartbeatInterval         = 3
	defaultHeartbeatTimeoutThreshold = 3
)

const (
	localAuthConfigEnvKey      = "PAAS_CRYPTO_PATH"
	defaultLocalAuthConfigPath = "/home/sn/resource/cipher"
)

var (
	fConfig    = &types.Config{}
	nativeAz   = ""
	loadAzOnce sync.Once
	stopCh     = make(chan struct{})
)

// MetricServerConfig define monitoring server config
type MetricServerConfig struct {
	ServerAddr  string                  `json:"serverAddr,omitempty" valid:",optional"`
	ServerMode  string                  `json:"serverMode,omitempty" valid:",optional"`
	Password    string                  `json:"password,omitempty" valid:",optional"`
	EnableTLS   bool                    `json:"enableTLS,omitempty" valid:",optional"`
	TimeoutConf redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
}

// GetConfig return the current fConfig
func GetConfig() *types.Config {
	return fConfig
}

// SetConfig set the current fConfig
func SetConfig(conf types.Config) {
	fConfig = &conf
}

// InitFunctionConfig is used to initialize the config
func InitFunctionConfig(data []byte) error {
	err := json.Unmarshal(data, &fConfig)
	if err != nil {
		return fmt.Errorf("failed to parse the config data: %s", err)
	}
	err = loadFunctionConfig(fConfig)
	if err != nil {
		return err
	}
	initDefaultTenantLimitQuota()
	initDefaultMemoryControlConfig()
	initDefaultMemoryEvaluatorConfig()
	initDefaultLocalAuthConfig()
	initDefaultHeartbeatConfig()
	initDefaultHTTPConfig()
	initDefaultWorkspaceOwnershipResolver()
	err = initWatchConfig(fConfig, stopCh)
	if err != nil {
		return err
	}
	return nil
}

// initDefaultWorkspaceOwnershipResolver sets the default workspace ownership
// resolver name when the config does not specify one.
func initDefaultWorkspaceOwnershipResolver() {
	if fConfig.WorkspaceOwnershipResolver == "" {
		fConfig.WorkspaceOwnershipResolver = "etcd"
	}
}

// loadFunctionConfig is used to initialize the config
func loadFunctionConfig(config *types.Config) error {
	if config.BusinessType == constant.BusinessTypeWiseCloud && config.DataSystemConfig == nil {
		return fmt.Errorf("invalid config: empty data system config in caas type")
	}
	if config.RouterEtcd.UseSecret {
		etcd3.SetETCDTLSConfig(&config.RouterEtcd)
	}
	if config.MetaEtcd.UseSecret {
		etcd3.SetETCDTLSConfig(&config.MetaEtcd)
	}
	_, err := govalidator.ValidateStruct(config)
	if err != nil {
		return fmt.Errorf("invalid config: %s", err)
	}
	err = setAlarmEnv(config)
	if err != nil {
		return err
	}
	if config.RawStsConfig.StsEnable {
		if err = sts.InitStsSDK(config.RawStsConfig.ServerConfig); err != nil {
			log.GetLogger().Errorf("failed to init sts sdk, err: %s", err.Error())
			return err
		}
		if err = os.Setenv(sts.EnvSTSEnable, "true"); err != nil {
			log.GetLogger().Errorf("failed to set env of %s, err: %s", sts.EnvSTSEnable, err.Error())
			return err
		}
		config.RawStsConfig.SensitiveConfigs.Auth =
			sts.DecryptSystemAuthConfig(config.RawStsConfig.SensitiveConfigs.Auth)
	}
	if config.SccConfig.Enable && crypto.InitializeSCC(config.SccConfig) != nil {
		return fmt.Errorf("failed to initialize scc")
	}
	return nil
}

// InitModuleConfig initializes config for module
func InitModuleConfig() error {
	configFromEnv, err := loadConfigFromEnv()
	if err != nil {
		log.GetLogger().Errorf("loadConfigFromEnv failed err %s", err)
		return err
	}

	fConfig = configFromEnv
	fConfig.EtcdLeaseConfig = &types.EtcdLeaseConfig{}

	log.GetLogger().Infof("init config success, authenticationEnable: %v", fConfig.AuthenticationEnable)

	utils.ValidateTimeout(&fConfig.HTTPConfig.RespTimeOut, DefaultTimeout)
	utils.ValidateTimeout(&fConfig.HTTPConfig.WorkerInstanceReadTimeOut, DefaultTimeout)
	if _, err = govalidator.ValidateStruct(fConfig); err != nil {
		log.GetLogger().Errorf("initConfigData error: %s", err.Error())
		return err
	}
	if fConfig.E2EMaxDelayTime <= 0 {
		fConfig.E2EMaxDelayTime = defaultE2EMaxDelay
	}
	if fConfig.RPCClientConcurrentNum < 1 {
		fConfig.RPCClientConcurrentNum = defaultRPCClientConcurrentNum
	}
	loadAzOnce.Do(func() {
		var exist bool
		nativeAz, exist = os.LookupEnv(fConfig.Runtime.AvailableZoneKey)
		if !exist || nativeAz == "" {
			nativeAz = constant.DefaultAZ
		}
		if len(nativeAz) > constant.ZoneNameLen {
			nativeAz = nativeAz[0 : constant.ZoneNameLen-1]
		}
	})
	initDefaultTenantLimitQuota()
	initDefaultMemoryControlConfig()
	initDefaultMemoryEvaluatorConfig()
	initDefaultHeartbeatConfig()
	initDefaultHTTPConfig()
	initDefaultWorkspaceOwnershipResolver()
	return nil
}

func loadConfigFromEnv() (*types.Config, error) {
	configJSON := os.Getenv(ConfigEnvKey)
	config := &types.Config{}
	err := json.Unmarshal([]byte(configJSON), config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// RecoverConfig will recover config
func RecoverConfig(stateConfig types.Config) error {
	fConfig = &types.Config{}
	err := utils.DeepCopyObj(stateConfig, &fConfig)
	if err != nil {
		return err
	}
	err = setAlarmEnv(fConfig)
	if err != nil {
		return err
	}
	log.GetLogger().Infof("configuration recovered ")
	return nil
}

// InitEtcd - init router etcd and meta etcd
func InitEtcd(stopCh <-chan struct{}) error {
	if &fConfig == nil {
		return fmt.Errorf("config is not initialized")
	}
	if err := etcd3.InitRouterEtcdClient(fConfig.RouterEtcd, fConfig.AlarmConfig, stopCh); err != nil {
		return fmt.Errorf("faaSFrontend failed to init route etcd: %s", err.Error())
	}

	if err := etcd3.InitMetaEtcdClient(fConfig.MetaEtcd, fConfig.AlarmConfig, stopCh); err != nil {
		return fmt.Errorf("faaSFrontend failed to init metadata etcd: %s", err.Error())
	}

	if len(fConfig.CAEMetaEtcd.Servers) != 0 {
		if err := etcd3.InitCAEMetaEtcdClient(fConfig.CAEMetaEtcd, fConfig.AlarmConfig, stopCh); err != nil {
			return fmt.Errorf("faaSFrontend failed to init cae metadata etcd: %s", err.Error())
		}
		log.GetLogger().Infof("init CAEMetaEtcd success")
	}

	if len(fConfig.DataSystemEtcd.Servers) != 0 {
		if err := etcd3.InitDataSystemEtcdClient(fConfig.DataSystemEtcd, fConfig.AlarmConfig, stopCh); err != nil {
			return fmt.Errorf("faaSFrontend failed to init dataSystemEtcd etcd: %s", err.Error())
		}
		log.GetLogger().Infof("init dataSystemEtcd success")
	}

	proxyrouting.InitGlobalOwnershipResolver(fConfig.WorkspaceOwnershipResolver, etcd3.GetMetaEtcdClient())

	return nil
}

// ClearSensitiveInfo -
func ClearSensitiveInfo() {
	if &fConfig == nil {
		return
	}
	utils.ClearStringMemory(fConfig.RouterEtcd.Password)
	utils.ClearStringMemory(fConfig.MetaEtcd.Password)
}

func setAlarmEnv(fConfig *types.Config) error {
	if !fConfig.AlarmConfig.EnableAlarm {
		log.GetLogger().Infof("enable alarm is false")
		return nil
	}
	utils.SetClusterNameEnv(fConfig.ClusterName)
	alarm.SetAlarmEnv(fConfig.AlarmConfig.AlarmLogConfig)
	alarm.SetXiangYunFourConfigEnv(fConfig.AlarmConfig.XiangYunFourConfig)
	err := alarm.SetPodIP()
	if err != nil {
		return err
	}
	return nil
}

func initDefaultLocalAuthConfig() {
	LocalAuthCryptoPath := defaultLocalAuthConfigPath
	if fConfig.AuthConfig.LocalAuthConfig.LocalAuthCryptoPath != "" {
		LocalAuthCryptoPath = fConfig.AuthConfig.LocalAuthConfig.LocalAuthCryptoPath
	}
	err := os.Setenv(localAuthConfigEnvKey, LocalAuthCryptoPath)
	if err != nil {
		log.GetLogger().Warnf("initDefaultLocalAuthConfig error, error is %s", err.Error())
		return
	}
	return
}

func initDefaultMemoryEvaluatorConfig() {
	if fConfig.MemoryEvaluatorConfig == nil {
		fConfig.MemoryEvaluatorConfig = &types.MemoryEvaluatorConfig{}
	}
	if fConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator <= 0 {
		fConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator = defaultRequestMemoryEvaluator
	}
	log.GetLogger().Infof("RequestMemoryEvaluator %f", fConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
}

func initDefaultHTTPConfig() {
	if fConfig.HTTPConfig == nil {
		fConfig.HTTPConfig = &types.FrontendHTTP{}
	}
	if fConfig.HTTPConfig.MaxStreamRequestBodySize == 0 {
		fConfig.HTTPConfig.MaxStreamRequestBodySize = defaultStreamLengthLimitMb
	}

	if fConfig.HTTPConfig.MaxDataSystemMultiDataBodySize == 0 {
		fConfig.HTTPConfig.MaxDataSystemMultiDataBodySize = defaultDataSystemPayloadLimitByte
	}
	if fConfig.HTTPConfig.ServerListenPort == 0 {
		fConfig.HTTPConfig.ServerListenPort = HTTPServerListenPort
	}
}

func initDefaultMemoryControlConfig() {
	if fConfig.MemoryControlConfig == nil {
		fConfig.MemoryControlConfig = &commonType.MemoryControlConfig{}
	}

	if fConfig.MemoryControlConfig.LowerMemoryPercent <= 0 {
		fConfig.MemoryControlConfig.LowerMemoryPercent = defaultLowerMemoryPercent
	}
	if fConfig.MemoryControlConfig.HighMemoryPercent <= 0 {
		fConfig.MemoryControlConfig.HighMemoryPercent = defaultHighMemoryPercent
	}
	if fConfig.MemoryControlConfig.StatefulHighMemPercent <= 0 {
		fConfig.MemoryControlConfig.StatefulHighMemPercent = defaultStatefulHighMemoryPercent
	}
	if fConfig.MemoryControlConfig.MemDetectIntervalMs <= 0 {
		fConfig.MemoryControlConfig.MemDetectIntervalMs = defaultMemoryRefreshInterval
	}
	if fConfig.MemoryControlConfig.BodyThreshold <= 0 {
		fConfig.MemoryControlConfig.BodyThreshold = defaultBodyThreshold
	}
}

func initDefaultTenantLimitQuota() {
	if fConfig.DefaultTenantLimitQuota <= 0 {
		fConfig.DefaultTenantLimitQuota = defaultTenantLimitQuota
	}
	log.GetLogger().Infof("defaultTenantLimitQuota %d", fConfig.DefaultTenantLimitQuota)
}

func initDefaultHeartbeatConfig() {
	if fConfig.HeartbeatConfig == nil {
		fConfig.HeartbeatConfig = &types.HeartbeatConfig{}
	}
	if fConfig.HeartbeatConfig.HeartbeatTimeout <= 0 {
		fConfig.HeartbeatConfig.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if fConfig.HeartbeatConfig.HeartbeatInterval <= 0 {
		fConfig.HeartbeatConfig.HeartbeatInterval = defaultHeartbeatInterval
	}
	if fConfig.HeartbeatConfig.HeartbeatTimeoutThreshold <= 0 {
		fConfig.HeartbeatConfig.HeartbeatTimeoutThreshold = defaultHeartbeatTimeoutThreshold
	}
}

func initWatchConfig(config *types.Config, stopCh <-chan struct{}) error {
	upgradecompatible.SetAccessFaaSSchedulerType(config.AccessFaaSSchedulerType)
	if config.WatchedConfigFilePath == "" {
		return nil
	}
	err := upgradecompatible.WatchConfig(config.WatchedConfigFilePath, stopCh)
	if err != nil {
		return fmt.Errorf("watch file [%s] failed, err: %v", config.WatchedConfigFilePath, err)
	}
	return nil
}
