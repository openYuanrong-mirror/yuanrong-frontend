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

package frontendsdk

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/valyala/fasthttp"

	"yuanrong.org/kernel/runtime/libruntime/api"
	"yuanrong.org/kernel/runtime/libruntime/common"
	"yuanrong.org/kernel/runtime/posixsdk"

	"frontend/pkg/common/faas_common/datasystemclient"
	"frontend/pkg/common/faas_common/instanceconfig"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/sts"
	"frontend/pkg/common/faas_common/sts/raw"
	commontypes "frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/common/faas_common/wisecloudtool/serviceaccount"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/functionmeta"
	"frontend/pkg/frontend/instanceconfigmanager"
	"frontend/pkg/frontend/instancemanager"
	"frontend/pkg/frontend/invocation"
	"frontend/pkg/frontend/responsehandler"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/subscriber"
	"frontend/pkg/frontend/types"
	"frontend/pkg/frontend/watcher"
	"frontend/pkg/frontend/wisecloud"
)

var (
	stopCh            = make(chan struct{})
	systemAuthDataKey string
)

const (
	initArgsFilePathEnvKey = "INIT_ARGS_FILE_PATH"
	dataSystemAddr         = "DATASYSTEM_ADDR"
	dsWorkerPort           = "31501"
	functionSystemPort     = "32568"
	functionName           = "0/0-system-faasfrontend/$latest"
	runtimeID              = "faas_frontend_libruntime"
	instanceIDPrefix       = "driver-faas-frontend"
	logFileName            = "frontendsdk"
	logConfigKey           = "LOG_CONFIG"
	localHostAddr          = "127.0.0.1"
	defaultJobID           = "12345678"
)

const (
	podName = "POD_NAME"
	podIP   = "POD_IP"
	nodeIP  = "NODE_IP"
)

// Frontend - FrontendAPI implement
type Frontend struct{}

// Init - init frontend SDK
func (f *Frontend) Init(configFilePath string) error {
	if configFilePath == "" {
		return fmt.Errorf("config file path is empty")
	}
	runtimeConfig, err := parseRuntimeCfgAndSetEnv(configFilePath)
	if err != nil {
		return fmt.Errorf("parse runtime config error: %s", err.Error())
	}
	err = log.InitRunLog(logFileName, true)
	if err != nil {
		return fmt.Errorf("init logger error, err %s", err.Error())
	}
	funcExecution := posixsdk.NewSDKPosixFuncExecutionWithHandler(posixsdk.RegisterHandler{
		InitHandler: initSDKHandler,
	})
	err = posixsdk.InitRuntime(runtimeConfig, funcExecution)
	if err != nil {
		utils.SafeCloseChannel(stopCh)
		log.GetLogger().Errorf("init runtime failed: %s", err.Error())
		return err
	}
	go posixsdk.Run()
	wisecloud.NewColdStartProvider(&config.GetConfig().WiseCloudConfig.ServiceAccountJwt)
	setRawStsAuthConfig(runtimeConfig)
	log.GetLogger().Infof("init frontend sdk successfully")
	return nil
}

func setEnv(configFilePath string, cfg *types.Config) error {
	logConfig, err := json.Marshal(cfg.Runtime.LogConfig)
	if err != nil {
		return err
	}
	err = os.Setenv(logConfigKey, string(logConfig))
	if err != nil {
		return err
	}
	err = os.Setenv(initArgsFilePathEnvKey, configFilePath)
	if err != nil {
		return err
	}
	err = os.Setenv(dataSystemAddr, fmt.Sprintf("%s:%s", os.Getenv(nodeIP), dsWorkerPort))
	if err != nil {
		return err
	}
	return nil
}

func parseRuntimeCfgAndSetEnv(configFilePath string) (*common.Configuration, error) {
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("read config failed, err %s", err.Error())
	}
	cfg := &types.Config{}
	err = json.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("unmarshal config failed, err %s", err.Error())
	}
	jobID := os.Getenv(podName)
	if jobID == "" {
		jobID = defaultJobID
	}
	fsAddr := os.Getenv(podIP)
	if fsAddr == "" {
		fsAddr = localHostAddr
	}
	runtimeCfg := &common.Configuration{
		RuntimeID:               runtimeID,
		InstanceID:              fmt.Sprintf("%s-%s", instanceIDPrefix, jobID),
		FunctionName:            functionName,
		LogLevel:                cfg.Runtime.LogConfig.Level,
		FSAddress:               fmt.Sprintf("%s:%s", fsAddr, functionSystemPort),
		VerifyFilePath:          cfg.VerifyFilePath,
		EnableEvent:             cfg.EnableEvent,
		LogPath:                 cfg.Runtime.LogConfig.FilePath,
		JobID:                   jobID,
		DriverMode:              true,
		MaxConcurrencyCreateNum: 5000,
		EnableSigaction:         cfg.Runtime.EnableSigaction,
	}
	if err = sts.InitStsSDK(cfg.RawStsConfig.ServerConfig); err != nil {
		return nil, err
	}
	if err = parseSystemAuth(cfg, runtimeCfg); err != nil {
		return nil, err
	}
	err = setEnv(configFilePath, cfg)
	if err != nil {
		return nil, err
	}
	err = parseServiceAccountJwt(cfg)
	if err != nil {
		return nil, err
	}
	return runtimeCfg, nil
}

func parseSystemAuth(cfg *types.Config, runtimeCfg *common.Configuration) error {
	encryptedKeyConfig := raw.Auth{
		EnableIam: strconv.FormatBool(cfg.Runtime.SystemAuthConfig.Enable),
		AccessKey: cfg.Runtime.SystemAuthConfig.AccessKey,
		SecretKey: cfg.Runtime.SystemAuthConfig.SecretKey,
		DataKey:   cfg.Runtime.SystemAuthConfig.DataKey,
	}
	decryptedKeyConfig := sts.DecryptSystemAuthConfig(encryptedKeyConfig)
	runtimeCfg.SystemAuthAccessKey = decryptedKeyConfig.AccessKey
	runtimeCfg.SystemAuthSecretKey = decryptedKeyConfig.SecretKey
	systemAuthDataKey = decryptedKeyConfig.DataKey
	return nil
}

func setRawStsAuthConfig(runtimeCfg *common.Configuration) {
	config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.AccessKey = runtimeCfg.SystemAuthAccessKey
	config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.SecretKey = runtimeCfg.SystemAuthSecretKey
	config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.DataKey = systemAuthDataKey
}

func parseServiceAccountJwt(cfg *types.Config) error {
	if cfg.RawStsConfig.StsEnable && len(cfg.WiseCloudConfig.ServiceAccountJwt.ServiceAccountKeyStr) > 0 {
		var err error
		cfg.WiseCloudConfig.ServiceAccountJwt.ServiceAccount, err =
			serviceaccount.ParseServiceAccount(cfg.WiseCloudConfig.ServiceAccountJwt.ServiceAccountKeyStr)
		if err != nil {
			return err
		}
		config.GetConfig().WiseCloudConfig.ServiceAccountJwt.ServiceAccount =
			cfg.WiseCloudConfig.ServiceAccountJwt.ServiceAccount
	} else {
		return nil
	}
	if cfg.WiseCloudConfig.ServiceAccountJwt.TlsConfig != nil &&
		len(cfg.WiseCloudConfig.ServiceAccountJwt.TlsConfig.TlsCipherSuitesStr) > 0 {
		var err error
		cfg.WiseCloudConfig.ServiceAccountJwt.TlsConfig.TlsCipherSuites, err =
			serviceaccount.ParseTlsCipherSuites(cfg.WiseCloudConfig.ServiceAccountJwt.TlsConfig.TlsCipherSuitesStr)
		if err != nil {
			return err
		}
		config.GetConfig().WiseCloudConfig.ServiceAccountJwt.TlsConfig =
			cfg.WiseCloudConfig.ServiceAccountJwt.TlsConfig
	} else {
		return nil
	}
	return nil
}

// InvokeHandler -
func (f *Frontend) InvokeHandler(ctx *InvokeProcessContext) error {
	return invocation.InvokeHandler(ctx)
}

// UploadWithKeyRetry -
func (f *Frontend) UploadWithKeyRetry(value []byte, config *Config, param SetParam, traceID string) (string, error) {
	return datasystemclient.UploadWithKeyRetry(value, config, param, traceID)
}

// DownloadArrayRetry -
func (f *Frontend) DownloadArrayRetry(keys []string, config *Config, traceID string) ([][]byte, error) {
	return datasystemclient.DownloadArrayRetry(keys, config, traceID)
}

// DeleteArrayRetry -
func (f *Frontend) DeleteArrayRetry(keys []string, config *Config, traceID string) ([]string, error) {
	return datasystemclient.DeleteArrayRetry(keys, config, traceID)
}

// SubscribeStream -
func (f *Frontend) SubscribeStream(param SubscribeParam, ctx StreamCtx) error {
	return datasystemclient.SubscribeStream(param, ctx)
}

func initSDKHandler(args []api.Arg, rt api.LibruntimeAPI) ([]byte, error) {
	var err error
	if err = config.InitFunctionConfig(args[0].Data); err != nil {
		log.GetLogger().Errorf("init frontend config fail, err: %s", err)
		return []byte{}, err
	}
	if err = config.InitEtcd(stopCh); err != nil {
		log.GetLogger().Errorf("failed to init etcd ,err:%s", err.Error())
		return []byte{}, err
	}

	if err = watcher.StartWatch(stopCh); err != nil {
		log.GetLogger().Errorf("failed to watch etcd ,err:%s", err.Error())
		return []byte{}, err
	}
	initSubscribe()
	schedulerproxy.Proxy.RTAPI = rt
	schedulerproxy.BlueProxy.RTAPI = rt
	util.SetAPIClientLibruntime(rt)
	util.InitializeDirectProxyRouting()
	datasystemclient.SetStreamEnable(config.GetConfig().StreamEnable)
	datasystemclient.InitDataSystemLibruntime(config.GetConfig().DataSystemConfig, rt, stopCh)
	responsehandler.Handler = (&invocation.FGAdapter{}).MakeResponseHandler()
	return []byte{}, nil
}

func initSubscribe() {
	functionmeta.GetFunctionMetaDataSubject().Subscribe(&subscriber.Observer{
		Update: func(data interface{}) {},
		Delete: func(data interface{}) {
			functionMeta, ok := data.(*commontypes.FuncSpec)
			if !ok {
				return
			}
			wisecloud.GetMetricsManager().ProcessFunctionDelete(functionMeta)
			wisecloud.GetQueueManager().ProcessFunctionDelete(functionMeta)
		},
	})
	functionmeta.GetFunctionMetaDataSubject().StartLoop(stopCh)

	instanceconfigmanager.GetInstanceConfigSubject().Subscribe(&subscriber.Observer{
		Update: func(data interface{}) {},
		Delete: func(data interface{}) {
			insConfig, ok := data.(*instanceconfig.Configuration)
			if !ok {
				return
			}
			wisecloud.GetMetricsManager().ProcessInsConfigDelete(insConfig)
			wisecloud.GetQueueManager().ProcessInsConfigDelete(insConfig)
		},
	})
	instanceconfigmanager.GetInstanceConfigSubject().StartLoop(stopCh)

	instancemanager.GetInstanceSubject().Subscribe(&subscriber.Observer{
		Update: func(data interface{}) {
			instance, ok := data.(*commontypes.InstanceSpecification)
			if !ok {
				return
			}
			wisecloud.GetQueueManager().ProcessInstanceUpdate(instance)
		},
		Delete: func(data interface{}) {
			instance, ok := data.(*commontypes.InstanceSpecification)
			if !ok {
				return
			}
			wisecloud.GetMetricsManager().ProcessInstanceDelete(instance)
		},
	})
	instancemanager.GetInstanceSubject().StartLoop(stopCh)
}

// ExecShutdownHandler -
func (f *Frontend) ExecShutdownHandler(signum int) {
	log.GetLogger().Infof("recv signal %d", signum)
	log.GetLogger().Sync()

	defer func() {
		log.GetLogger().Infof("recv signal %d over", signum)
		log.GetLogger().Sync()
	}()
	posixsdk.ExecShutdownHandler(signum)
}

// CheckFrontendIsHealth -
func (f *Frontend) CheckFrontendIsHealth() bool {
	return util.NewClient().IsHealth()
}

// CheckLocalDataSystemStatusReady 检查本节点数据系统状态是否为ready
func (f *Frontend) CheckLocalDataSystemStatusReady() bool {
	return !config.GetConfig().StreamEnable || datasystemclient.IsLocalDataSystemStatusReady()
}

// Auth -
func (f *Frontend) Auth(ctx *fasthttp.RequestCtx, ak string, hexSk []byte) bool {
	sk, err := hex.DecodeString(string(hexSk))
	if err != nil {
		sk = hexSk
	}
	return wisecloud.Auth(ctx, ak, sk)
}

// NewFrontend -
func NewFrontend() FrontendAPI {
	return &Frontend{}
}
