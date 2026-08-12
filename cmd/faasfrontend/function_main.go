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

// Package main -
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "go.uber.org/automaxprocs"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/datasystemclient"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/monitor"
	"frontend/pkg/common/faas_common/tracer"
	"frontend/pkg/common/faas_common/urnutils"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/invocation"
	"frontend/pkg/frontend/metrics"
	"frontend/pkg/frontend/middleware"
	"frontend/pkg/frontend/responsehandler"
	"frontend/pkg/frontend/sandboxrouter"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/server"
	"frontend/pkg/frontend/sshproxy"
	"frontend/pkg/frontend/state"
	"frontend/pkg/frontend/watcher"
)

var stopCh = make(chan struct{})

const (
	defaultArgsLength = 5
	defaultFileMode   = 0o640
	serverNum         = 2
)

// InitHandlerLibruntime is the init handler called by runtime
func InitHandlerLibruntime(args []api.Arg, rt api.LibruntimeAPI) ([]byte, error) {
	log.SetupLoggerLibruntime(rt.GetFormatLogger())
	var err error
	defer func() {
		if err != nil {
			fmt.Printf("panic, module: faasfrontend, err: %s\n", err.Error())
			log.GetLogger().Errorf("panic, module: faasfrontend, err: %s", err.Error())
		}
		log.GetLogger().Sync()
	}()
	if err = config.InitFunctionConfig(args[0].Data); err != nil {
		log.GetLogger().Errorf("init frontend config fail, err: %s", err)
		return []byte{}, err
	}
	if err = config.InitEtcd(stopCh); err != nil {
		log.GetLogger().Errorf("failed to init etcd ,err:%s", err.Error())
		return []byte{}, err
	}
	if err = watcher.StartWatch(stopCh); err != nil {
		log.GetLogger().Errorf("failed to watch etcd, err:%s", err.Error())
		return []byte{}, err
	}
	state.InitState()
	var stateByte []byte
	stateByte, err = state.GetStateByte()
	if err == nil && len(stateByte) != 0 {
		return []byte{}, RecoverHandlerLibruntime(stateByte, rt)
	}
	cfg := config.GetConfig()
	state.Update(cfg)
	if err = setupFaaSFrontendLibruntime(rt, stopCh); err != nil {
		return []byte{}, err
	}
	config.ClearSensitiveInfo()
	return []byte{}, nil
}

// CallHandlerLibruntime handles the invoke request between in-cloud faas functions
// the posix args are all value only (type=0, no object ref) args, the convention:
// args[0]: target faas function name
// args[1]: target faas service name
// args[2]: target tenant id
// args[3]: target function version
// args[4]: invoke payload to the target function
func CallHandlerLibruntime(argsLibrt []api.Arg) ([]byte, error) {
	if len(argsLibrt) < defaultArgsLength {
		return nil, fmt.Errorf("invalid call with num of argsLibrt %d", len(argsLibrt))
	}

	req := InCloudFunctionInvokeRequest{
		functionName:    string(argsLibrt[0].Data),
		serviceName:     string(argsLibrt[1].Data),
		tenantID:        string(argsLibrt[2].Data),
		functionVersion: string(argsLibrt[3].Data),
		invokePayload:   argsLibrt[4].Data,
	}
	resp := innerInvoke(req)
	b, err := json.Marshal(resp)
	if err != nil {
		return []byte{}, err
	}
	return b, nil
}

// InCloudFunctionInvokeRequest -
type InCloudFunctionInvokeRequest struct {
	functionName    string
	serviceName     string
	tenantID        string
	functionVersion string
	invokePayload   []byte
}

// InCloudFunctionInvokeResponse -
type InCloudFunctionInvokeResponse struct {
	Code    int
	Message string
}

func innerInvoke(request InCloudFunctionInvokeRequest) InCloudFunctionInvokeResponse {
	return InCloudFunctionInvokeResponse{
		Code:    0,
		Message: "Successful in-cloud invoke",
	}
}

// CheckpointHandlerLibruntime is the checkpoint handler called by runtime
func CheckpointHandlerLibruntime(checkpointID string) ([]byte, error) {
	return state.GetStateByte()
}

func initStateAndConfig(stateData []byte) error {
	var err error
	log.GetLogger().Infof("trigger: faasfrontend.RecoverHandler")
	if err = state.SetState(stateData); err != nil {
		log.GetLogger().Errorf("recover frontend error:%s", err.Error())
		return fmt.Errorf("faaS frontend recover error:%s", err.Error())
	}
	state.InitState()
	cfg := config.GetConfig()
	state.Update(cfg)
	return nil
}

// RecoverHandlerLibruntime is the recover handler called by runtime
func RecoverHandlerLibruntime(stateData []byte, rt api.LibruntimeAPI) error {
	var err error
	log.SetupLoggerLibruntime(rt.GetFormatLogger())
	err = initStateAndConfig(stateData)
	if err != nil {
		return err
	}
	if err = setupFaaSFrontendLibruntime(rt, stopCh); err != nil {
		log.GetLogger().Errorf("restart initHandler error:%s", err.Error())
		return fmt.Errorf("faaS frontend restart initHandler error:%s", err.Error())
	}
	config.ClearSensitiveInfo()
	return nil
}

// ShutdownHandlerLibruntime is the shutdown handler called by runtime
func ShutdownHandlerLibruntime(gracePeriodSecond uint64) error {
	log.GetLogger().Infof("trigger: faasfrontend.ShutdownHandler")
	utils.SafeCloseChannel(stopCh)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.GracefulShutdown(server.GetHTTPServer())
		wg.Done()
	}()
	// Stop Prometheus metrics server
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(gracePeriodSecond)*time.Second)
	defer cancel()
	if err := metrics.StopPrometheusServer(ctx); err != nil {
		log.GetLogger().Warnf("failed to stop Prometheus metrics server: %v", err)
	}
	wg.Wait()
	log.GetLogger().Infof("faasfrontendLibruntime exit")
	log.GetLogger().Sync()
	return nil
}

// SignalHandlerLibruntime is the signal handler called by runtime
func SignalHandlerLibruntime(signal int, payload []byte) error {
	return nil
}

// HealthCheckHandlerLibruntime reports the frontend control-plane function health to libruntime.
func HealthCheckHandlerLibruntime() (api.HealthType, error) {
	return api.Healthy, nil
}

func setupFaaSFrontendLibruntime(rt api.LibruntimeAPI, stopChLibrt <-chan struct{}) error {
	util.SetAPIClientLibruntime(rt)
	util.InitializeDirectProxyRouting()
	schedulerproxy.Proxy.RTAPI = rt
	schedulerproxy.BlueProxy.RTAPI = rt
	shutdown := func() {}
	go tracer.InitCommonTracer(shutdown, "frontend")
	cfg := config.GetConfig()
	enableStream := os.Getenv(constant.EnableStream)
	if strings.ToLower(enableStream) == "true" {
		datasystemclient.SetStreamEnable(true)
	}
	datasystemclient.InitDataSystemLibruntime(cfg.DataSystemConfig, rt, stopChLibrt)
	monitor.SetMemoryControlConfig(cfg.MemoryControlConfig)
	if err := monitor.InitMemMonitor(stopCh); err != nil {
		log.GetLogger().Errorf("failed to init mem monitor")
		return err
	}
	if err := assembleAdapter(); err != nil {
		return err
	}
	httpServer := server.CreateHTTPServer()
	go func() {
		err := server.Start(httpServer, stopCh)
		if err != nil {
			log.GetLogger().Errorf("start faas frontend server failed will exit, err:%s", err.Error())
			rt.Exit(0, "")
		}
	}()
	if err := sshproxy.Start(stopCh); err != nil {
		return fmt.Errorf("start SSH frontend: %w", err)
	}

	// Start Prometheus metrics server if configured
	if cfg.HTTPConfig != nil && cfg.HTTPConfig.PrometheusMetricsPort > 0 {
		metricsAddress := fmt.Sprintf("%s:%d", config.GetConfig().HTTPConfig.ServerListenIP,
			cfg.HTTPConfig.PrometheusMetricsPort)
		if err := metrics.StartPrometheusServer(metricsAddress, "/metrics"); err != nil {
			log.GetLogger().Warnf("failed to start Prometheus metrics server: %v", err)
		} else {
			log.GetLogger().Infof("Prometheus metrics server started on port %d", cfg.HTTPConfig.PrometheusMetricsPort)
		}
	}

	if err := sandboxrouter.StartIfEnabled(config.GetConfig().SandboxRouter, stopCh); err != nil {
		return err
	}

	return nil
}

func assembleAdapter() error {
	switch config.GetConfig().BusinessType {
	case constant.BusinessTypeFG:
		urnutils.SetSeparator(urnutils.TenantProductSplitStr)
		fgAdapter := &invocation.FGAdapter{}
		responsehandler.Handler = fgAdapter.MakeResponseHandler()
		middleware.Invoker = fgAdapter.MakeInvoker()
	default:
		log.GetLogger().Errorf("Not support businessType")
		return errors.New("assembleAdapter error,not support businessType")
	}
	return nil
}
