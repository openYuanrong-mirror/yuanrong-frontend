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

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"frontend/pkg/common/faas_common/autogc"
	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/monitor"
	"frontend/pkg/common/faas_common/signals"
	"frontend/pkg/common/faas_common/tracer"
	"frontend/pkg/common/faas_common/urnutils"
	"frontend/pkg/frontend/asyncinvocation"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/functiontask"
	"frontend/pkg/frontend/invocation"
	"frontend/pkg/frontend/metrics"
	"frontend/pkg/frontend/middleware"
	"frontend/pkg/frontend/responsehandler"
	"frontend/pkg/frontend/sandboxrouter"
	"frontend/pkg/frontend/server"
	"frontend/pkg/frontend/sshproxy"
	"frontend/pkg/frontend/stream"
	"frontend/pkg/frontend/watcher"
)

const (
	logFileName = "frontend"
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	// init logger config
	err := log.InitRunLog(logFileName, true)
	if err != nil {
		fmt.Print("init logger error: " + err.Error())
		return
	}
	autogc.InitAutoGOGC()
	shutdown := func() { fmt.Println("common tracer is not initialized") }
	go tracer.InitCommonTracer(shutdown, "frontend")
	defer func() {
		if shutdown != nil {
			shutdown()
		}
	}()
	// init constant
	err = config.InitModuleConfig()
	if err != nil {
		logAndPrintError(fmt.Sprintf("init module config error: %s", err.Error()))
		return
	}
	util.InitializeDirectProxyRouting()
	// Fix Critical #4: Load async invocation config
	asyncinvocation.LoadConfigFromMain(config.GetConfig())
	urnutils.SetSeparator(config.GetConfig().FunctionNameSeparator)
	stopCh := signals.WaitForSignal()
	if err = config.InitEtcd(stopCh); err != nil {
		logAndPrintError(fmt.Sprintf("init etcd error: %s", err.Error()))
		return
	}
	if err = watcher.StartWatch(stopCh); err != nil {
		logAndPrintError(fmt.Sprintf("start etcd watcher error: %s", err.Error()))
		return
	}

	go metrics.StartReportMetrics(stopCh)
	err = setupModuleFrontend(stopCh)
	if err != nil {
		logAndPrintError(fmt.Sprintf("setup module frontend error: %s", err.Error()))
		return
	}
	if err = sshproxy.Start(stopCh); err != nil {
		logAndPrintError(fmt.Sprintf("start SSH frontend error: %s", err.Error()))
		return
	}
	// 流监听
	if err := stream.StartListenFrontendResponseStream(stopCh); err != nil {
		log.GetLogger().Warnf("failed to listen frontend response stream, err: %s", err.Error())
	}
	startModuleHTTPServer(stopCh)
}

func startModuleHTTPServer(stopCh <-chan struct{}) {
	errChan := make(chan error, 1)
	httpServer := server.CreateHTTPServer()
	go func() {
		if err := server.Start(httpServer, stopCh); err != nil {
			errChan <- err
			logAndPrintError(fmt.Sprintf("start http server error: %s", err.Error()))
		}
	}()
	if err := waitShutdown(httpServer, stopCh, errChan); err != nil {
		logAndPrintError(fmt.Sprintf("wait http server error: %s", err.Error()))
	}
}

func logAndPrintError(errMessage string) {
	log.GetLogger().Errorf(errMessage)
	fmt.Println(errMessage)
}

func waitShutdown(server *http.Server, stopCh <-chan struct{}, errChan <-chan error) error {
	if server == nil {
		return errors.New("http server is nil")
	}
	if stopCh == nil || errChan == nil {
		return errors.New("input channel is nil")
	}
	select {
	case <-stopCh:
		log.GetLogger().Infof("received termination signal")
		ctx, cancel := context.WithTimeout(context.Background(), constant.DefaultServerWriteTimeOut)
		defer cancel()
		return server.Shutdown(ctx)
	case err := <-errChan:
		return err
	}
}

func setupModuleFrontend(stopCh <-chan struct{}) error {
	updateConfig()
	if err := config.WatchConfig(config.ConfigFilePath, stopCh, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig %s failed, err %s", config.ConfigFilePath, err.Error())
	}
	if err := monitor.InitMemMonitor(stopCh); err != nil {
		log.GetLogger().Errorf("failed to init mem monitor")
		return err
	}
	fgAdapter := &invocation.FGAdapter{}
	responsehandler.Handler = fgAdapter.MakeResponseHandler()
	middleware.Invoker = fgAdapter.MakeInvoker()
	if err := sandboxrouter.StartIfEnabled(config.GetConfig().SandboxRouter, stopCh); err != nil {
		return err
	}
	return nil
}

func updateConfig() {
	cfg := config.GetConfig()
	monitor.SetMemoryControlConfig(cfg.MemoryControlConfig)
	functiontask.GetBusProxies().UpdateConfig()
}
