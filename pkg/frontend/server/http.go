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

// Package server -
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/healthlog"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/tls"
	"frontend/pkg/frontend/serverstatus"

	"frontend/pkg/frontend/api"
	"frontend/pkg/frontend/common"
	"frontend/pkg/frontend/common/httpconstant"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/functiontask"
	"frontend/pkg/frontend/middleware"
	"frontend/pkg/frontend/selfregister"
)

const (
	logFileName    = "frontend"
	idleTimeOffset = 10
)

var (
	server      *http.Server
	activeConns int64
	wg          sync.WaitGroup
)

// CreateHTTPServer -
func CreateHTTPServer() *http.Server {
	listenIP := config.GetConfig().HTTPConfig.ServerListenIP
	if len(listenIP) == 0 {
		listenIP = os.Getenv(constant.PodIPEnvKey)
		log.GetLogger().Warnf("failed to get pod ip from HTTPConfig, try to use %s as listen IP", listenIP)
	}
	if len(listenIP) == 0 {
		log.GetLogger().Errorf("failed to get pod ip from env POD_IP")
		return nil
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.RedirectTrailingSlash = false
	engine.Use(gin.Recovery())
	api.InitRoute(engine)
	server = &http.Server{
		Handler:      allowQuerySemicolons(engine),
		ReadTimeout:  time.Duration(config.GetConfig().HTTPConfig.ServerReadTimeout) * time.Second,
		WriteTimeout: time.Duration(config.GetConfig().HTTPConfig.ServerWriteTimeout) * time.Second,
		Addr:         fmt.Sprintf("%s:%d", listenIP, config.GetConfig().HTTPConfig.ServerListenPort),
		ConnState:    recordConn,
	}
	return server
}

func recordConn(conn net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		atomic.AddInt64(&activeConns, 1)
		wg.Add(1)
	case http.StateClosed, http.StateHijacked:
		atomic.AddInt64(&activeConns, -1)
		wg.Done()
	default:
		return
	}
}

// GetHTTPServer -
func GetHTTPServer() *http.Server {
	return server
}

// Start starts the HTTP server. Process entrypoints own watcher startup.
func Start(server *http.Server, stopCh <-chan struct{}) error {
	// starts to listen and serve
	if server == nil {
		return errors.New("http server is nil")
	}
	log.GetLogger().Infof("FaaS-Frontend HTTP server starting on %s", server.Addr)
	if err := selfregister.RegisterFrontendInstanceToEtcd(stopCh); err != nil {
		return err
	}

	if config.GetConfig().HTTPSConfig != nil && config.GetConfig().HTTPSConfig.HTTPSEnable {
		err := tls.InitTLSConfig(*config.GetConfig().HTTPSConfig)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return err
		}
		server.TLSConfig = tls.GetClientTLSConfig()
		err = server.ListenAndServeTLS("", "")
		if err != nil {
			log.GetLogger().Errorf("error when https ListenAndServeTLS: %s", err.Error())
			return err
		}
	} else {
		if err := server.ListenAndServe(); err != nil {
			log.GetLogger().Errorf("error when http ListenAndServe: %s", err.Error())
			return err
		}
	}
	go healthlog.PrintHealthLog(stopCh, printInputLog, logFileName)
	return nil
}

// GracefulShutdown Shutdown Gracefully
func GracefulShutdown(httpServer *http.Server) {
	if httpServer == nil {
		log.GetLogger().Infof("http server is not initialize, no need to shutdown")
		return
	}
	log.GetLogger().Infof("http server start graceful shutdown")
	serverstatus.Shutdown()
	// wait long-connections closed
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.GetLogger().Infof("all connections closed gracefully")
	case <-time.After(time.Duration(config.GetConfig().HTTPConfig.ClientIdleTimeout+idleTimeOffset) * time.Second):
		log.GetLogger().Infof("timeout reached, forcing shutdown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), common.GracefulShutdownTimeOut)
	defer cancel()
	defer func() {
		err := httpServer.Shutdown(ctx)
		if err != nil {
			log.GetLogger().Errorf("http server shutdown error")
		}
	}()
	middleware.GraceExit()
	log.GetLogger().Infof("http server shutdown after processing graceful exit")
}

// getReadBufferSize get the default read buffer size for server and client
func getReadBufferSize() int {
	return httpconstant.DefaultGraphReadBufferSize
}

func allowQuerySemicolons(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, ";") {
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.RawQuery = strings.ReplaceAll(r.URL.RawQuery, ";", httpconstant.SemicolonReplacer)
			h.ServeHTTP(w, r2)
		} else {
			h.ServeHTTP(w, r)
		}
	})
}

func printInputLog() {
	busProxyNum := functiontask.GetBusProxies().GetNum()
	log.GetLogger().Infof("%s is alive. The number of busProxy is %d", logFileName, busProxyNum)
}
