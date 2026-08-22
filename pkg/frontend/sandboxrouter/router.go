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

// Package sandboxrouter assembles the sandbox router: an InstanceInfo-watch
// resolver behind a reverse-proxy data plane, started on its own port alongside
// the frontend.
package sandboxrouter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/sandboxrouter/config"
	"frontend/pkg/frontend/sandboxrouter/proxy"
	"frontend/pkg/frontend/sandboxrouter/resolver"
)

const shutdownTimeout = 10 * time.Second

// Router owns the resolver and the HTTP server for the sandbox data plane.
type Router struct {
	cfg        *config.SandboxRouterConfig
	resolver   *resolver.InstanceInfoWatchResolver
	httpServer *http.Server
}

// StartIfEnabled builds and starts the sandbox router in a background goroutine.
// A nil config means "use the default enabled sandbox router"; an explicit
// config with Enabled=false remains a no-op. It is called from each frontend
// entrypoint (which are compiled as separate mains), so the shared start logic
// lives here rather than in a cmd-local helper. The router etcd client must
// already be initialised by InitEtcd before this is called.
func StartIfEnabled(cfg *config.SandboxRouterConfig, stopCh <-chan struct{}) error {
	cfg = effectiveConfig(cfg)
	if !cfg.Enabled {
		return nil
	}
	router, err := New(cfg)
	if err != nil {
		return err
	}
	go func() {
		if err := router.Start(stopCh); err != nil {
			log.GetLogger().Errorf("sandbox router exited with error: %s", err.Error())
		}
	}()
	return nil
}

func effectiveConfig(cfg *config.SandboxRouterConfig) *config.SandboxRouterConfig {
	if cfg == nil {
		return &config.SandboxRouterConfig{Enabled: true}
	}
	return cfg
}

// New builds a Router from cfg, applying defaults. It does not start anything.
func New(cfg *config.SandboxRouterConfig) (*Router, error) {
	if cfg == nil {
		return nil, errors.New("sandboxrouter: nil config")
	}
	cfg.ApplyDefaults()

	res := resolver.NewInstanceInfoWatchResolverWithTimeout(
		time.Duration(cfg.ResolveTimeoutMs) * time.Millisecond)
	server := proxy.New(res)
	server.SetAuth(cfg.EnableJWTAuth, cfg.ValidateIAM, uint16(cfg.RRTPort), uint16(cfg.TunnelPort))

	backendTLS, err := cfg.BackendTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("sandboxrouter: backend TLS config: %w", err)
	}
	server.SetHTTPSTransport(&http.Transport{TLSClientConfig: backendTLS})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.ListenIP, cfg.ListenPort),
		Handler: server,
		// No WriteTimeout: WebSocket/SSE/long-lived streams must not be cut off.
		IdleTimeout: time.Duration(cfg.IdleTimeoutSeconds) * time.Second,
	}
	return &Router{cfg: cfg, resolver: res, httpServer: httpServer}, nil
}

// Start launches the resolver watch and serves until stopCh is closed, then
// shuts down gracefully. It blocks; callers run it in a goroutine.
func (r *Router) Start(stopCh <-chan struct{}) error {
	if err := r.resolver.Start(stopCh); err != nil {
		return fmt.Errorf("sandboxrouter: start resolver: %w", err)
	}

	go func() {
		<-stopCh
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := r.httpServer.Shutdown(ctx); err != nil {
			log.GetLogger().Warnf("sandboxrouter: shutdown: %s", err.Error())
		}
	}()

	var err error
	if r.cfg.EnableTLSClientAuth {
		tlsCfg, buildErr := buildClientAuthTLS(r.cfg)
		if buildErr != nil {
			return buildErr
		}
		r.httpServer.TLSConfig = tlsCfg
		log.GetLogger().Infof("sandboxrouter: listening (mTLS) on %s", r.httpServer.Addr)
		err = r.httpServer.ListenAndServeTLS(r.cfg.TLSCertFile, r.cfg.TLSKeyFile)
	} else {
		log.GetLogger().Infof("sandboxrouter: listening on %s", r.httpServer.Addr)
		err = r.httpServer.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// buildClientAuthTLS builds a TLS config that requires and verifies a client
// certificate signed by the configured CA.
func buildClientAuthTLS(cfg *config.SandboxRouterConfig) (*tls.Config, error) {
	if cfg.TLSCAFile == "" || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, errors.New("sandboxrouter: mTLS requires tlsCAFile, tlsCertFile and tlsKeyFile")
	}
	caPEM, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("sandboxrouter: read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("sandboxrouter: no valid certificates in CA file %s", cfg.TLSCAFile)
	}
	return &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		MinVersion: tls.VersionTLS12,
	}, nil
}
