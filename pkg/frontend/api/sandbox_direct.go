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

package api

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/tracer"
	frontendhttputil "frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/middleware"
	routerconfig "frontend/pkg/frontend/sandboxrouter/config"
)

const (
	sandboxDirectPrefix       = "/direct"
	sandboxTunnelPrefix       = "/tunnel"
	sandboxInternalSrcKey     = "X-Internal-Src"
	sandboxInternalSrcValue   = "1"
	sandboxInternalProxyValue = "2"
	sandboxInternalTenantKey  = "X-Internal-Tenant"
	sandboxDefaultRRTPort     = 50090
	sandboxControlPathParts   = 2
)

// registerSandboxDirectRoute exposes the RRT direct-invoke route on the normal
// frontend endpoint. When frontend JWT is enabled, the proxy forwards only the
// validated tenant identity. When it is disabled, authentication is deferred
// to the local sandboxRouter. In both cases RRT never sees platform credentials.
func registerSandboxDirectRoute(r *gin.Engine) {
	r.Any(sandboxDirectPrefix, sandboxTraceHandler(sandboxDirectProxyHandler(sandboxDirectRouterPath, true)))
	r.Any(sandboxDirectPrefix+"/*proxyPath", sandboxTraceHandler(sandboxDirectProxyHandler(sandboxDirectRouterPath, true)))
	r.Any(sandboxTunnelPrefix, sandboxDirectProxyHandler(sandboxTunnelRouterPath, false))
	r.Any(sandboxTunnelPrefix+"/*proxyPath", sandboxDirectProxyHandler(sandboxTunnelRouterPath, false))
}

// agentTraceMiddleware resolves the request trace-id via InitTraceID (external
// X-Trace-Id / X-Request-Id header if present, otherwise a freshly minted UUID) and
// echoes it back on the response header so the caller always observes the value.
// Used both as the /api/agent group middleware and inside traceHandler, so every
// agent handler can assume X-Trace-Id is non-empty. As a group middleware it must
// NOT call c.Next() explicitly — returning lets the chain continue.
func agentTraceMiddleware(c *gin.Context) {
	traceID := frontendhttputil.InitTraceID(c)
	c.Header(constant.HeaderTraceID, traceID)
}

// traceHandler wraps a gin handler with the trace middleware plus the tracer span.
func traceHandler(handler gin.HandlerFunc) gin.HandlerFunc {
	wrapped := tracer.WrapGinHandler(handler)
	return func(c *gin.Context) {
		traceID := frontendhttputil.InitTraceID(c)
		c.Header(constant.HeaderTraceID, traceID)
		wrapped(c)
	}
}

// traceHTTPHandler adapts a plain net/http handler (e.g. a gorilla/websocket
// upgrade handler wrapped by gin.WrapF) to the same trace wiring.
func traceHTTPHandler(handler http.HandlerFunc) gin.HandlerFunc {
	wrapped := tracer.WrapGinHandler(gin.WrapF(handler))
	return func(c *gin.Context) {
		traceID := frontendhttputil.InitTraceID(c)
		c.Header(constant.HeaderTraceID, traceID)
		wrapped(c)
	}
}

// sandboxTraceHandler is a thin alias of traceHandler, kept for the sandbox routes.
func sandboxTraceHandler(handler gin.HandlerFunc) gin.HandlerFunc {
	return traceHandler(handler)
}

func sandboxDirectRouterPath(path string, cfg *routerconfig.SandboxRouterConfig) string {
	routerPath := strings.TrimPrefix(path, sandboxDirectPrefix)
	if routerPath == "" {
		return "/"
	}
	trimmed := strings.TrimPrefix(routerPath, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < sandboxControlPathParts || parts[0] == "" {
		return "/"
	}
	if _, err := strconv.ParseUint(parts[1], 10, 16); err == nil {
		return "/" // explicit ports are not allowed on the RRT control-plane route
	}

	switch parts[1] {
	case "invoke", "healthz", "upload", "download":
		return sandboxDirectControlPath(parts[0], sandboxDirectRRTPort(cfg), parts[1:])
	default:
		return "/"
	}
}

func sandboxTunnelRouterPath(path string, cfg *routerconfig.SandboxRouterConfig) string {
	routerPath := strings.TrimPrefix(path, sandboxTunnelPrefix)
	if routerPath == "" {
		return "/"
	}
	trimmed := strings.TrimPrefix(routerPath, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || parts[0] == "" {
		return routerPath
	}
	if len(parts) >= sandboxControlPathParts {
		if _, err := strconv.ParseUint(parts[1], 10, 16); err == nil {
			return routerPath // compatibility for /tunnel/{safeID}/{port}/...
		}
	}
	return sandboxDirectControlPath(parts[0], cfg.TunnelPort, parts[1:])
}

func sandboxDirectControlPath(safeID string, port int, tail []string) string {
	if len(tail) == 0 {
		return fmt.Sprintf("/%s/%d", safeID, port)
	}
	return fmt.Sprintf("/%s/%d/%s", safeID, port, strings.Join(tail, "/"))
}

func sandboxDirectRRTPort(cfg *routerconfig.SandboxRouterConfig) int {
	if cfg.RRTPort != 0 {
		return cfg.RRTPort
	}
	return sandboxDefaultRRTPort
}

func sandboxDirectProxyHandler(
	pathMapper func(string, *routerconfig.SandboxRouterConfig) string,
	requireAuthenticatedTenant bool,
) gin.HandlerFunc {
	cfg := config.GetConfig().SandboxRouter
	if cfg == nil || !cfg.Enabled {
		return func(c *gin.Context) {
			c.String(http.StatusServiceUnavailable, "sandbox router is disabled")
		}
	}
	cfg.ApplyDefaults()
	target, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", cfg.ListenPort))
	if err != nil {
		return func(c *gin.Context) {
			c.String(http.StatusInternalServerError, "invalid sandbox router target")
		}
	}
	proxy := newSandboxDirectProxy(pathMapper, cfg, target)
	return sandboxDirectServeHandler(proxy, pathMapper, cfg, requireAuthenticatedTenant)
}

func newSandboxDirectProxy(
	pathMapper func(string, *routerconfig.SandboxRouterConfig) string,
	cfg *routerconfig.SandboxRouterConfig,
	target *url.URL,
) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			in := pr.In
			forwardedProto := in.Header.Get("X-Forwarded-Proto")
			authenticatedTenant := in.Header.Get(sandboxInternalTenantKey)
			frontendAuthenticated := authenticatedTenant != ""
			pr.SetURL(target)
			pr.Out.URL.Path = pathMapper(in.URL.Path, cfg)
			pr.Out.URL.RawPath = ""
			pr.Out.URL.RawQuery = in.URL.RawQuery
			if q := pr.Out.URL.Query(); q.Has("token") || q.Has("tenant_id") {
				if frontendAuthenticated {
					q.Del("token")
				}
				// tenant_id is only an API routing hint and must never override the
				// validated identity at sandboxRouter/RRT.
				q.Del("tenant_id")
				pr.Out.URL.RawQuery = q.Encode()
			}
			pr.SetXForwarded()
			pr.Out.Header.Del(sandboxInternalTenantKey)
			if frontendAuthenticated {
				pr.Out.Header.Del(jwtauth.HeaderXAuth)
				pr.Out.Header.Set(sandboxInternalSrcKey, sandboxInternalSrcValue)
				pr.Out.Header.Set(sandboxInternalTenantKey, authenticatedTenant)
			} else {
				// Frontend JWT can be disabled while router JWT remains enabled (for
				// example in smoke deployments). Do not claim that such a request was
				// authenticated: preserve its token for router-side validation and use
				// a separate loopback-only marker so the router strips it before RRT.
				pr.Out.Header.Set(sandboxInternalSrcKey, sandboxInternalProxyValue)
			}
			if forwardedProto != "" {
				pr.Out.Header.Set("X-Forwarded-Proto", forwardedProto)
			}
		},
	}
}

func sandboxDirectServeHandler(
	proxy *httputil.ReverseProxy,
	pathMapper func(string, *routerconfig.SandboxRouterConfig) string,
	cfg *routerconfig.SandboxRouterConfig,
	requireAuthenticatedTenant bool,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Never trust an internal tenant marker supplied by the client. Rebuild it
		// exclusively from the JWT identity stored by the validated middleware.
		c.Request.Header.Del(sandboxInternalTenantKey)
		if tenant, ok := middleware.JWTAuthenticatedTenant(c); ok {
			c.Request.Header.Set(sandboxInternalTenantKey, tenant)
		} else if requireAuthenticatedTenant && config.GetConfig().IamConfig.EnableFuncTokenAuth {
			c.String(http.StatusForbidden, "authenticated tenant is required")
			return
		}
		started := time.Now()
		routerPath := pathMapper(c.Request.URL.Path, cfg)
		proxy.ServeHTTP(c.Writer, c.Request)
		log.GetLogger().Infof(
			"sandbox direct proxy method(%s) path(%s) routerPath(%s) status(%d) size(%d) durationMs(%d) traceID(%s)",
			c.Request.Method,
			c.Request.URL.Path,
			routerPath,
			c.Writer.Status(),
			c.Writer.Size(),
			time.Since(started).Milliseconds(),
			c.Request.Header.Get(constant.HeaderTraceID),
		)
	}
}
