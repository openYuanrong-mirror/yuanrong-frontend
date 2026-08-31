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

// Package api wraps different api versions, and can be easily switched between different versions
// API provides http handlers used by fast-http, the handlers should only do http context checking and should dispatch
// the actual logic to
package api

import (
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"

	"frontend/pkg/common/constants"
	"frontend/pkg/common/faas_common/tracer"
	commonJob "frontend/pkg/common/job"
	"frontend/pkg/frontend/api/agent"
	"frontend/pkg/frontend/api/app"
	"frontend/pkg/frontend/api/auth"
	"frontend/pkg/frontend/api/datasystem"
	frontend "frontend/pkg/frontend/api/functionsystem"
	"frontend/pkg/frontend/api/job"
	"frontend/pkg/frontend/api/lease"
	"frontend/pkg/frontend/api/metaservice"
	"frontend/pkg/frontend/api/resources"
	"frontend/pkg/frontend/api/sandbox"
	v1 "frontend/pkg/frontend/api/v1"
	"frontend/pkg/frontend/common"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/frontendsdkadapter/handler"
	"frontend/pkg/frontend/httpproxy"
	"frontend/pkg/frontend/middleware"
	"frontend/pkg/frontend/posixws"
	"frontend/pkg/frontend/webui"
	"frontend/pkg/frontend/wsproxy"
)

const (
	// naming convention: url + method + description
	urlPostInvoke = "/serverless/v1/functions/" + common.GinUrnParamMark +
		common.FunctionUrnParam + "/invocations"
	urlInterruptSession = "/serverless/v1/functions/" + common.GinUrnParamMark +
		common.FunctionUrnParam + "/sessions" + constants.DynamicRouterParamPrefix + "sessionId/interrupt"
	urlDeleteSession = "/serverless/v1/functions/" + common.GinUrnParamMark +
		common.FunctionUrnParam + "/sessions" + constants.DynamicRouterParamPrefix + "sessionId"
	urlListSessionContexts = "/serverless/v1/functions/" + common.GinUrnParamMark +
		common.FunctionUrnParam + "/session-contexts"
	urlGetSessionContext        = urlListSessionContexts + constants.DynamicRouterParamPrefix + "sessionCtxId"
	urlListSessionContextTurns  = urlGetSessionContext + "/turns"
	urlGetSessionContextTurn    = urlListSessionContextTurns + constants.DynamicRouterParamPrefix + "turnId"
	urlListSessionContextEvents = urlGetSessionContext + "/events"
	urlForkSessionContext       = urlGetSessionContext + "/fork"
	urlShortInvoke              = "/invocations/:tenant-id/:namespace/:function/"
	urlShortInvokeOld           = "/:tenant-id/:namespace/:function/"
	urlStreamSubscribe          = "/serverless/v1/stream/subscribe"
	urlGetHealthCheck           = "/healthz"
	urlClusterHealthy           = "/serverless/v1/componentshealth"
	urlCapabilities             = "/serverless/v1/capabilities"
	urlResources                = "/global-scheduler/resources"
	urlGetAsyncResult           = "/serverless/v1/functions/async-results/:request-id"
	// url to faasmanager
	urlLease          = "/client/v1/lease"
	urlLeaseKeepAlive = "/client/v1/lease/keepalive"
	// url to frontend
	urlPreCreate = "/serverless/v1/posix/instance/create"
	urlPreInvoke = "/serverless/v1/posix/instance/invoke"
	urlPreKill   = "/serverless/v1/posix/instance/kill"
	urlCreate    = "/frontend/v1/instance/create"
	urlInvoke    = "/frontend/v1/instance/invoke"
	urlKill      = "/frontend/v1/instance/kill"
	// url to datasystem
	urlPut         = "/datasystem/v1/obj/put"
	urlGet         = "/datasystem/v1/obj/get"
	urlIncreaseRef = "/datasystem/v1/obj/increaseref"
	urlDecreaseRef = "/datasystem/v1/obj/decreaseref"
	urlKvSet       = "/datasystem/v1/kv/set"
	urlKvGet       = "/datasystem/v1/kv/get"
	urlKvDel       = "/datasystem/v1/kv/del"
	urlKvMSetTx    = "/datasystem/v1/kv/msettx"
	urlUpload      = "/serverless/v2/data/kv/multiset"
	urlDownload    = "/serverless/v2/data/kv/multiget"
	urlDelete      = "/serverless/v2/data/kv/multidel"
	urlExecute     = "/serverless/v2/aggregation/execute"
	// url to app
	urlGroupApp   = "/app/v1"
	urlCreateApp  = "/posix/instance/create"
	urlListApp    = "/list"
	urlGetAppInfo = "/getappinfo" +
		constants.DynamicRouterParamPrefix + commonJob.PathParamSubmissionId
	urlStopApp = "/posix/kill" +
		constants.DynamicRouterParamPrefix + commonJob.PathParamSubmissionId
	urlDeleteApp = "/delete" +
		constants.DynamicRouterParamPrefix + commonJob.PathParamSubmissionId
)

// InitRoute -
func InitRoute(r *gin.Engine) {
	// Apply invoke preprocessing middleware to:
	// 1. Mark invoke URLs for role-based authentication
	// 2. Detect public functions and skip JWT authentication
	r.Use(middleware.InvokePreprocessMiddleware())

	// Apply global JWT authentication middleware with whitelist support
	// For invoke URLs: allow RoleUser and RoleDeveloper
	// For other URLs: only allow RoleDeveloper
	r.Use(middleware.GlobalJWTAuthMiddleware())

	r.GET(urlGetHealthCheck, v1.HealthzHandler)
	r.GET(urlClusterHealthy, v1.ClusterHealthHandler) // Health check
	r.GET(urlCapabilities, v1.CapabilitiesHandler)
	r.GET(urlResources, resources.QueryHandler)                    // Cluster resource query
	r.POST(urlPostInvoke, tracer.WrapGinHandler(v1.InvokeHandler)) // Invocation
	r.POST(urlInterruptSession, tracer.WrapGinHandler(v1.InterruptSessionHandler))
	r.DELETE(urlDeleteSession, tracer.WrapGinHandler(v1.DeleteSessionHandler))
	r.GET(urlListSessionContexts, tracer.WrapGinHandler(v1.ListSessionContextsHandler))
	r.GET(urlGetSessionContext, tracer.WrapGinHandler(v1.GetSessionContextHandler))
	r.GET(urlListSessionContextTurns, tracer.WrapGinHandler(v1.ListSessionContextTurnsHandler))
	r.GET(urlGetSessionContextTurn, tracer.WrapGinHandler(v1.GetSessionContextTurnHandler))
	r.GET(urlListSessionContextEvents, tracer.WrapGinHandler(v1.ListSessionContextEventsHandler))
	r.POST(urlForkSessionContext, tracer.WrapGinHandler(v1.ForkSessionContextHandler))
	r.DELETE(urlGetSessionContext, tracer.WrapGinHandler(v1.DeleteSessionContextHandler))
	r.POST(urlShortInvoke, tracer.WrapGinHandler(v1.ShortInvokeHandler))    // Invocation
	r.POST(urlShortInvokeOld, tracer.WrapGinHandler(v1.ShortInvokeHandler)) // Deprecated short invocation (backward compatibility)
	r.GET(urlGetAsyncResult, v1.GetAsyncResultHandler)                      // Async invocation result
	r.GET(urlStreamSubscribe, v1.SubscribeHandler)                          // Subscribe Stream
	r.PUT(urlLease, lease.NewLeaseHandler)
	r.DELETE(urlLease, lease.DelLeaseHandler)
	r.POST(urlLeaseKeepAlive, lease.KeepAliveHandler)
	r.POST(urlPreCreate, tracer.WrapGinHandler(frontend.CreateHandler))
	r.POST(urlPreInvoke, tracer.WrapGinHandler(frontend.InvokeHandler))
	r.POST(urlPreKill, tracer.WrapGinHandler(frontend.KillHandler))
	r.POST(urlCreate, tracer.WrapGinHandler(frontend.CreateHandler))
	r.POST(urlInvoke, tracer.WrapGinHandler(frontend.InvokeHandler))
	r.POST(urlKill, tracer.WrapGinHandler(frontend.KillHandler))
	r.POST(urlPut, datasystem.PutHandler)
	r.POST(urlGet, datasystem.GetHandler)
	r.POST(urlIncreaseRef, datasystem.IncreaseRefHandler)
	r.POST(urlDecreaseRef, datasystem.DecreaseRefHandler)
	r.POST(urlKvSet, datasystem.KvSetHandler)
	r.POST(urlKvMSetTx, datasystem.KvMSetTxHandler)
	r.POST(urlKvGet, datasystem.KvGetHandler)
	r.POST(urlKvDel, datasystem.KvDelHandler)
	r.NoRoute(v1.ProxyHandler)
	r.POST(urlUpload, handler.MultiSetHandler)
	r.POST(urlDownload, handler.MultiGetHandler)
	r.POST(urlDelete, handler.MultiDelHandler)
	r.POST(urlExecute, handler.ExecuteHandler)
	// External app requests pass through dashboard before reaching frontend for job handling.
	appGroup := r.Group(urlGroupApp)
	{
		appGroup.POST(urlCreateApp, app.CreateHandler)
		appGroup.GET(urlListApp, app.ListHandler)
		appGroup.GET(urlGetAppInfo, app.GetInfoHandler)
		appGroup.DELETE(urlDeleteApp, app.DeleteHandler)
		appGroup.POST(urlStopApp, app.StopHandler)
	}
	// External job requests access frontend directly for job handling.
	jobGroup := r.Group(commonJob.PathGroupJobs)
	{
		jobGroup.POST("", job.SubmitJobHandler)
		jobGroup.GET("", job.ListJobsHandler)
		jobGroup.GET(commonJob.PathGetJobs, job.GetJobInfoHandler)
		jobGroup.DELETE(commonJob.PathDeleteJobs, job.DeleteJobHandler)
		jobGroup.POST(commonJob.PathStopJobs, job.StopJobHandler)
	}

	metaservice.RegisterFunctionRoutes(r)

	// Auth routes for Keycloak integration (via iam-server)
	iamConfig := config.GetConfig().IamConfig
	authHandler := auth.NewHandler(iamConfig.Addr)
	authGroup := r.Group("/auth")
	{
		authGroup.GET("/login-page", authHandler.LoginPageHandler)
		authGroup.GET("/register-page", authHandler.RegisterPageHandler)
		authGroup.GET("/token-page", authHandler.TokenPageHandler)
		authGroup.GET("/token-exchange-page", authHandler.TokenExchangePageHandler)
		authGroup.GET("/login", authHandler.LoginHandler)
		authGroup.GET("/register", authHandler.RegisterHandler)
		authGroup.GET("/callback", authHandler.CallbackHandler)
		authGroup.POST("/logout", authHandler.LogoutHandler)
		authGroup.POST("/token/direct", authHandler.DirectTokenHandler)
		authGroup.POST("/token/exchange", authHandler.TokenExchangeHandler)
		authGroup.GET("/user", authHandler.UserHandler)
		authGroup.GET("/token/require", authHandler.RequireTokenProxyHandler)
		authGroup.GET("/token/abandon", authHandler.AbandonTokenProxyHandler)
	}

	registerSandboxDirectRoute(r)

	// sandbox management (direct create/delete without job polling)
	sandboxGroup := r.Group("/api/sandbox")
	{
		sandboxGroup.POST("/create", sandboxTraceHandler(sandbox.CreateHandler))
		sandboxGroup.DELETE("/:instanceId", sandboxTraceHandler(sandbox.DeleteHandler))
	}
	sandboxV1Group := r.Group("/api/sandbox/v1/sandboxes")
	{
		sandboxV1Group.POST("", sandboxTraceHandler(sandbox.CreateV1Handler))
		sandboxV1Group.DELETE("/:sandboxID", sandboxTraceHandler(sandbox.DeleteHandler))
		sandboxV1Group.POST("/:sandboxID/pause", sandboxTraceHandler(sandbox.PauseV1Handler))
		sandboxV1Group.POST("/:sandboxID/resume", sandboxTraceHandler(sandbox.ResumeV1Handler))
		sandboxV1Group.POST("/:sandboxID/reload", sandboxTraceHandler(sandbox.ReloadV1Handler))
		sandboxV1Group.POST("/:sandboxID/snapshots", sandboxTraceHandler(sandbox.CreateReusableSnapshotV1Handler))
		sandboxV1Group.POST("/:sandboxID/invoke", sandboxTraceHandler(sandbox.InvokeV1Handler))
	}

	snapshotV1Group := r.Group("/api/sandbox/v1/snapshots")
	{
		snapshotV1Group.GET("", sandboxTraceHandler(sandbox.ListReusableSnapshotsV1Handler))
		snapshotV1Group.GET("/:snapshotID", sandboxTraceHandler(sandbox.GetReusableSnapshotV1Handler))
		snapshotV1Group.DELETE("/:snapshotID", sandboxTraceHandler(sandbox.DeleteReusableSnapshotV1Handler))
	}

	// agent management (create/kill/get/list by user function URN, with workspace/user/userid)
	agentGroup := r.Group("/api/agent")
	{
		agentGroup.POST("", agent.CreateHandler)
		agentGroup.POST("/:instanceId/invoke", agent.InvokeHandler)
		agentGroup.DELETE("/:instanceId", agent.DeleteHandler)
		agentGroup.GET("", agent.ListHandler)
		agentGroup.GET("/:instanceId", agent.GetHandler)
		// agent instance file transfer (upload/download to the owning proxy)
		agentGroup.POST("/:instanceId/files/upload", agent.FileUploadHandler)
		agentGroup.GET("/:instanceId/files/download", agent.FileDownloadHandler)
		agentGroup.GET("/:instanceId/files/list", agent.FileListHandler)
		agentGroup.POST("/:instanceId/files/mkdir", agent.FileMkdirHandler)
	}

	// web terminal
	terminalGroup := r.Group("/terminal")
	{
		terminalGroup.GET("", gin.WrapF(webui.HandleIndex))
		terminalGroup.GET("/ws", gin.WrapF(webui.HandleWebSocket))
		staticFS, _ := fs.Sub(webui.StaticFiles, "static")
		terminalGroup.GET("/static/*filepath", gin.WrapH(http.StripPrefix("/terminal/static", http.FileServer(http.FS(staticFS)))))
	}
	r.GET("api/instances", gin.WrapF(webui.HandleInstances))

	// POSIX WebSocket for create/invoke operations
	r.GET("/serverless/v1/posix/ws", gin.WrapF(posixws.HandlePosixWebSocket))

	// Agent WS passthrough: L4-tunnel external WebSocket connections through
	// to an in-sandbox AgentServer (reuses function_proxy tcp.tunnel).
	r.GET("/serverless/v1/ws", gin.WrapF(wsproxy.HandleWebSocket))

	// HTTP passthrough: L4-tunnel external HTTP requests through to an
	// in-sandbox HTTP server (reuses function_proxy tcp.tunnel).
	r.Any("/serverless/v1/http", gin.WrapF(httpproxy.HandleHTTP))
	r.Any("/serverless/v1/http/*splat", gin.WrapF(httpproxy.HandleHTTP))

	// Function invoke tool (requires authentication)
	r.GET("/functions", gin.WrapF(webui.HandleInvokePage))

	// API documentation page (no authentication required)
	r.GET("/api-docs", gin.WrapF(webui.HandleAPIDoc))

	// Welcome/introduction page (no authentication required)
	r.GET("/", gin.WrapF(webui.HandleWelcome))
}
