/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"frontend/pkg/common/faas_common/urnutils"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/functionmeta"
	"frontend/pkg/frontend/middleware"
	"frontend/pkg/frontend/sessioncontext"
)

const (
	sessionContextIDParam = "sessionCtxId"
	turnIDParam           = "turnId"
	defaultPageLimit      = 50
	maxPageLimit          = 200
	defaultEventLimit     = 100
	maxEventLimit         = 1000
	maxSessionContextID   = 63
)

var newSessionContextService = func() *sessioncontext.Service {
	return sessioncontext.NewService(sessioncontext.DataSystemReader{})
}

var newSessionContextManagerClient = func() sessioncontext.ManagerClient {
	return sessioncontext.HTTPManagerClient{}
}

var loadSessionContextFuncSpec = functionmeta.LoadFuncSpec

var sessionContextAuthEnabled = func() bool {
	return config.GetConfig().IamConfig.EnableFuncTokenAuth
}

var sessionContextAuthenticatedTenant = middleware.JWTAuthenticatedTenant

func ListSessionContextsHandler(ctx *gin.Context) {
	scope, ok := sessionContextScope(ctx)
	if !ok {
		return
	}
	limit, ok := queryInt(ctx, "limit", defaultPageLimit, maxPageLimit, false)
	if !ok {
		return
	}
	result, err := newSessionContextService().ListSessions(
		scope, limit, ctx.Query("pageToken"), requestTraceID(ctx))
	writeSessionContextResult(ctx, result, err)
}

func GetSessionContextHandler(ctx *gin.Context) {
	scope, sessionID, ok := sessionContextResourceScope(ctx)
	if !ok {
		return
	}
	result, err := newSessionContextService().GetSession(scope, sessionID, requestTraceID(ctx))
	writeSessionContextResult(ctx, result, err)
}

func ListSessionContextTurnsHandler(ctx *gin.Context) {
	scope, sessionID, ok := sessionContextResourceScope(ctx)
	if !ok {
		return
	}
	limit, ok := queryInt(ctx, "limit", defaultPageLimit, maxPageLimit, false)
	if !ok {
		return
	}
	result, err := newSessionContextService().ListTurns(
		scope, sessionID, limit, ctx.Query("pageToken"), requestTraceID(ctx))
	writeSessionContextResult(ctx, result, err)
}

func GetSessionContextTurnHandler(ctx *gin.Context) {
	scope, sessionID, ok := sessionContextResourceScope(ctx)
	if !ok {
		return
	}
	turnID := ctx.Param(turnIDParam)
	if turnID == "" {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_QUERY", "turnId is required")
		return
	}
	result, err := newSessionContextService().GetTurn(scope, sessionID, turnID, requestTraceID(ctx))
	writeSessionContextResult(ctx, result, err)
}

func ListSessionContextEventsHandler(ctx *gin.Context) {
	scope, sessionID, ok := sessionContextResourceScope(ctx)
	if !ok {
		return
	}
	afterSeq, ok := queryInt(ctx, "afterSeq", 0, int(^uint(0)>>1), true)
	if !ok {
		return
	}
	limit, ok := queryInt(ctx, "limit", defaultEventLimit, maxEventLimit, false)
	if !ok {
		return
	}
	source := ctx.Query("source")
	if source != "" && source != "PLATFORM" && source != "SDK" {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_QUERY",
			"source must be PLATFORM or SDK")
		return
	}
	result, err := newSessionContextService().ListEvents(scope, sessionID, sessioncontext.EventFilter{
		AfterSeq: afterSeq, Limit: limit, TurnID: ctx.Query("turnId"),
		Source: source, EventType: ctx.Query("eventType"),
	}, requestTraceID(ctx))
	writeSessionContextResult(ctx, result, err)
}

type forkSessionContextBody struct {
	TargetSessionCtxID     string `json:"targetSessionCtxId"`
	TargetSessionContextID string `json:"targetSessionContextId"`
	TurnID                 string `json:"turnId"`
}

func ForkSessionContextHandler(ctx *gin.Context) {
	scope, sourceID, ok := sessionContextResourceScope(ctx)
	if !ok {
		return
	}
	var body forkSessionContextBody
	if err := ctx.ShouldBindJSON(&body); err != nil {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_REQUEST", "invalid fork request")
		return
	}
	targetID := body.TargetSessionCtxID
	if targetID == "" {
		targetID = body.TargetSessionContextID
	}
	if len(targetID) == 0 || len(targetID) > maxSessionContextID || body.TurnID == "" || targetID == sourceID {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_REQUEST",
			"targetSessionContextId and turnId are required, and target must differ from source")
		return
	}
	funcKey := urnutils.CombineFunctionKey(scope.TenantID, scope.RegisteredName, scope.Version)
	status, response, err := newSessionContextManagerClient().Post(ctx.Request.Context(), funcKey,
		"/sessioncontext/fork", requestTraceID(ctx), gin.H{
			"tenantId": scope.TenantID, "funcKey": funcKey, "functionUrn": scope.FunctionURN,
			"sourceSessionCtxId": sourceID, "targetSessionCtxId": targetID,
			"turnId": body.TurnID, "traceId": requestTraceID(ctx),
		})
	writeManagerResponse(ctx, status, response, err)
}

func DeleteSessionContextHandler(ctx *gin.Context) {
	scope, sessionID, ok := sessionContextResourceScope(ctx)
	if !ok {
		return
	}
	funcKey := urnutils.CombineFunctionKey(scope.TenantID, scope.RegisteredName, scope.Version)
	status, response, err := newSessionContextManagerClient().Post(ctx.Request.Context(), funcKey,
		"/sessioncontext/delete", requestTraceID(ctx), gin.H{
			"tenantId": scope.TenantID, "funcKey": funcKey, "functionUrn": scope.FunctionURN,
			"sessionCtxId": sessionID, "traceId": requestTraceID(ctx),
		})
	writeManagerResponse(ctx, status, response, err)
}

func writeManagerResponse(ctx *gin.Context, status int, body []byte, err error) {
	if err != nil {
		writeSessionContextError(ctx, http.StatusServiceUnavailable, "SCHEDULER_UNAVAILABLE", err.Error())
		return
	}
	if status == http.StatusNoContent {
		ctx.Status(status)
		return
	}
	if status >= 200 && status < 300 {
		var response any
		if len(body) == 0 || json.Unmarshal(body, &response) != nil {
			ctx.Status(status)
			return
		}
		ctx.JSON(status, response)
		return
	}
	var response sessioncontext.ErrorResponse
	if json.Unmarshal(body, &response) != nil || response.Code == "" {
		writeSessionContextError(ctx, http.StatusBadGateway, "SCHEDULER_ERROR", "invalid scheduler response")
		return
	}
	ctx.JSON(status, response)
}

func sessionContextResourceScope(ctx *gin.Context) (sessioncontext.Scope, string, bool) {
	scope, ok := sessionContextScope(ctx)
	if !ok {
		return sessioncontext.Scope{}, "", false
	}
	sessionID := ctx.Param(sessionContextIDParam)
	if len(sessionID) == 0 || len(sessionID) > maxSessionContextID {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_SESSION_CONTEXT",
			"sessionContextId length must be between 1 and 63")
		return sessioncontext.Scope{}, "", false
	}
	return scope, sessionID, true
}

func sessionContextScope(ctx *gin.Context) (sessioncontext.Scope, bool) {
	rawURN := ctx.Param("function-urn")
	functionURN, err := urnutils.GetFunctionInfo(rawURN)
	if err != nil {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_QUERY", "invalid function URN")
		return sessioncontext.Scope{}, false
	}
	if functionURN.FuncVersion == "" {
		functionURN.FuncVersion = urnutils.DefaultURNVersion
	}
	if sessionContextAuthEnabled() {
		tenantID, authenticated := sessionContextAuthenticatedTenant(ctx)
		if !authenticated {
			writeSessionContextError(ctx, http.StatusUnauthorized, "UNAUTHORIZED", "authenticated tenant is required")
			return sessioncontext.Scope{}, false
		}
		if tenantID != functionURN.TenantID {
			writeSessionContextError(ctx, http.StatusForbidden, "FUNCTION_TENANT_MISMATCH",
				"function tenant does not match authenticated tenant")
			return sessioncontext.Scope{}, false
		}
	}
	funcKey := urnutils.CombineFunctionKey(
		functionURN.TenantID, functionURN.FuncName, functionURN.FuncVersion)
	spec, exists := loadSessionContextFuncSpec(funcKey)
	if !exists || spec == nil {
		writeSessionContextError(ctx, http.StatusNotFound, "FUNCTION_NOT_FOUND", "function not found")
		return sessioncontext.Scope{}, false
	}
	if !spec.ExtendedMetaData.EnableSessionCtx {
		writeSessionContextError(ctx, http.StatusBadRequest, "SESSION_CONTEXT_NOT_ENABLED",
			"function does not enable SessionContext")
		return sessioncontext.Scope{}, false
	}
	return sessioncontext.NewScope(
		functionURN.TenantID, functionURN.FuncName, functionURN.FuncVersion, functionURN.String()), true
}

func queryInt(ctx *gin.Context, name string, defaultValue, maximum int, allowZero bool) (int, bool) {
	raw := ctx.Query(name)
	if raw == "" {
		return defaultValue, true
	}
	value, err := strconv.Atoi(raw)
	minimum := 1
	if allowZero {
		minimum = 0
	}
	if err != nil || value < minimum || value > maximum {
		writeSessionContextError(ctx, http.StatusBadRequest, "INVALID_QUERY",
			fmt.Sprintf("%s is out of range", name))
		return 0, false
	}
	return value, true
}

func requestTraceID(ctx *gin.Context) string {
	for _, header := range []string{"X-Trace-ID", "X-Request-ID"} {
		if value := ctx.GetHeader(header); value != "" {
			return value
		}
	}
	return ""
}

func writeSessionContextResult(ctx *gin.Context, result any, err error) {
	if err == nil {
		ctx.JSON(http.StatusOK, result)
		return
	}
	var serviceErr *sessioncontext.Error
	if !errors.As(err, &serviceErr) {
		writeSessionContextError(ctx, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
		return
	}
	status := http.StatusInternalServerError
	switch serviceErr.Code {
	case sessioncontext.ErrInvalidQuery:
		status = http.StatusBadRequest
	case sessioncontext.ErrSessionNotFound, sessioncontext.ErrTurnNotFound:
		status = http.StatusNotFound
	case sessioncontext.ErrDataSystem:
		status = http.StatusServiceUnavailable
	case sessioncontext.ErrDataCorrupted:
		status = http.StatusInternalServerError
	}
	writeSessionContextError(ctx, status, string(serviceErr.Code), serviceErr.Message)
}

func writeSessionContextError(ctx *gin.Context, status int, code, message string) {
	ctx.JSON(status, gin.H{"code": code, "message": strings.TrimSpace(message)})
}
