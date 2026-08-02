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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	commontype "frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/sessioncontext"
)

const (
	sessionContextTestURN = "sn:cn:yrk:default:function:0@agentrt@e2e0729:latest"
	completedEventSeq     = 2
)

type sessionContextMemoryReader map[string][]byte

func (reader sessionContextMemoryReader) Get(key, _, _ string) ([]byte, error) {
	return reader[key], nil
}

func newSessionContextHandlerReader(
	t *testing.T, scope sessioncontext.Scope, sessionID string,
) sessionContextMemoryReader {
	t.Helper()
	reader := sessionContextMemoryReader{}
	putJSON := func(key string, value any) {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		reader[key] = raw
	}
	putJSON(sessioncontext.RegistryKey(scope.RegisteredName), sessioncontext.Registry{
		SchemaVersion: 1,
		SessionContexts: []sessioncontext.RegistryEntry{{
			FunctionVersion: "latest", SessionContextID: sessionID,
			CreatedAt: "2026-07-29T09:00:00Z",
		}},
	})
	putJSON(sessioncontext.TurnKey(scope, sessionID, 1), sessioncontext.TurnRecord{
		SessionContextID: sessionID, TurnIndex: 1, TurnID: "turn-000001",
		StartSeq: 1, CreatedAt: "2026-07-29T09:00:00Z", SchemaVersion: 1,
	})
	putJSON(sessioncontext.EventKey(scope, sessionID, 1), sessioncontext.Event{
		SessionContextID: sessionID, TurnID: "turn-000001", Seq: 1,
		EventID: "event-1", Source: "PLATFORM", Type: "input.message",
		Data: map[string]any{"message": "hello"}, SchemaVersion: 1,
		CreatedAt: "2026-07-29T09:00:00Z",
	})
	putJSON(sessioncontext.EventKey(scope, sessionID, completedEventSeq), sessioncontext.Event{
		SessionContextID: sessionID, TurnID: "turn-000001", Seq: completedEventSeq,
		EventID: "event-2", Source: "SDK", Type: "turn.completed",
		Data: map[string]any{"output": "done"}, SchemaVersion: 1,
		CreatedAt: "2026-07-29T09:00:01Z",
	})
	return reader
}

type sessionContextHandlerTest struct {
	name       string
	handler    gin.HandlerFunc
	hasSession bool
	extraParam gin.Param
	contains   []string
}

func sessionContextHandlerTests() []sessionContextHandlerTest {
	return []sessionContextHandlerTest{
		{name: "list sessions", handler: ListSessionContextsHandler,
			contains: []string{`"sessionContextId":"session-1"`}},
		{name: "get session", handler: GetSessionContextHandler, hasSession: true,
			contains: []string{`"turnCount":1`, `"eventCount":2`, `"state":"COMPLETED"`}},
		{name: "list turns", handler: ListSessionContextTurnsHandler, hasSession: true,
			contains: []string{`"turnId":"turn-000001"`, `"result":"done"`}},
		{name: "get turn", handler: GetSessionContextTurnHandler, hasSession: true,
			extraParam: gin.Param{Key: turnIDParam, Value: "turn-000001"},
			contains:   []string{`"inputs":["hello"]`, `"state":"COMPLETED"`}},
		{name: "list events", handler: ListSessionContextEventsHandler, hasSession: true,
			contains: []string{`"seq":1`, `"seq":2`, `"nextAfterSeq":2`}},
	}
}

func runSessionContextHandlerTests(t *testing.T, sessionID string) {
	t.Helper()
	for _, test := range sessionContextHandlerTests() {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			ctx.Params = []gin.Param{{Key: "function-urn", Value: sessionContextTestURN}}
			if test.hasSession {
				ctx.Params = append(ctx.Params, gin.Param{Key: sessionContextIDParam, Value: sessionID})
			}
			if test.extraParam.Key != "" {
				ctx.Params = append(ctx.Params, test.extraParam)
			}
			test.handler(ctx)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			for _, expected := range test.contains {
				require.Contains(t, recorder.Body.String(), expected)
			}
		})
	}
}

func TestSessionContextHandlers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scope := sessioncontext.NewScope(
		"default", "0@agentrt@e2e0729", "latest", sessionContextTestURN)
	sessionID := "session-1"
	reader := newSessionContextHandlerReader(t, scope, sessionID)
	restore := installSessionContextHandlerDependencies(reader, false, "default")
	defer restore()
	runSessionContextHandlerTests(t, sessionID)
}

func TestSessionContextHandlerRejectsCrossTenantQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	restore := installSessionContextHandlerDependencies(sessionContextMemoryReader{}, true, "other-tenant")
	defer restore()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	ctx.Params = []gin.Param{{Key: "function-urn", Value: sessionContextTestURN}}
	ListSessionContextsHandler(ctx)

	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Contains(t, recorder.Body.String(), "FUNCTION_TENANT_MISMATCH")
}

func TestSessionContextHandlerRequiresSessionContextFeature(t *testing.T) {
	gin.SetMode(gin.TestMode)
	restore := installSessionContextHandlerDependencies(sessionContextMemoryReader{}, false, "default")
	defer restore()

	loadSessionContextFuncSpec = func(string) (*commontype.FuncSpec, bool) {
		spec := &commontype.FuncSpec{}
		// The legacy Agent Session switch must not enable the new SessionContext APIs.
		spec.ExtendedMetaData.EnableAgentSession = true
		return spec, true
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	ctx.Params = []gin.Param{{Key: "function-urn", Value: sessionContextTestURN}}
	ListSessionContextsHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "SESSION_CONTEXT_NOT_ENABLED")
}

func installSessionContextHandlerDependencies(
	reader sessionContextMemoryReader, authEnabled bool, tenant string,
) func() {
	originalService := newSessionContextService
	originalLoad := loadSessionContextFuncSpec
	originalAuth := sessionContextAuthEnabled
	originalTenant := sessionContextAuthenticatedTenant
	newSessionContextService = func() *sessioncontext.Service {
		return sessioncontext.NewService(reader)
	}
	loadSessionContextFuncSpec = func(string) (*commontype.FuncSpec, bool) {
		spec := &commontype.FuncSpec{}
		spec.ExtendedMetaData.EnableSessionCtx = true
		return spec, true
	}
	sessionContextAuthEnabled = func() bool { return authEnabled }
	sessionContextAuthenticatedTenant = func(*gin.Context) (string, bool) {
		return tenant, true
	}
	return func() {
		newSessionContextService = originalService
		loadSessionContextFuncSpec = originalLoad
		sessionContextAuthEnabled = originalAuth
		sessionContextAuthenticatedTenant = originalTenant
	}
}
