/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

package agent

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/tracer"
	"frontend/pkg/frontend/common/util"
)

const (
	// traceParentFieldCount is the number of dash-separated fields in a W3C
	// traceparent header: version-traceID-parentID-flags.
	traceParentFieldCount = 4
	// traceIDHexLength is the length of the W3C trace id in hex characters
	// (16 bytes).
	traceIDHexLength = 32
)

// setupTracingForTest installs a real in-process tracer provider (AlwaysSample)
// plus the W3C propagator and force-enables tracer.EnableCommonTracer, mirroring
// the wrap_test.go house pattern. Everything is restored via t.Cleanup.
func setupTracingForTest(t *testing.T) *gomonkey.Patches {
	t.Helper()
	originalProvider := otel.GetTracerProvider()
	originalPropagator := otel.GetTextMapPropagator()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(originalProvider)
		otel.SetTextMapPropagator(originalPropagator)
	})
	patches := gomonkey.ApplyFunc(tracer.EnableCommonTracer, func() bool { return true })
	t.Cleanup(patches.Reset)
	return patches
}

// the /api/agent routes are registered as
// tracer.WrapGinHandler(handler) in api.go; the wrapped handler must inject a
// W3C Traceparent whose trace id derives from the inbound X-Trace-Id, even when
// the handler itself rejects the request (here: empty body -> 400).
func TestWrappedAgentCreateHandlerInjectsTraceparent(t *testing.T) {
	setupTracingForTest(t)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req, err := http.NewRequest(http.MethodPost, "/api/agent", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	const uuidTraceID = "123e4567-e89b-12d3-a456-426614174000"
	req.Header.Set(constant.HeaderTraceID, uuidTraceID)
	ctx.Request = req

	tracer.WrapGinHandler(CreateHandler)(ctx)

	// The handler still ran and rejected the empty body.
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	// The wrapper injected a well-formed W3C Traceparent. The trace-id derivation
	// from X-Trace-Id is covered by the tracer package's own wrap tests
	// (contextAwareIDGenerator is unexported and cannot be installed from this
	// package), so only well-formedness is asserted here.
	traceParent := req.Header.Get(constant.HeaderTraceParent)
	require.NotEmpty(t, traceParent)
	parts := strings.Split(traceParent, "-")
	require.Len(t, parts, traceParentFieldCount)
	require.Len(t, parts[1], traceIDHexLength)
	_, err = hex.DecodeString(parts[1])
	require.NoError(t, err)
	_, err = hex.DecodeString(parts[2])
	require.NoError(t, err)
}

// with tracing disabled the wrapper is a pure pass-through —
// no Traceparent injection, handler behavior unchanged.
func TestWrappedAgentCreateHandlerSkipsTracingWhenDisabled(t *testing.T) {
	patches := gomonkey.ApplyFunc(tracer.EnableCommonTracer, func() bool { return false })
	defer patches.Reset()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req, err := http.NewRequest(http.MethodPost, "/api/agent", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	req.Header.Set(constant.HeaderTraceID, "trace-disabled-1")
	ctx.Request = req

	tracer.WrapGinHandler(CreateHandler)(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, req.Header.Get(constant.HeaderTraceParent))
}

// buildAgentInvokeOptions must copy the wrapper-injected Traceparent
// request header into CustomExtensions["traceparent"] so it travels to the C++
// side via CreateRequest.createOptions (consumed by
// TraceManager::GetTraceParentFromOptions to parent the yr.create span).
func TestCreateHandlerPropagatesTraceparentToInvokeOptions(t *testing.T) {
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(_ api.FunctionMeta, _ []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-traceparent", nil
		},
	})
	defer stubFuncSpec(t).Reset()

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-traceparent",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})
	// The wrapper injects this header when tracing is enabled; set it directly to
	// keep this test independent of the global tracer state.
	ctx.Request.Header.Set(constant.HeaderTraceParent, traceParent)

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, traceParent, capturedInvokeOpt.CustomExtensions[util.TraceParentExtensionKey])
	// The pre-existing keys are untouched.
	require.Equal(t, "detached", capturedInvokeOpt.CustomExtensions["lifecycle"])
	require.Equal(t, agentConcurrency, capturedInvokeOpt.CustomExtensions["Concurrency"])
	require.Equal(t, "last_failure_on_timeout", capturedInvokeOpt.CreateOpt["create_error_policy"])
}

// without a Traceparent header the extension key is
// simply not set — the C++ side falls back to synthesizing a same-trace parent
// from the trace id (existing behavior).
func TestCreateHandlerOmitsTraceparentWhenAbsent(t *testing.T) {
	var capturedInvokeOpt api.InvokeOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(_ api.FunctionMeta, _ []api.Arg, invokeOpt api.InvokeOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-no-traceparent", nil
		},
	})
	defer stubFuncSpec(t).Reset()

	recorder, ctx := newAgentCreateRecorder(t, CreateAgentRequest{
		Namespace: "agent-ns",
		Name:      "agent-no-traceparent",
		Urn:       validAgentURN,
		Workspace: "/home/snuser/workspaceA",
	})

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	_, ok := capturedInvokeOpt.CustomExtensions[util.TraceParentExtensionKey]
	require.False(t, ok)
	require.Equal(t, "detached", capturedInvokeOpt.CustomExtensions["lifecycle"])
}
