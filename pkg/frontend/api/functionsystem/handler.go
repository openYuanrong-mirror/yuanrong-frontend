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

// Package frontend the api of frontend
package frontend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/tracer"
	"frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/common/tenantauth"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/metrics"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/serverstatus"

	"yuanrong.org/kernel/runtime/libruntime/api"
	libruntimeconfig "yuanrong.org/kernel/runtime/libruntime/config"
)

var (
	createMetricsOnce sync.Once
	invokeMetricsOnce sync.Once
	killMetricsOnce   sync.Once
)

func initCreateMetrics() {
	createMetricsOnce.Do(func() {
		// Register counter for create handler operations with function name and http code
		err := metrics.RegisterCounter("handler_create_operations_total", "Total number of create handler operations", []string{"function_name", "http_code"})
		if err != nil {
			log.GetLogger().Warnf("failed to register handler_create_operations_total metric: %v", err)
		}
		// Register histogram for create handler operation duration
		err = metrics.RegisterHistogram("handler_create_operation_duration_seconds", "Create handler operation duration in seconds", []string{"function_name"}, nil)
		if err != nil {
			log.GetLogger().Warnf("failed to register handler_create_operation_duration_seconds metric: %v", err)
		}
	})
}

func initInvokeMetrics() {
	invokeMetricsOnce.Do(func() {
		// Register counter for invoke handler operations with function name and http code
		err := metrics.RegisterCounter("handler_invoke_operations_total", "Total number of invoke handler operations", []string{"function_name", "http_code"})
		if err != nil {
			log.GetLogger().Warnf("failed to register handler_invoke_operations_total metric: %v", err)
		}
		// Register histogram for invoke handler operation duration
		err = metrics.RegisterHistogram("handler_invoke_operation_duration_seconds", "Invoke handler operation duration in seconds", []string{"function_name"}, nil)
		if err != nil {
			log.GetLogger().Warnf("failed to register handler_invoke_operation_duration_seconds metric: %v", err)
		}
	})
}

func initKillMetrics() {
	killMetricsOnce.Do(func() {
		// Register counter for kill handler operations with http code
		err := metrics.RegisterCounter("handler_kill_operations_total", "Total number of kill handler operations", []string{"http_code"})
		if err != nil {
			log.GetLogger().Warnf("failed to register handler_kill_operations_total metric: %v", err)
		}
		// Register histogram for kill handler operation duration
		err = metrics.RegisterHistogram("handler_kill_operation_duration_seconds", "Kill handler operation duration in seconds", []string{}, nil)
		if err != nil {
			log.GetLogger().Warnf("failed to register handler_kill_operation_duration_seconds metric: %v", err)
		}
	})
}

// CreateHandler the handler of create
func CreateHandler(ctx *gin.Context) {
	// Initialize metrics on first call
	initCreateMetrics()

	startTime := time.Now()
	var httpCode int
	hasError := false
	var functionName string

	// Use defer to ensure metrics are reported even if function returns early
	defer func() {
		if httpCode == 0 {
			if hasError {
				httpCode = http.StatusInternalServerError
			} else {
				httpCode = http.StatusOK
			}
		}
		httpCodeStr := strconv.Itoa(httpCode)

		// Report operation count
		if err := metrics.IncrementCounter("handler_create_operations_total", functionName, httpCodeStr); err != nil {
			log.GetLogger().Debugf("failed to report handler_create_operations_total metric: %v", err)
		}

		// Report operation duration
		duration := time.Since(startTime)
		if err := metrics.ObserveHistogram("handler_create_operation_duration_seconds", duration.Seconds(), functionName); err != nil {
			log.GetLogger().Debugf("failed to report handler_create_operation_duration_seconds metric: %v", err)
		}
	}()

	remoteClientID, traceID := getHeaderPrams(ctx)
	spanCtx, span := otel.Tracer(tracer.GetOtelServiceName()).Start(ctx.Request.Context(), "http.create")
	defer span.End()
	log.GetLogger().Infof("%s|receive instance create request, remoteClientID: %s", traceID, remoteClientID)
	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read request body error %s", err.Error())
		httpCode = http.StatusInternalServerError
		hasError = true
		SetCtxResponse(ctx, nil, httpCode)
		return
	}
	functionName = "unknown"
	resp, err := util.GetFunctionSystemClient().CreateRaw(spanCtx, body, buildRawRequestOption(spanCtx))
	log.GetLogger().Debugf("receive instance create response, msg: %s", resp)
	if err != nil {
		httpCode = http.StatusBadRequest
		hasError = true
		SetCtxResponse(ctx, []byte(err.Error()), httpCode)
		return
	}
	httpCode = http.StatusOK
	SetCtxResponse(ctx, resp, httpCode)
}

// InvokeHandler the handler of invoke
func InvokeHandler(ctx *gin.Context) {
	// Initialize metrics on first call
	initInvokeMetrics()

	startTime := time.Now()
	var httpCode int
	hasError := false
	var functionName string

	// Use defer to ensure metrics are reported even if function returns early
	defer func() {
		if httpCode == 0 {
			if hasError {
				httpCode = http.StatusInternalServerError
			} else {
				httpCode = http.StatusOK
			}
		}
		httpCodeStr := strconv.Itoa(httpCode)

		// Report operation count
		if err := metrics.IncrementCounter("handler_invoke_operations_total", functionName, httpCodeStr); err != nil {
			log.GetLogger().Debugf("failed to report handler_invoke_operations_total metric: %v", err)
		}

		// Report operation duration
		duration := time.Since(startTime)
		if err := metrics.ObserveHistogram("handler_invoke_operation_duration_seconds", duration.Seconds(), functionName); err != nil {
			log.GetLogger().Debugf("failed to report handler_invoke_operation_duration_seconds metric: %v", err)
		}
	}()

	remoteClientID, traceID := getHeaderPrams(ctx)
	spanCtx, span := otel.Tracer(tracer.GetOtelServiceName()).Start(ctx.Request.Context(), "http.invoke")
	defer span.End()
	log.GetLogger().Infof("%s|receive instance invoke request, remoteClientID: %s", traceID, remoteClientID)

	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read request body error %s", err.Error())
		httpCode = http.StatusInternalServerError
		hasError = true
		SetCtxResponse(ctx, nil, httpCode)
		return
	}
	functionName = "unknown"
	notify, err := util.GetFunctionSystemClient().InvokeRaw(spanCtx, body, buildRawRequestOption(spanCtx))
	log.GetLogger().Debugf("receive instance invoke response, msg: %s", notify)
	if err != nil {
		httpCode = http.StatusBadRequest
		hasError = true
		SetCtxResponse(ctx, []byte(err.Error()), httpCode)
		return
	}
	httpCode = http.StatusOK
	SetCtxResponse(ctx, notify, httpCode)
}

// KillHandler the handler of kill
func KillHandler(ctx *gin.Context) {
	// Initialize metrics on first call
	initKillMetrics()

	startTime := time.Now()
	var httpCode int
	hasError := false
	defer reportKillMetrics(startTime, &httpCode, &hasError)

	remoteClientID, traceID := getHeaderPrams(ctx)
	spanCtx, span := otel.Tracer(tracer.GetOtelServiceName()).Start(ctx.Request.Context(), "http.kill")
	defer span.End()
	log.GetLogger().Infof("%s|receives instance kill request, remoteClientID: %s", traceID, remoteClientID)
	httpCode, hasError = handleKillRequest(ctx, traceID, spanCtx)
}

func handleKillRequest(ctx *gin.Context, traceID string, spanCtx context.Context) (int, bool) {
	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read request body error %s", err.Error())
		SetCtxResponse(ctx, nil, http.StatusInternalServerError)
		return http.StatusInternalServerError, true
	}
	killReqRaw, isJSON, err := buildKillRequestBody(ctx, traceID, body)
	if err != nil {
		SetCtxResponse(ctx, []byte(err.Error()), http.StatusBadRequest)
		return http.StatusBadRequest, true
	}

	needsAuth, errCode, authErr := ensureKillJWTContext(ctx, traceID)
	if authErr != nil {
		log.GetLogger().Warnf("%s|rejects instance kill request: %s", traceID, authErr.Error())
		SetCtxResponse(ctx, []byte(authErr.Error()), errCode)
		return errCode, true
	}
	if needsAuth {
		killReq, _, decodeErr := decodeKillRequest(killReqRaw, false)
		if decodeErr != nil {
			log.GetLogger().Errorf("%s|failed to decode kill request protobuf: %s", traceID, decodeErr.Error())
			SetCtxResponse(ctx, []byte(decodeErr.Error()), http.StatusBadRequest)
			return http.StatusBadRequest, true
		}
		if errCode, authErr := authorizeKillRequest(ctx, killReq); authErr != nil {
			log.GetLogger().Warnf("%s|rejects instance kill request: %s", traceID, authErr.Error())
			SetCtxResponse(ctx, []byte(authErr.Error()), errCode)
			return errCode, true
		}
	}
	resp, err := util.GetFunctionSystemClient().KillRaw(spanCtx, killReqRaw, buildRawRequestOption(spanCtx))
	log.GetLogger().Debugf("receive instance kill response, msg: %s", resp)
	if err != nil {
		SetCtxResponse(ctx, []byte(err.Error()), http.StatusBadRequest)
		return http.StatusBadRequest, true
	}
	if err := writeKillResponse(ctx, traceID, resp, isJSON, http.StatusOK); err != nil {
		SetCtxResponse(ctx, []byte(err.Error()), http.StatusInternalServerError)
		return http.StatusInternalServerError, true
	}
	return http.StatusOK, false
}

func reportKillMetrics(startTime time.Time, httpCode *int, hasError *bool) {
	if *httpCode == 0 {
		if *hasError {
			*httpCode = http.StatusInternalServerError
		} else {
			*httpCode = http.StatusOK
		}
	}
	httpCodeStr := strconv.Itoa(*httpCode)
	if err := metrics.IncrementCounter("handler_kill_operations_total", httpCodeStr); err != nil {
		log.GetLogger().Debugf("failed to report handler_kill_operations_total metric: %v", err)
	}
	duration := time.Since(startTime)
	if err := metrics.ObserveHistogram("handler_kill_operation_duration_seconds", duration.Seconds()); err != nil {
		log.GetLogger().Debugf("failed to report handler_kill_operation_duration_seconds metric: %v", err)
	}
}

func buildKillRequestBody(ctx *gin.Context, traceID string, body []byte) ([]byte, bool, error) {
	// Clients may send JSON instead of a serialized core_service.KillRequest.
	isJSON := ctx.ContentType() == "application/json"
	if !isJSON {
		return body, false, nil
	}
	killReqRaw, err := killRequestJSONToProto(body)
	if err != nil {
		log.GetLogger().Errorf("%s|failed to decode kill request json: %s", traceID, err.Error())
		return nil, true, err
	}
	return killReqRaw, true, nil
}

func writeKillResponse(ctx *gin.Context, traceID string, resp []byte, isJSON bool, httpCode int) error {
	if isJSON {
		jsonResp, convErr := killResponseProtoToJSON(resp)
		if convErr != nil {
			log.GetLogger().Errorf("%s|failed to encode kill response json: %s", traceID, convErr.Error())
			return convErr
		}
		ctx.Writer.Header().Set("Content-Type", "application/json")
		SetCtxResponse(ctx, jsonResp, httpCode)
		return nil
	}
	SetCtxResponse(ctx, resp, httpCode)
	return nil
}

// killRequestJSONToProto transcodes a JSON-encoded kill request into the
// serialized core_service.KillRequest protobuf expected by the runtime. Unknown
// JSON fields are ignored so the wire contract can evolve without breaking
// older clients.
func killRequestJSONToProto(body []byte) ([]byte, error) {
	_, raw, err := decodeKillRequest(body, true)
	return raw, err
}

func decodeKillRequest(body []byte, isJSON bool) (*core.KillRequest, []byte, error) {
	killReq := &core.KillRequest{}
	if len(body) > 0 {
		if isJSON {
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, killReq); err != nil {
				return nil, nil, err
			}
		} else if err := proto.Unmarshal(body, killReq); err != nil {
			return nil, nil, err
		}
	}
	raw, err := proto.Marshal(killReq)
	if err != nil {
		return nil, nil, err
	}
	return killReq, raw, nil
}

func needsKillAuthorization(ctx *gin.Context) bool {
	_, hasSub := ctx.Get("jwt_sub")
	_, hasRole := ctx.Get("jwt_role")
	return hasSub || hasRole || ctx.GetHeader(jwtauth.HeaderXAuth) != ""
}

func ensureKillJWTContext(ctx *gin.Context, traceID string) (bool, int, error) {
	if !needsKillAuthorization(ctx) {
		return false, http.StatusOK, nil
	}
	if _, hasSub := ctx.Get("jwt_sub"); hasSub {
		if _, hasRole := ctx.Get("jwt_role"); hasRole {
			return true, http.StatusOK, nil
		}
	}
	token := ctx.GetHeader(jwtauth.HeaderXAuth)
	identity, errCode, err := tenantauth.AuthenticateDeveloperToken(token, traceID)
	if err != nil {
		return true, errCode, err
	}
	ctx.Set("jwt_sub", identity.TenantID)
	ctx.Set("jwt_role", identity.Role)
	return true, http.StatusOK, nil
}

func authorizeKillRequest(ctx *gin.Context, killReq *core.KillRequest) (int, error) {
	callerTenant, _ := ctx.Get("jwt_sub")
	callerRole, _ := ctx.Get("jwt_role")
	callerTenantID, ok := callerTenant.(string)
	if !ok || callerTenantID == "" {
		return http.StatusForbidden, errors.New("missing caller tenant in JWT context")
	}
	callerRoleName, ok := callerRole.(string)
	if !ok || callerRoleName == "" {
		return http.StatusForbidden, errors.New("missing caller role in JWT context")
	}
	if callerRoleName != jwtauth.RoleDeveloper {
		return http.StatusForbidden, errors.New("caller role is not authorized to delete instances")
	}
	instanceID := killReq.GetInstanceID()
	if instanceID == "" {
		return http.StatusBadRequest, errors.New("missing instanceID")
	}
	// KillAllInstances overloads instanceID with the driver job ID. Job IDs are
	// not entries in the frontend's RUNNING-instance cache, so applying the
	// single-instance ownership check would reject every driver Finalize call.
	if killReq.GetSignal() == int32(libruntimeconfig.KillAllInstances) {
		remoteClientID, _ := getHeaderPrams(ctx)
		if remoteClientID == "" || remoteClientID != instanceID {
			return http.StatusForbidden, errors.New("job ID does not match remote client ID")
		}
		return http.StatusOK, nil
	}
	summary, ok := execendpoint.Default().GetSummary(instanceID)
	if !ok {
		return http.StatusNotFound, errors.New("instance not found in frontend cache")
	}
	if callerTenantID == tenantauth.SystemTenantID {
		return http.StatusOK, nil
	}
	if callerTenantID == summary.TenantID {
		return http.StatusOK, nil
	}
	return http.StatusForbidden, errors.New("caller tenant is not authorized to delete target instance")
}

// killResponseProtoToJSON transcodes a serialized core_service.KillResponse
// protobuf into JSON. Enum codes are emitted as numbers and all fields are
// always populated so clients can rely on a stable {"code","message"} shape.
func killResponseProtoToJSON(raw []byte) ([]byte, error) {
	killResp := &core.KillResponse{}
	if len(raw) > 0 {
		if err := proto.Unmarshal(raw, killResp); err != nil {
			return nil, err
		}
	}
	return protojson.MarshalOptions{UseEnumNumbers: true, EmitUnpopulated: true}.Marshal(killResp)
}

func getHeaderPrams(ctx *gin.Context) (string, string) {
	remoteClientID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderRemoteClientId, "remoteClientId")
	traceID := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTraceID, "traceId")
	return remoteClientID, traceID
}

func buildRawRequestOption(ctx context.Context) api.RawRequestOption {
	carrier := propagation.HeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return api.RawRequestOption{
		TraceParent: carrier.Get(constant.HeaderTraceParent),
	}
}

// SetCtxResponse set ctx response
func SetCtxResponse(ctx *gin.Context, body []byte, statusCode int) {
	if len(body) == 0 {
		log.GetLogger().Warnf("the body of ctx response is empty")
	}
	ctx.Writer.WriteHeader(statusCode)
	if serverstatus.IsShutdown() {
		ctx.Writer.Header().Set("Connection", "close")
	}
	if _, err := ctx.Writer.Write(body); err != nil {
		log.GetLogger().Errorf("failed to set response body in context error %s", err.Error())
	}
}
