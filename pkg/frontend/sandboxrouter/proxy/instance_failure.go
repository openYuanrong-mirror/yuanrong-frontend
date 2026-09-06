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

package proxy

import (
	"encoding/json"
	"errors"
	"net/http"

	"frontend/pkg/common/faas_common/logger/log"

	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"frontend/pkg/frontend/sandboxrouter/route"
)

type failureResponse struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	InstanceID string `json:"instance_id,omitempty"`
	State      string `json:"state"`
	ExitCode   *int32 `json:"exit_code,omitempty"`
	ExitType   *int32 `json:"exit_type,omitempty"`
	ErrCode    *int32 `json:"err_code,omitempty"`
	Retryable  bool   `json:"retryable"`
}

func (s *Server) writeInstanceFailure(w http.ResponseWriter, r *http.Request, key route.Key,
	failure *route.InstanceFailure, payload *jwtauth.JWTPayload) {
	w.Header().Set("Cache-Control", "no-store")
	detailed := s.isControlPort(key.Port)
	if detailed && s.authEnabled {
		if frontendAuthenticatedRequest(r) {
			payload = &jwtauth.JWTPayload{Sub: r.Header.Get(internalTenantHeader)}
		}
		if code, msg := authorize(payload, &route.Target{Tenant: failure.Tenant}); code != 0 {
			http.Error(w, msg, code)
			return
		}
	}
	response := failureResponse{Code: "SANDBOX_EXITED", State: "FATAL", Message: "sandbox has exited"}
	status := http.StatusGone
	switch failure.Status.Code {
	case execendpoint.StatusFailed:
		status = http.StatusServiceUnavailable
		response.Code, response.State = "SANDBOX_RECOVERING", "FAILED"
		response.Message, response.Retryable = "sandbox is recovering", true
	case execendpoint.StatusScheduleFailed:
		status = http.StatusConflict
		response.Code, response.State = "SANDBOX_SCHEDULE_FAILED", "SCHEDULE_FAILED"
		response.Message = "sandbox scheduling failed"
	}
	if detailed {
		response.InstanceID = failure.InstanceID
		if failure.Status.Msg != "" {
			response.Message = failure.Status.Msg
		}
		response.ExitCode = &failure.Status.ExitCode
		response.ExitType = &failure.Status.Type
		response.ErrCode = &failure.Status.ErrCode
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.GetLogger().Warnf("failed to write sandbox failure response: %v", err)
	}
}

type runtimeFailureLookup interface {
	FailureForRuntime(route.Key, string, string) *route.InstanceFailure
}

func (s *Server) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	lookup, ok := s.resolver.(runtimeFailureLookup)
	info, _ := r.Context().Value(reqInfoKey).(*reqInfo)
	if ok && info != nil {
		failure := lookup.FailureForRuntime(info.parsed.Key, info.target.InstanceID, info.target.RuntimeID)
		if failure != nil && failure.Tenant == info.target.Tenant {
			s.writeInstanceFailure(w, r, info.parsed.Key, failure, info.payload)
			return
		}
	}
	errorHandler(w, r, err)
}

func instanceFailure(err error) *route.InstanceFailure {
	var failure *route.InstanceFailure
	if errors.As(err, &failure) {
		return failure
	}
	return nil
}
