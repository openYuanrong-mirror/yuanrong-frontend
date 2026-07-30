/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package sessioncontext

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/localauth"
	"frontend/pkg/common/faas_common/logger/log"
	commontls "frontend/pkg/common/faas_common/tls"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/schedulerproxy"
)

type ManagerClient interface {
	Post(ctx context.Context, funcKey, path, traceID string, payload any) (int, []byte, error)
}

type HTTPManagerClient struct {
	Client *http.Client
}

func (c HTTPManagerClient) Post(ctx context.Context, funcKey, path, traceID string,
	payload any) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	owner, err := schedulerproxy.Proxy.Get(funcKey, log.GetLogger())
	if err != nil {
		return 0, nil, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		status, responseBody, callErr := c.post(ctx, owner, path, traceID, body)
		if callErr != nil {
			return status, responseBody, callErr
		}
		if status != http.StatusConflict {
			return status, responseBody, nil
		}
		var response ErrorResponse
		if json.Unmarshal(responseBody, &response) != nil || response.Code != "NOT_FUNCTION_OWNER" {
			return status, responseBody, nil
		}
		owner = schedulerproxy.Proxy.GetByInstanceID(response.Message)
		if owner == nil {
			return status, responseBody, fmt.Errorf("redirected scheduler %s not found", response.Message)
		}
	}
	return http.StatusServiceUnavailable, nil, fmt.Errorf("scheduler owner redirects exhausted")
}

func (c HTTPManagerClient) post(ctx context.Context, owner *schedulerproxy.SchedulerNodeInfo,
	path, traceID string, body []byte) (int, []byte, error) {
	if owner == nil || owner.InstanceInfo == nil || owner.InstanceInfo.Address == "" {
		return 0, nil, fmt.Errorf("function owner scheduler is unavailable")
	}
	cfg := config.GetConfig()
	httpsEnabled := cfg != nil && cfg.HTTPSConfig != nil && cfg.HTTPSConfig.HTTPSEnable
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		commontls.GetURLScheme(httpsEnabled)+"://"+owner.InstanceInfo.Address+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(constant.HeaderTraceID, traceID)
	if cfg != nil && cfg.LocalAuth != nil {
		authorization, timestamp := localauth.SignLocally(
			cfg.LocalAuth.AKey, cfg.LocalAuth.SKey, "sessioncontext", cfg.LocalAuth.Duration)
		request.Header.Set(constant.HeaderAuthorization, authorization)
		request.Header.Set(constant.HeaderAuthTimestamp, timestamp)
	}
	client := c.Client
	if client == nil {
		transport := &http.Transport{}
		if httpsEnabled {
			transport.TLSClientConfig = commontls.GetClientTLSConfig()
		}
		client = &http.Client{Timeout: 60 * time.Second, Transport: transport}
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	return response.StatusCode, responseBody, err
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
