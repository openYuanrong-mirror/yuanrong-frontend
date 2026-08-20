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

package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/core"
	"frontend/pkg/frontend/api/app"
	"frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/common/util"
)

const reusableSnapshotMasterPath = "/snap-manager/reusable-snapshots"

const reusableSnapshotRequestTimeout = 30 * time.Second

type reusableSnapshotMasterClient interface {
	GetActiveMasterAddr() string
}

type reusableSnapshotDoer interface {
	Do(*http.Request) (*http.Response, error)
}

var (
	newReusableSnapshotMasterClient                      = func() reusableSnapshotMasterClient { return util.NewClient() }
	reusableSnapshotHTTPClient      reusableSnapshotDoer = &http.Client{Timeout: reusableSnapshotRequestTimeout}
)

type reusableSnapshotCreateRequest struct {
	Name string `json:"name"`
}

// CreateReusableSnapshotV1Handler creates a non-expiring reusable Snapshot
// while leaving the source sandbox running.
func CreateReusableSnapshotV1Handler(ctx *gin.Context) {
	var request reusableSnapshotCreateRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			fmt.Errorf("invalid request body: %v", err))
		return
	}
	name := strings.TrimSpace(request.Name)
	if request.Name != "" && name == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			errors.New("name must be a non-empty string"))
		return
	}
	payload, err := proto.Marshal(&core.SnapOptions{
		Type:         common.SnapType_SNAPSHOT,
		Ttl:          0,
		LeaveRunning: true,
		Name:         name,
	})
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	killResponse, err := executeSandboxLifecycleKill(
		ctx, sandboxPauseInstanceSignal, payload,
		sandboxSnapshotRequestIDPattern, "snapshot",
	)
	if err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	var snapshotInfo core.SnapshotInfo
	if err := proto.Unmarshal(killResponse.GetPayload(), &snapshotInfo); err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
			fmt.Errorf("invalid snapshot response: %v", err))
		return
	}
	if strings.TrimSpace(snapshotInfo.GetSnapshotID()) == "" {
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
			errors.New("invalid snapshot response identity"))
		return
	}
	app.SetCtxResponse(ctx, reusableSnapshotInfo{
		SnapshotID: snapshotInfo.GetSnapshotID(),
		Names:      append([]string{}, snapshotInfo.GetNames()...),
	}, http.StatusOK, nil)
}

// GetReusableSnapshotV1Handler returns one tenant-scoped reusable Snapshot.
func GetReusableSnapshotV1Handler(ctx *gin.Context) {
	snapshotID := strings.TrimSpace(ctx.Param("snapshotID"))
	if snapshotID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, errors.New("snapshotID is required"))
		return
	}
	query := url.Values{"snapshot_id": {snapshotID}}
	proxyReusableSnapshotRequest(ctx, http.MethodGet, query)
}

// ListReusableSnapshotsV1Handler lists tenant-scoped reusable Snapshots.
func ListReusableSnapshotsV1Handler(ctx *gin.Context) {
	query := url.Values{}
	if name := strings.TrimSpace(ctx.Query("name")); name != "" {
		query.Set("name", name)
	}
	if pageToken := strings.TrimSpace(ctx.Query("pageToken")); pageToken != "" {
		query.Set("pageToken", pageToken)
	}
	if pageSize := strings.TrimSpace(ctx.Query("pageSize")); pageSize != "" {
		query.Set("pageSize", pageSize)
	}
	proxyReusableSnapshotRequest(ctx, http.MethodGet, query)
}

// DeleteReusableSnapshotV1Handler deletes one tenant-scoped reusable Snapshot.
func DeleteReusableSnapshotV1Handler(ctx *gin.Context) {
	snapshotID := strings.TrimSpace(ctx.Param("snapshotID"))
	if snapshotID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest, errors.New("snapshotID is required"))
		return
	}
	requestID := strings.TrimSpace(ctx.GetHeader(sandboxLifecycleRequestIDHeader))
	if requestID == "" {
		app.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			fmt.Errorf("%s is required", sandboxLifecycleRequestIDHeader))
		return
	}
	query := url.Values{"snapshot_id": {snapshotID}, "request_id": {requestID}}
	proxyReusableSnapshotRequest(ctx, http.MethodDelete, query)
}

func proxyReusableSnapshotRequest(ctx *gin.Context, method string, query url.Values) {
	activeMasterAddr := strings.TrimSpace(newReusableSnapshotMasterClient().GetActiveMasterAddr())
	if activeMasterAddr == "" {
		app.SetCtxResponse(ctx, nil, http.StatusServiceUnavailable,
			errors.New("active function master is unavailable"))
		return
	}
	query.Set("tenant_id", reusableSnapshotTenant(ctx))
	upstreamURL := normalizeReusableSnapshotMasterURL(activeMasterAddr) + reusableSnapshotMasterPath
	request, err := http.NewRequestWithContext(ctx.Request.Context(), method,
		upstreamURL+"?"+query.Encode(), nil)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	for _, header := range []string{
		constant.HeaderTraceID, constant.HeaderTraceParent,
		sandboxLifecycleRequestIDHeader, "Authorization", "X-Auth-Token",
	} {
		if value := ctx.GetHeader(header); value != "" {
			request.Header.Set(header, value)
		}
	}
	response, err := reusableSnapshotHTTPClient.Do(request)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadGateway,
			fmt.Errorf("active function master request failed: %w", err))
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		app.SetCtxResponse(ctx, nil, http.StatusBadGateway,
			fmt.Errorf("active function master response failed: %w", err))
		return
	}
	if response.StatusCode >= http.StatusBadRequest {
		app.SetCtxResponse(ctx, nil, response.StatusCode,
			fmt.Errorf("active function master rejected request: %s", strings.TrimSpace(string(body))))
		return
	}
	var payload json.RawMessage = body
	if !json.Valid(payload) {
		app.SetCtxResponse(ctx, nil, http.StatusBadGateway,
			errors.New("active function master returned invalid JSON"))
		return
	}
	app.SetCtxResponse(ctx, payload, response.StatusCode, nil)
}

func reusableSnapshotTenant(ctx *gin.Context) string {
	if tenant := httputil.GetCompatibleGinHeader(ctx.Request, constant.HeaderTenantID, "tenantId"); tenant != "" {
		return tenant
	}
	if tenant := tenantClaim(ctx.Request); tenant != "" {
		return tenant
	}
	return "default"
}

func normalizeReusableSnapshotMasterURL(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + strings.TrimRight(addr, "/")
}
