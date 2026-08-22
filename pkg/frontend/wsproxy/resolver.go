/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses.2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wsproxy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/proxyrouting"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
)

// routeWaitTimeout bounds how long resolveInstance waits for the instance
// route cache (etcd-watcher-backed) to populate. A freshly-created sandbox
// may not have its /sn/instance event delivered yet, same race sshproxy
// handles with its routeWait.
const routeWaitTimeout = 10 * time.Second

// resolveInstance applies WebSocket-specific lifecycle validation after the
// shared owner resolver has selected the owning proxy's TCP tunnel.
func resolveInstance(ctx context.Context, instanceID string) (*types.InstanceSpecification, string, error) {
	if execendpoint.Default().IsPaused(instanceID) {
		return nil, "", execendpoint.NewInstancePausedError(instanceID)
	}
	routeCtx, cancel := context.WithTimeout(ctx, routeWaitTimeout)
	defer cancel()
	ownerRoute, err := proxyrouting.Wait(
		routeCtx, instanceID, proxyrouting.CapabilityTCPTunnel, proxyrouting.TransportTCPTunnel)
	if err != nil {
		return nil, "", err
	}
	instance := ownerRoute.Instance
	if instance.InstanceStatus.Code != int32(constant.KernelInstanceStatusRunning) {
		return nil, "", fmt.Errorf("instance %s is not running", instanceID)
	}
	return instance, ownerRoute.Address, nil
}

// newRequestID is a per-tunnel correlation id surfaced in the tunnel header
// for log tracing on the function_proxy side. Not load-bearing for routing.
func newRequestID() string {
	return uuid.NewString()
}

// tenantOfInstance returns the instance's owning tenant from its createOptions.
// agent create writes the authenticated tenant into CreateOptions["tenantId"]
// (see api/agent applyAgentCreateOpts), so this is the field the cross-check
// must compare against the JWT sub.
func tenantOfInstance(instance *types.InstanceSpecification) string {
	if instance == nil || instance.CreateOptions == nil {
		return ""
	}
	return strings.TrimSpace(instance.CreateOptions["tenantId"])
}

var _ = log.GetLogger // retained for resolver-side logging hooks in later iterations
