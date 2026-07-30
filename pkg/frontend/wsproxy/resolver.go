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
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/instancemanager"
)

// routeWaitTimeout bounds how long resolveInstance waits for the instance
// route cache (etcd-watcher-backed) to populate. A freshly-created sandbox
// may not have its /sn/instance event delivered yet, same race sshproxy
// handles with its routeWait.
const routeWaitTimeout = 10 * time.Second

// tunnelAddressWaitInterval is the poll cadence for the function_proxy
// tcp.tunnel capability to appear in the proxy endpoint cache. Mirrors
// sshproxy.proxyRoutePollInterval.
const tunnelAddressWaitInterval = 100 * time.Millisecond

// resolveInstance is the wsproxy analogue of sshproxy.resolveInstance, with
// the SSH-specific bits stripped: it confirms the instance is RUNNING and
// resolves the owning function_proxy's tcp.tunnel address. Unlike SSH it
// does NOT check CreateOptions["host_user"] — the AgentServer has no SSH
// login user, and the frontend authenticates the caller via JWT, not via
// backend SSH keys. The tenant cross-check (instanceTenant != jwtSub) is
// done by the handler before dialing, to keep the resolver a pure
// routing primitive.
func resolveInstance(ctx context.Context, instanceID string) (*types.InstanceSpecification, string, error) {
	routeCtx, cancel := context.WithTimeout(ctx, routeWaitTimeout)
	defer cancel()
	instance, err := instancemanager.WaitInstanceByID(routeCtx, instanceID)
	if err != nil {
		return nil, "", fmt.Errorf("wait for instance %s route: %w", instanceID, err)
	}
	if instance.InstanceStatus.Code != int32(constant.KernelInstanceStatusRunning) {
		return nil, "", fmt.Errorf("instance %s is not running", instanceID)
	}
	if strings.TrimSpace(instance.FunctionProxyID) == "" {
		return nil, "", fmt.Errorf("instance %s has no functionProxyID", instanceID)
	}
	tunnelAddress, err := waitProxyTCPTunnelAddress(routeCtx, instance.FunctionProxyID)
	if err != nil {
		return nil, "", fmt.Errorf("resolve TCP tunnel for instance %s: %w", instanceID, err)
	}
	return instance, tunnelAddress, nil
}

// waitProxyTCPTunnelAddress polls the proxy endpoint cache until the owning
// function_proxy publishes a healthy tcp.tunnel address. Identical to
// sshproxy.waitProxyTCPTunnelAddress — both consume the same
// util.LookupProxyTCPTunnelAddress backed by the /sn/proxy watch.
func waitProxyTCPTunnelAddress(ctx context.Context, functionProxyID string) (string, error) {
	ticker := time.NewTicker(tunnelAddressWaitInterval)
	defer ticker.Stop()
	for {
		if address, ok := util.LookupProxyTCPTunnelAddress(functionProxyID); ok {
			return address, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("proxy %s has no healthy tcp.tunnel endpoint: %w", functionProxyID, ctx.Err())
		case <-ticker.C:
		}
	}
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
