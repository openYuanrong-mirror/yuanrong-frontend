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

package resolver

import (
	"context"
	"errors"
	"strings"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/frontend/sandboxrouter/route"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type deletionStateReader interface {
	GetResponse(etcd3.EtcdCtxInfo, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

// ConfirmInstanceDeleted checks both lifecycle records at one linearizable
// revision. A retained failure summary or a missing owner alone cannot prove
// deletion. This bounded, keys-only scan is only used for failed-instance DELETE.
func ConfirmInstanceDeleted(ctx context.Context, instanceID string) (bool, error) {
	client := etcd3.GetRouterEtcdClient()
	if client == nil {
		return false, errNoEtcdClient
	}
	readCtx, cancel := context.WithTimeout(ctx, defaultReadThroughTimeout)
	defer cancel()
	return confirmInstanceDeleted(readCtx, client, instanceID)
}

func confirmInstanceDeleted(ctx context.Context, reader deletionStateReader, instanceID string) (bool, error) {
	safeID := route.SanitizeID(strings.TrimSpace(instanceID))
	if safeID == "" {
		return false, errors.New("instance ID is required")
	}
	// Do not use a serializable read: an older replica could miss a recreation.
	response, err := reader.GetResponse(etcd3.CreateEtcdCtxInfoWithTimeout(ctx, defaultReadThroughTimeout),
		instanceRoutePathPrefix+"/"+safeID)
	if err != nil {
		return false, err
	}
	if response == nil || response.Header == nil || response.Header.Revision <= 0 {
		return false, errors.New("missing authoritative deletion revision")
	}
	if len(response.Kvs) != 0 {
		return false, nil
	}
	// RouteInfo can disappear before InstanceInfo. Check every tenant so that a
	// retained summary cannot hide a new owner or an ambiguous sanitized ID.
	instances, err := reader.GetResponse(etcd3.CreateEtcdCtxInfoWithTimeout(ctx, defaultReadThroughTimeout),
		constant.InstancePathPrefix+"/business/yrk/tenant/", clientv3.WithPrefix(),
		clientv3.WithKeysOnly(), clientv3.WithRev(response.Header.Revision))
	if err != nil {
		return false, err
	}
	if instances == nil || instances.More {
		return false, errors.New("incomplete authoritative instance scan")
	}
	for _, kv := range instances.Kvs {
		if route.SanitizeID(rawInstanceID(string(kv.Key))) == safeID {
			return false, nil
		}
	}
	return true, nil
}
