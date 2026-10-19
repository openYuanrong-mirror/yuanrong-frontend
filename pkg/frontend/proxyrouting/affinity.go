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

package proxyrouting

import (
	"fmt"
	"sync"
)

// AffinityPolicy maps a user to a owning proxy node IP from a candidate list.
type AffinityPolicy interface {
	Select(user string, nodeIPs []string) (owningIP string, err error)
	Name() string
}

var (
	affinityMu       sync.RWMutex
	affinityRegistry = make(map[string]AffinityPolicy)
)

// defaultAffinityPolicyName is the name of the default affinity policy.
var defaultAffinityPolicyName = "hrw"

// RegisterAffinityPolicy registers an affinity policy under the given name.
func RegisterAffinityPolicy(name string, policy AffinityPolicy) {
	affinityMu.Lock()
	defer affinityMu.Unlock()
	affinityRegistry[name] = policy
}

// GetAffinityPolicy returns the affinity policy registered under name.
func GetAffinityPolicy(name string) (AffinityPolicy, bool) {
	affinityMu.RLock()
	defer affinityMu.RUnlock()
	policy, ok := affinityRegistry[name]
	return policy, ok
}

// DefaultAffinityPolicy returns the process-wide default affinity policy.
func DefaultAffinityPolicy() AffinityPolicy {
	policy, ok := GetAffinityPolicy(defaultAffinityPolicyName)
	if !ok {
		panic(fmt.Sprintf("default affinity policy %q is not registered", defaultAffinityPolicyName))
	}
	return policy
}
