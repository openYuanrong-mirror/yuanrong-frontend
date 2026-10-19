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
	"errors"
	"hash/fnv"
	"math/bits"
	"sort"
)

// HRWPolicy implements Highest-Random-Weight (Rendezvous Hashing) affinity.
type HRWPolicy struct{}

// Select returns the node IP with the highest score for the given user.
func (HRWPolicy) Select(user string, nodeIPs []string) (string, error) {
	if len(nodeIPs) == 0 {
		return "", errors.New("affinity selection requires at least one node IP")
	}
	sorted := make([]string, len(nodeIPs))
	copy(sorted, nodeIPs)
	sort.Strings(sorted)
	var bestIP string
	var bestScore uint64
	for i, ip := range sorted {
		score := hrwScore(user, ip)
		if i == 0 || score > bestScore {
			bestScore = score
			bestIP = ip
		}
	}
	return bestIP, nil
}

// Name returns the policy identifier.
func (HRWPolicy) Name() string {
	return "hrw"
}

// hrwScore computes a stable 64-bit score for a (user, ip) pair using FNV-1a.
func hrwScore(user, ip string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(user))
	h.Write([]byte("|"))
	h.Write([]byte(ip))
	return mix64(h.Sum64())
}

// mix64 improves bit dispersion of the FNV-1a output via a finalizer.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	_ = bits.TrailingZeros64(x)
	return x
}

func init() {
	RegisterAffinityPolicy("hrw", HRWPolicy{})
}
