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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHRWDistributionUniformity(t *testing.T) {
	nodeIPs := []string{
		"10.0.0.1",
		"10.0.0.2",
		"10.0.0.3",
		"10.0.0.4",
		"10.0.0.5",
	}
	policy := HRWPolicy{}
	counts := make(map[string]int, len(nodeIPs))
	const numUsers = 1000
	for i := 0; i < numUsers; i++ {
		user := fmt.Sprintf("user-%d", i)
		ip, err := policy.Select(user, nodeIPs)
		require.NoError(t, err)
		counts[ip]++
	}

	expected := numUsers / len(nodeIPs)
	for _, ip := range nodeIPs {
		require.InDelta(t, expected, counts[ip], float64(expected)*0.3,
			"node %s got %d assignments, expected ~%d", ip, counts[ip], expected)
	}
}

func TestHRWMigrationRatioOnNodeAdd(t *testing.T) {
	nodeIPs := []string{
		"10.0.0.1",
		"10.0.0.2",
		"10.0.0.3",
		"10.0.0.4",
		"10.0.0.5",
	}
	newNodeIPs := append(append([]string{}, nodeIPs...), "10.0.0.6")
	policy := HRWPolicy{}

	const numUsers = 1000
	changed := 0
	for i := 0; i < numUsers; i++ {
		user := fmt.Sprintf("user-%d", i)
		old, err := policy.Select(user, nodeIPs)
		require.NoError(t, err)
		new, err := policy.Select(user, newNodeIPs)
		require.NoError(t, err)
		if old != new {
			changed++
		}
	}

	// Adding 1 node to 5 should migrate roughly 1/6 of users.
	require.InDelta(t, numUsers/6, changed, float64(numUsers)*0.05,
		"expected ~%d migrations, got %d", numUsers/6, changed)
}

func TestHRWListOrderIndependence(t *testing.T) {
	nodeIPs := []string{
		"10.0.0.1",
		"10.0.0.2",
		"10.0.0.3",
		"10.0.0.4",
		"10.0.0.5",
	}
	policy := HRWPolicy{}
	const numUsers = 100
	for i := 0; i < numUsers; i++ {
		user := fmt.Sprintf("user-%d", i)
		expected, err := policy.Select(user, nodeIPs)
		require.NoError(t, err)

		shuffled := make([]string, len(nodeIPs))
		copy(shuffled, nodeIPs)
		rand.Shuffle(len(shuffled), func(a, b int) {
			shuffled[a], shuffled[b] = shuffled[b], shuffled[a]
		})
		got, err := policy.Select(user, shuffled)
		require.NoError(t, err)
		require.Equal(t, expected, got, "order independence violated for user %s", user)
	}
}

func TestHRWEmptyListReturnsError(t *testing.T) {
	policy := HRWPolicy{}
	_, err := policy.Select("user", nil)
	require.Error(t, err)
}

func TestPolicyRegistry(t *testing.T) {
	custom := HRWPolicy{} // reuse HRW as a stand-in custom policy
	RegisterAffinityPolicy("test-custom", custom)

	got, ok := GetAffinityPolicy("test-custom")
	require.True(t, ok)
	require.Equal(t, "hrw", got.Name())

	_, ok = GetAffinityPolicy("nonexistent")
	require.False(t, ok)

	defaultPolicy := DefaultAffinityPolicy()
	require.NotNil(t, defaultPolicy)
	require.Equal(t, "hrw", defaultPolicy.Name())
}
