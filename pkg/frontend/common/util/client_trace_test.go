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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
	"yuanrong.org/kernel/runtime/libruntime/api"
)

// convertSimpleRuntimeCreateOptions must merge
// CustomExtensions (including the wrapper-injected "traceparent") and CreateOpt
// into the gRPC CreateRequest.createOptions map, and keep injecting the default
// source=frontend marker.
func TestConvertSimpleRuntimeCreateOptionsCarriesTraceparent(t *testing.T) {
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	options := api.InvokeOptions{
		CustomExtensions: map[string]string{
			traceParentExtensionKey: traceParent,
			"lifecycle":             "detached",
		},
		CreateOpt: map[string]string{
			"create_error_policy": "last_failure_on_timeout",
			"tenantId":            "tenant-a",
		},
	}

	createOptions := convertSimpleRuntimeCreateOptions(options)

	require.Equal(t, traceParent, createOptions[traceParentExtensionKey])
	require.Equal(t, "detached", createOptions["lifecycle"])
	require.Equal(t, "last_failure_on_timeout", createOptions["create_error_policy"])
	require.Equal(t, "tenant-a", createOptions["tenantId"])
	// The frontend source marker is still injected when absent.
	require.Equal(t, frontendProxyCreateSource, createOptions[frontendProxyCreateSourceKey])
}

// An explicit source in CustomExtensions must not be overwritten by the default.
func TestConvertSimpleRuntimeCreateOptionsKeepsExplicitSource(t *testing.T) {
	options := api.InvokeOptions{
		CustomExtensions: map[string]string{frontendProxyCreateSourceKey: "explicit-source"},
	}

	createOptions := convertSimpleRuntimeCreateOptions(options)

	require.Equal(t, "explicit-source", createOptions[frontendProxyCreateSourceKey])
}
