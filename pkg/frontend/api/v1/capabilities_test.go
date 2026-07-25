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

package v1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

var capabilityTestCases = []struct {
	name                   string
	dataSystemDeployed     string
	bypassDataSystem       string
	wantDataSystemDeployed bool
	wantBypassDataSystem   bool
}{
	{name: "defaults", wantDataSystemDeployed: true},
	{
		name: "canonical values", dataSystemDeployed: "false", bypassDataSystem: "true",
		wantBypassDataSystem: true,
	},
	{
		name: "numeric values", dataSystemDeployed: "0", bypassDataSystem: "1",
		wantBypassDataSystem: true,
	},
	{
		name: "yes and no", dataSystemDeployed: "no", bypassDataSystem: "yes",
		wantBypassDataSystem: true,
	},
	{
		name: "on and off", dataSystemDeployed: "off", bypassDataSystem: "on",
		wantBypassDataSystem: true,
	},
	{
		name: "case and whitespace", dataSystemDeployed: " TRUE ", bypassDataSystem: " FALSE ",
		wantDataSystemDeployed: true,
	},
	{
		name: "invalid values use defaults", dataSystemDeployed: "invalid", bypassDataSystem: "invalid",
		wantDataSystemDeployed: true,
	},
}

func TestCapabilitiesHandler(t *testing.T) {
	for _, tt := range capabilityTestCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envDataSystemDeployed, tt.dataSystemDeployed)
			t.Setenv(envBypassDataSystem, tt.bypassDataSystem)

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			CapabilitiesHandler(ctx)

			require.Equal(t, http.StatusOK, recorder.Code)
			var response capabilitiesResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, tt.wantDataSystemDeployed, response.DataSystem.DataSystemDeployed)
			require.Equal(t, tt.wantBypassDataSystem, response.DataSystem.BypassDataSystem)
		})
	}
}

func TestFunctionAgentClientSwitchDoesNotChangeDeploymentCapability(t *testing.T) {
	t.Setenv("DATA_SYSTEM_ENABLE", "false")
	t.Setenv(envDataSystemDeployed, "")
	t.Setenv(envBypassDataSystem, "")

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	CapabilitiesHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var response capabilitiesResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.True(t, response.DataSystem.DataSystemDeployed)
	require.False(t, response.DataSystem.BypassDataSystem)
}
