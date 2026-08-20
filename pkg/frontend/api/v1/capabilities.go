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
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	envDataSystemDeployed = "YR_DATASYSTEM_DEPLOYED"
	envBypassDataSystem   = "YR_BYPASS_DATASYSTEM"
)

type dataSystemCapability struct {
	// These fields mirror YR_DATASYSTEM_DEPLOYED and YR_BYPASS_DATASYSTEM so SDK
	// drivers and runtimes use the same deployment-capability vocabulary.
	DataSystemDeployed bool `json:"dataSystemDeployed"`
	BypassDataSystem   bool `json:"bypassDataSystem"`
}

type capabilitiesResponse struct {
	DataSystem dataSystemCapability `json:"dataSystem"`
}

func envBool(name string, defaultValue bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

// CapabilitiesHandler is a read-only discovery endpoint used during Driver SDK
// initialization. It reports server deployment state; it does not negotiate or
// mutate that state for the caller. The endpoint is intentionally anonymous so
// capability discovery can happen before authenticated SDK initialization.
func CapabilitiesHandler(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, capabilitiesResponse{
		DataSystem: dataSystemCapability{
			DataSystemDeployed: envBool(envDataSystemDeployed, true),
			BypassDataSystem:   envBool(envBypassDataSystem, false),
		},
	})
}
