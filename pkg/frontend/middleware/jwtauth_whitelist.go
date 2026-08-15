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

package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/frontend/common/jwtauth"
)

// AuthWhitelistRule defines a rule for authentication whitelist
type AuthWhitelistRule struct {
	Path      string   // URL path pattern
	Methods   []string // HTTP methods (empty means all methods)
	MatchType string   // "exact" or "prefix"
	SkipAuth  bool     // If true, skip authentication for this path
}

// defaultAuthWhitelist contains default paths that skip authentication
// Note: This whitelist is for HTTP-level static URL-based rules.
// For dynamic function-level authentication (e.g., public functions),
var defaultAuthWhitelist = []AuthWhitelistRule{
	{
		Path:      "/",
		Methods:   []string{"GET"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	{
		Path:      "/api-docs",
		Methods:   []string{"GET"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	{
		Path:      "/auth/",
		Methods:   []string{},
		MatchType: "prefix",
		SkipAuth:  true,
	},
	{
		Path:      "/healthz",
		Methods:   []string{"GET"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	{
		Path:      "/serverless/v1/componentshealth",
		Methods:   []string{"GET"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	{
		Path:      "/client/v1/lease",
		Methods:   []string{"PUT", "DELETE"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	{
		Path:      "/client/v1/lease/keepalive",
		Methods:   []string{"POST"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	// Terminal static resources (CSS, JS) don't require authentication
	{
		Path:      "/terminal/static/",
		Methods:   []string{"GET"},
		MatchType: "prefix",
		SkipAuth:  true,
	},
	// Terminal WebSocket endpoint - authentication handled inside handler
	{
		Path:      "/terminal/ws",
		Methods:   []string{"GET"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	// Agent WS passthrough endpoint - authentication handled inside handler
	// (JWT via X-Auth / ?token / Sec-WebSocket-Protocol, like posixws/webterm).
	{
		Path:      "/serverless/v1/ws",
		Methods:   []string{"GET"},
		MatchType: "exact",
		SkipAuth:  true,
	},
	// HTTP passthrough endpoint - authentication handled inside handler
	// (reuses wsproxy's DialSandboxTunnel pipeline, like /serverless/v1/ws).
	{
		Path:      "/serverless/v1/http/",
		Methods:   []string{},
		MatchType: "prefix",
		SkipAuth:  true,
	},
	// Note: Other /terminal paths (e.g., /terminal) require JWT authentication
}

// customAuthWhitelist can be configured at runtime
var customAuthWhitelist []AuthWhitelistRule

// SetAuthWhitelist sets custom authentication whitelist rules
func SetAuthWhitelist(rules []AuthWhitelistRule) {
	customAuthWhitelist = rules
}

// AddAuthWhitelistRule adds a single whitelist rule
func AddAuthWhitelistRule(rule AuthWhitelistRule) {
	customAuthWhitelist = append(customAuthWhitelist, rule)
}

// isInAuthWhitelist checks if a request path should skip authentication
func isInAuthWhitelist(path, method string) bool {
	// Check custom whitelist first
	for _, rule := range customAuthWhitelist {
		if matchRule(path, method, rule) {
			return rule.SkipAuth
		}
	}

	// Check default whitelist
	for _, rule := range defaultAuthWhitelist {
		if matchRule(path, method, rule) {
			return rule.SkipAuth
		}
	}

	return false
}

// matchRule checks if a path and method match a whitelist rule
func matchRule(path, method string, rule AuthWhitelistRule) bool {
	// Check method match (empty methods means all methods)
	if len(rule.Methods) > 0 {
		methodMatch := false
		for _, m := range rule.Methods {
			if method == m {
				methodMatch = true
				break
			}
		}
		if !methodMatch {
			return false
		}
	}

	// Check path match
	switch rule.MatchType {
	case "exact":
		return path == rule.Path
	case "prefix":
		return strings.HasPrefix(path, rule.Path)
	default:
		return path == rule.Path
	}
}

// GlobalJWTAuthMiddleware applies JWT authentication to all routes except whitelisted ones
// or when the skip flag is set (e.g., for public functions)
func GlobalJWTAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		method := c.Request.Method
		log.GetLogger().Debugf("Path %s %s JWT authentication", method, path)

		// Reverse tunnel is a data-plane gateway alias. The SDK deliberately does
		// not send platform JWTs on the default ws:// tunnel, and server-side tunnel
		// setup is already owned by the authenticated sandbox create call. Do not
		// gate /tunnel on frontend JWT.
		if isTunnelRequest(c.Request) {
			log.GetLogger().Debugf("Tunnel path %s %s skips frontend JWT authentication", method, path)
			c.Next()
			return
		}

		// Check if path is in whitelist
		if isInAuthWhitelist(path, method) {
			log.GetLogger().Debugf("Path %s %s is in auth whitelist, skipping JWT authentication", method, path)
			c.Next()
			return
		}

		// Check if JWT authentication should be skipped for public functions
		if ShouldSkipJWTAuth(c) {
			log.GetLogger().Infof("Public function detected for %s %s, skipping JWT authentication", method, path)
			c.Next()
			return
		}

		// Apply JWT authentication with different role requirements based on URL type
		// For invoke URLs (urlPostInvoke and urlShortInvoke), allow both RoleUser and RoleDeveloper
		// For other URLs, only allow RoleDeveloper
		var jwtHandler gin.HandlerFunc
		if IsInvokeURL(c) {
			// Allow both user and developer roles for invoke endpoints
			jwtHandler = JWTAuthMiddlewareWithRoles([]string{jwtauth.RoleUser, jwtauth.RoleDeveloper})
		} else {
			// Only allow developer role for other endpoints
			jwtHandler = JWTAuthMiddlewareWithRoles([]string{jwtauth.RoleDeveloper})
		}
		jwtHandler(c)
	}
}

func isTunnelRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	path := r.URL.Path
	if path != "/tunnel" && !strings.HasPrefix(path, "/tunnel/") {
		return false
	}
	return true
}
