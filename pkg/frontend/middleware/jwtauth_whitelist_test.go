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
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/config"
)

func TestIsInAuthWhitelist(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		method   string
		expected bool
	}{
		{
			name:     "root path GET - should be in whitelist",
			path:     "/",
			method:   "GET",
			expected: true,
		},
		{
			name:     "root path POST - should not be in whitelist",
			path:     "/",
			method:   "POST",
			expected: false,
		},
		{
			name:     "healthz GET - should be in whitelist",
			path:     "/healthz",
			method:   "GET",
			expected: true,
		},
		{
			name:     "healthz POST - should not be in whitelist",
			path:     "/healthz",
			method:   "POST",
			expected: false,
		},
		{
			name:     "componentshealth GET - should be in whitelist",
			path:     "/serverless/v1/componentshealth",
			method:   "GET",
			expected: true,
		},
		{
			name:     "capabilities GET - should be in whitelist",
			path:     "/serverless/v1/capabilities",
			method:   "GET",
			expected: true,
		},
		{
			name:     "capabilities POST - should not be in whitelist",
			path:     "/serverless/v1/capabilities",
			method:   "POST",
			expected: false,
		},
		{
			name:     "lease PUT - should be in whitelist",
			path:     "/client/v1/lease",
			method:   "PUT",
			expected: true,
		},
		{
			name:     "lease DELETE - should be in whitelist",
			path:     "/client/v1/lease",
			method:   "DELETE",
			expected: true,
		},
		{
			name:     "lease POST - should not be in whitelist",
			path:     "/client/v1/lease",
			method:   "POST",
			expected: false,
		},
		{
			name:     "terminal ws - should be in whitelist (auth handled in handler)",
			path:     "/terminal/ws",
			method:   "GET",
			expected: true,
		},
		{
			name:     "terminal static - should be in whitelist (public resources)",
			path:     "/terminal/static/style.css",
			method:   "GET",
			expected: true,
		},
		{
			name:     "terminal static JS - should be in whitelist",
			path:     "/terminal/static/xterm.js",
			method:   "GET",
			expected: true,
		},
		{
			name:     "tunnel alias is not a static whitelist rule",
			path:     "/tunnel/sandbox-demo",
			method:   "GET",
			expected: false,
		},
		{
			name:     "auth login page - should be in whitelist",
			path:     "/auth/login-page",
			method:   "GET",
			expected: true,
		},
		{
			name:     "auth callback - should be in whitelist",
			path:     "/auth/callback",
			method:   "GET",
			expected: true,
		},
		{
			name:     "auth token exchange - should be in whitelist",
			path:     "/auth/token/exchange",
			method:   "POST",
			expected: true,
		},
		{
			name:     "invoke endpoint - should not be in whitelist",
			path:     "/serverless/v1/posix/instance/invoke",
			method:   "POST",
			expected: false,
		},
		{
			name:     "global scheduler resources should require auth",
			path:     "/global-scheduler/resources",
			method:   "GET",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isInAuthWhitelist(tt.path, tt.method)
			assert.Equal(t, tt.expected, result, "Expected %v for path %s %s", tt.expected, tt.method, tt.path)
		})
	}
}

func TestMatchRule(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		method   string
		rule     AuthWhitelistRule
		expected bool
	}{
		{
			name:   "exact match with correct method",
			path:   "/healthz",
			method: "GET",
			rule: AuthWhitelistRule{
				Path:      "/healthz",
				Methods:   []string{"GET"},
				MatchType: "exact",
			},
			expected: true,
		},
		{
			name:   "exact match with wrong method",
			path:   "/healthz",
			method: "POST",
			rule: AuthWhitelistRule{
				Path:      "/healthz",
				Methods:   []string{"GET"},
				MatchType: "exact",
			},
			expected: false,
		},
		{
			name:   "exact match with empty methods (all methods)",
			path:   "/healthz",
			method: "POST",
			rule: AuthWhitelistRule{
				Path:      "/healthz",
				Methods:   []string{},
				MatchType: "exact",
			},
			expected: true,
		},
		{
			name:   "prefix match - matched",
			path:   "/terminal/static/style.css",
			method: "GET",
			rule: AuthWhitelistRule{
				Path:      "/terminal",
				Methods:   []string{},
				MatchType: "prefix",
			},
			expected: true,
		},
		{
			name:   "prefix match - not matched",
			path:   "/api/terminal",
			method: "GET",
			rule: AuthWhitelistRule{
				Path:      "/terminal",
				Methods:   []string{},
				MatchType: "prefix",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchRule(tt.path, tt.method, tt.rule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCustomAuthWhitelist(t *testing.T) {
	// Save original whitelist
	originalWhitelist := customAuthWhitelist
	defer func() {
		customAuthWhitelist = originalWhitelist
	}()

	// Add custom rule
	customRule := AuthWhitelistRule{
		Path:      "/custom/api",
		Methods:   []string{"POST"},
		MatchType: "exact",
		SkipAuth:  true,
	}
	SetAuthWhitelist([]AuthWhitelistRule{customRule})

	// Test custom rule is applied
	result := isInAuthWhitelist("/custom/api", "POST")
	assert.True(t, result, "Custom whitelist rule should be applied")

	// Test original rules still work
	result = isInAuthWhitelist("/healthz", "GET")
	assert.True(t, result, "Default whitelist rules should still work")

	// Add another rule
	AddAuthWhitelistRule(AuthWhitelistRule{
		Path:      "/another/api",
		Methods:   []string{"GET"},
		MatchType: "prefix",
		SkipAuth:  true,
	})

	result = isInAuthWhitelist("/another/api/test", "GET")
	assert.True(t, result, "Added whitelist rule should be applied")
}

func TestGlobalJWTAuthMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name               string
		path               string
		routePath          string
		method             string
		enableAuth         bool
		authHeader         string
		iamAddr            string
		isInvokeURL        bool
		expectedStatusCode int
		shouldCallNext     bool
	}{
		{
			name:               "whitelisted path should skip auth",
			path:               "/healthz",
			method:             "GET",
			enableAuth:         true,
			authHeader:         "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:               "non-whitelisted path with auth disabled",
			path:               "/serverless/v1/posix/instance/invoke",
			method:             "POST",
			enableAuth:         false,
			authHeader:         "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:       "non-whitelisted path without auth header should be rejected",
			path:       "/serverless/v1/posix/instance/invoke",
			method:     "POST",
			enableAuth: true,
			authHeader: "",
			// Global middleware only skips configured whitelist/public-function paths.
			// Non-whitelisted paths require a JWT when EnableFuncTokenAuth=true.
			expectedStatusCode: http.StatusUnauthorized,
			shouldCallNext:     false,
		},
		{
			name:       "terminal websocket path is whitelisted",
			path:       "/terminal/ws",
			method:     "GET",
			enableAuth: true,
			authHeader: "",
			// /terminal/ws is explicitly whitelisted because the handler performs its own authentication.
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:               "plaintext tunnel path should skip auth",
			path:               "/tunnel/sandbox-demo",
			method:             "GET",
			enableAuth:         true,
			authHeader:         "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:               "tls tunnel path should also skip auth",
			path:               "/tunnel/sandbox-demo",
			method:             "GET",
			enableAuth:         true,
			authHeader:         "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:        "invoke URL with RoleUser should be allowed",
			path:        "/serverless/v1/functions/tenant1:namespace1:func1:v1/invocations",
			routePath:   "/serverless/v1/functions/:function-urn/invocations",
			method:      "POST",
			enableAuth:  true,
			authHeader:  testJWT("tenant1", jwtauth.RoleUser),
			isInvokeURL: true,
			// Empty IAM address exercises the real ValidateWithIamServer skip path after role checks pass.
			iamAddr:            "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:        "invoke URL with RoleDeveloper should be allowed",
			path:        "/serverless/v1/functions/tenant1:namespace1:func1:v1/invocations",
			routePath:   "/serverless/v1/functions/:function-urn/invocations",
			method:      "POST",
			enableAuth:  true,
			authHeader:  testJWT("tenant1", jwtauth.RoleDeveloper),
			isInvokeURL: true,
			// Empty IAM address exercises the real ValidateWithIamServer skip path after role checks pass.
			iamAddr:            "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:        "short invoke URL with RoleUser should be allowed",
			path:        "/tenant1/namespace1/function1/",
			routePath:   "/:tenant-id/:namespace/:function/",
			method:      "POST",
			enableAuth:  true,
			authHeader:  testJWT("tenant1", jwtauth.RoleUser),
			isInvokeURL: true,
			// Empty IAM address exercises the real ValidateWithIamServer skip path after role checks pass.
			iamAddr:            "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
		{
			name:               "non-invoke URL with RoleUser should be rejected",
			path:               "/serverless/v1/posix/instance/create",
			method:             "POST",
			enableAuth:         true,
			authHeader:         testJWT("tenant1", jwtauth.RoleUser),
			expectedStatusCode: http.StatusUnauthorized,
			shouldCallNext:     false,
		},
		{
			name:       "non-invoke URL with RoleDeveloper should be allowed",
			path:       "/serverless/v1/posix/instance/create",
			method:     "POST",
			enableAuth: true,
			authHeader: testJWT("tenant1", jwtauth.RoleDeveloper),
			// Empty IAM address exercises the real ValidateWithIamServer skip path after role checks pass.
			iamAddr:            "",
			expectedStatusCode: http.StatusOK,
			shouldCallNext:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup config
			origIAMAddr := config.GetConfig().IamConfig.Addr
			config.GetConfig().IamConfig.EnableFuncTokenAuth = tt.enableAuth
			config.GetConfig().IamConfig.Addr = tt.iamAddr
			defer func() {
				config.GetConfig().IamConfig.Addr = origIAMAddr
			}()

			// Create test router
			router := gin.New()
			nextCalled := false
			router.Use(func(c *gin.Context) {
				if tt.isInvokeURL {
					// GlobalJWTAuthMiddleware relies on InvokePreprocessMiddleware to set this marker in production.
					// This test focuses on role selection inside GlobalJWTAuthMiddleware, not metadata loading.
					c.Set(isInvokeURLKey, true)
				}
				c.Next()
			})
			router.Use(GlobalJWTAuthMiddleware())
			routePath := tt.routePath
			if routePath == "" {
				routePath = tt.path
			}
			router.Handle(tt.method, routePath, func(c *gin.Context) {
				nextCalled = true
				c.Status(http.StatusOK)
			})

			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.name == "tls tunnel path should also skip auth" {
				req.Header.Set("X-Forwarded-Proto", "https")
			}
			if tt.authHeader != "" {
				req.Header.Set(jwtauth.HeaderXAuth, tt.authHeader)
			}
			w := httptest.NewRecorder()

			// Execute request
			router.ServeHTTP(w, req)

			// Verify
			assert.Equal(t, tt.expectedStatusCode, w.Code)
			if tt.shouldCallNext {
				assert.True(t, nextCalled, "Next handler should be called")
			}
		})
	}
}

func TestDirectSandboxInvokeAllowsUserRole(t *testing.T) {
	gin.SetMode(gin.TestMode)
	origIAMAddr := config.GetConfig().IamConfig.Addr
	origEnableAuth := config.GetConfig().IamConfig.EnableFuncTokenAuth
	config.GetConfig().IamConfig.EnableFuncTokenAuth = true
	config.GetConfig().IamConfig.Addr = ""
	defer func() {
		config.GetConfig().IamConfig.Addr = origIAMAddr
		config.GetConfig().IamConfig.EnableFuncTokenAuth = origEnableAuth
	}()

	router := gin.New()
	router.Use(InvokePreprocessMiddleware())
	router.Use(GlobalJWTAuthMiddleware())
	router.POST("/direct/:sandbox/invoke", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/direct/default-demo/invoke", nil)
	req.Header.Set(jwtauth.HeaderXAuth, testJWT("tenant1", jwtauth.RoleUser))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
