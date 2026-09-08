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

package sandboxrouter

import (
	"testing"

	"frontend/pkg/frontend/sandboxrouter/config"
)

func TestEffectiveConfigDefaultsNilToEnabled(t *testing.T) {
	cfg := effectiveConfig(nil)
	if cfg == nil {
		t.Fatal("effectiveConfig(nil) returned nil")
	}
	if !cfg.Enabled {
		t.Fatal("nil sandboxrouter config should default to enabled")
	}
}

func TestEffectiveConfigRespectsExplicitDisabled(t *testing.T) {
	input := &config.SandboxRouterConfig{Enabled: false}
	cfg := effectiveConfig(input)
	if cfg != input {
		t.Fatal("effectiveConfig should preserve explicit config pointer")
	}
	if cfg.Enabled {
		t.Fatal("explicit sandboxrouter enabled=false should remain disabled")
	}
}

func TestStartIfEnabledDelegatesListenerToExternalProcess(t *testing.T) {
	t.Setenv("SANDBOX_ROUTER_EXTERNAL", "true")
	originalStartWatch := startExternalInstanceCacheWatch
	t.Cleanup(func() { startExternalInstanceCacheWatch = originalStartWatch })
	called := false
	startExternalInstanceCacheWatch = func(stopCh <-chan struct{}) error {
		if stopCh == nil {
			t.Fatal("instance cache watcher requires the frontend stop channel")
		}
		called = true
		return nil
	}
	if err := StartIfEnabled(&config.SandboxRouterConfig{Enabled: true}, make(chan struct{})); err != nil {
		t.Fatalf("external sandbox router delegation failed: %v", err)
	}
	if !called {
		t.Fatal("external sandbox router must keep the frontend instance cache watcher active")
	}
}
