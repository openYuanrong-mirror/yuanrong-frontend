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

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"frontend/pkg/common/job"
	"net/http"
	"testing"

	"frontend/pkg/frontend/common/jwtauth"
	"frontend/pkg/frontend/sandboxrouter/execendpoint"
	"github.com/stretchr/testify/require"
	"yuanrong.org/kernel/runtime/libruntime/api"
)

func TestDeleteHandlerRetainedFailure(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		status               int32
		tenant               string
		deleted, unavailable bool
		want                 int
		kills, reads         int
	}{
		{"fatal already deleted", execendpoint.StatusFatal, "tenant-owner", true, false, 200, 0, 1},
		{"failed already deleted", execendpoint.StatusFailed, "tenant-owner", true, false, 200, 0, 1},
		{"fatal still exists", execendpoint.StatusFatal, "tenant-owner", false, false, 500, 1, 1},
		{"authority unavailable", execendpoint.StatusFatal, "tenant-owner", false, true, 503, 0, 1},
		{"cross tenant", execendpoint.StatusFatal, "another-tenant", true, false, 403, 0, 0},
		{"running uses kill", execendpoint.StatusRunning, "tenant-owner", true, false, 500, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "sandbox-retained-delete"
			execendpoint.Default().PutSummary(execendpoint.Summary{InstanceID: id, TenantID: "tenant-owner", StatusCode: tc.status, NodeID: "original-node"})
			t.Cleanup(func() { execendpoint.Default().Delete(id) })
			original := confirmSandboxInstanceDeleted
			t.Cleanup(func() { confirmSandboxInstanceDeleted = original })
			reads, kills := 0, 0
			confirmSandboxInstanceDeleted = func(ctx context.Context, instanceID string) (bool, error) {
				reads++
				require.Equal(t, id, instanceID)
				if tc.unavailable {
					return false, errors.New("etcd unavailable")
				}
				return tc.deleted, nil
			}
			setAPIClientsForTest(t, &runtimeStub{kill: func(string, int, []byte, api.InvokeOptions) error {
				kills++
				return errors.New("owner has no instance")
			}})
			// Repeated DELETE remains authorized through retained history and succeeds
			// without erasing the watcher's deletion/version fence.
			repeats := 1
			if tc.want == http.StatusOK {
				repeats = 2
			}
			for i := 0; i < repeats; i++ {
				ctx, rec := deleteTestContext(t, id, tc.tenant, jwtauth.RoleDeveloper)
				DeleteHandler(ctx)
				require.Equal(t, tc.want, rec.Code)
				if tc.want == http.StatusOK {
					var response job.Response
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
					var data map[string]string
					require.NoError(t, json.Unmarshal(response.Data, &data))
					require.Equal(t, "deleted", data["status"])
				}
			}
			require.Equal(t, tc.kills*repeats, kills)
			require.Equal(t, tc.reads*repeats, reads)
			_, retained := execendpoint.Default().GetSummary(id)
			require.True(t, retained)
		})
	}
}
