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

package route

import "fmt"

// InstanceFailure is a lifecycle rejection, rather than a missing port route.
// Tenant and RuntimeID are internal identity facts and must not be serialized.
type InstanceFailure struct {
	InstanceID string
	Tenant     string
	RuntimeID  string
	Status     InstanceStatus
}

func (e *InstanceFailure) Error() string {
	return fmt.Sprintf("instance %s state %d: %s", e.InstanceID, e.Status.Code, e.Status.Msg)
}

// FailureFor returns an independent snapshot so cache updates cannot change an
// in-flight response. FATAL includes both normal and abnormal process exits.
func FailureFor(info InstanceInfo) *InstanceFailure {
	switch info.InstanceStatus.Code {
	case 4, 6, 7: // FAILED, FATAL, SCHEDULE_FAILED
		return &InstanceFailure{InstanceID: info.InstanceID, Tenant: info.TenantID,
			RuntimeID: info.RuntimeID, Status: info.InstanceStatus}
	default:
		return nil
	}
}
