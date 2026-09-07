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

import "net/url"

// Target is a resolved backend for a Key. Scheme decides whether the
// proxy uses the http or https (backend-TLS) transport. Source is filled by the
// resolver (e.g. "instanceinfo-watch"). Tenant is the owning tenant id, taken
// from instance metadata and used for router-side authorization (the request's
// JWT tenant must match); "" when unknown.
type Target struct {
	InstanceID string
	RuntimeID  string
	Key        Key
	TargetURL  *url.URL
	Scheme     string
	Source     string
	Tenant     string
}
