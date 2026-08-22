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

// InstanceInfo is the minimal view of the /sn/instance JSON that the sandbox
// router needs. The full kernel InstanceInfo proto (serialized via
// MessageToJsonString) carries far more; we only read these fields. proxyGrpcAddress
// (proto field 38) and extensions["portForward"] (proto field 33) are NOT present
// in frontend's existing InstanceSpecification struct, which is why we parse our own.
type InstanceInfo struct {
	InstanceID       string            `json:"instanceID"`
	TenantID         string            `json:"tenantID"`
	Function         string            `json:"function"`
	FunctionProxyID  string            `json:"functionProxyID"`
	ProxyGrpcAddress string            `json:"proxyGrpcAddress"`
	RuntimeAddress   string            `json:"runtimeAddress"`
	ContainerID      string            `json:"containerID"`
	ContainerIP      string            `json:"containerIP"`
	RequestID        string            `json:"requestID"`
	Version          JSONInt64         `json:"version"`
	InstanceStatus   InstanceStatus    `json:"instanceStatus"`
	Extensions       map[string]string `json:"extensions"`
}

// InstanceStatus mirrors the kernel-controlled status code; only Code is used
// by the resolver to decide RUNNING vs exited.
type InstanceStatus struct {
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}
