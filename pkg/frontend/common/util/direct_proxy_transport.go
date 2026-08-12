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
	"context"
	"io"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/types"
)

type simpleRuntimeInvokeRequest struct {
	ctx        context.Context
	funcMeta   api.FunctionMeta
	instanceID string
	args       []api.Arg
	options    api.InvokeOptions
}

type simpleRuntimeRawInvokeRequest struct {
	ctx     context.Context
	invoke  []byte
	options api.RawRequestOption
}

type simpleRuntimeRawCreateRequest struct {
	ctx     context.Context
	create  []byte
	options api.RawRequestOption
}

type simpleRuntimeCreateRequest struct {
	funcMeta api.FunctionMeta
	tenantID string
	args     []api.Arg
	options  api.InvokeOptions
}

type simpleRuntimeKillRequest struct {
	ctx        context.Context
	instanceID string
	tenantID   string
	signal     int
	payload    []byte
	options    api.InvokeOptions
}

type frontendProxyInvokeClient interface {
	InvokeByInstanceID(simpleRuntimeInvokeRequest) ([]byte, error)
	InvokeByInstanceIDStream(simpleRuntimeInvokeRequest, types.ResponseWriter) ([]byte, error)
	InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest) ([]byte, error)
	UploadFile(ctx context.Context, instanceID string, path string,
		reader io.Reader, tenantID string) (*frontend_proxy.FileTransferResponse, error)
	DownloadFile(ctx context.Context, instanceID string, path string,
		offset int64, tenantID string) (frontend_proxy.FrontendProxyService_DownloadFileClient, error)
}

type frontendProxyLifecycleClient interface {
	CreateInstance(simpleRuntimeCreateRequest) (string, error)
	CreateInstanceRaw(simpleRuntimeRawCreateRequest) ([]byte, error)
	KillInstance(simpleRuntimeKillRequest) error
}
