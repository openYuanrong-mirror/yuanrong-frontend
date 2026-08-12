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
	"testing"

	"github.com/stretchr/testify/require"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/common/httpconstant"
)

type directInvokeTransportStub struct {
	request     simpleRuntimeInvokeRequest
	streamCalls int
	unaryCalls  int
	payload     []byte
}

func (s *directInvokeTransportStub) InvokeByInstanceID(req simpleRuntimeInvokeRequest) ([]byte, error) {
	s.unaryCalls++
	s.request = req
	return s.payload, nil
}

func (s *directInvokeTransportStub) InvokeByInstanceIDStream(
	req simpleRuntimeInvokeRequest,
	_ types.ResponseWriter,
) ([]byte, error) {
	s.streamCalls++
	s.request = req
	return s.payload, nil
}

func (s *directInvokeTransportStub) InvokeByInstanceIDRaw(simpleRuntimeRawInvokeRequest) ([]byte, error) {
	return nil, nil
}

func (s *directInvokeTransportStub) UploadFile(
	context.Context,
	string,
	string,
	io.Reader,
	string,
) (*frontend_proxy.FileTransferResponse, error) {
	return nil, nil
}

func (s *directInvokeTransportStub) DownloadFile(
	context.Context,
	string,
	string,
	int64,
	string,
) (frontend_proxy.FrontendProxyService_DownloadFileClient, error) {
	return nil, nil
}

type directSSEWriterStub struct {
	disconnect chan struct{}
}

func (w *directSSEWriterStub) SSEWrite(data []byte) (int, error) {
	return len(data), nil
}

func (w *directSSEWriterStub) ClientDisconnectChan() <-chan struct{} {
	return w.disconnect
}

func TestDirectProxyClientSelectsStreamingInvokeAndDropsLegacyRoute(t *testing.T) {
	transport := &directInvokeTransportStub{payload: []byte("final")}
	client := &directProxyClient{invokeClient: transport}
	w := &directSSEWriterStub{disconnect: make(chan struct{})}

	req, err := NewDirectInvokeRequest(InvokeRequest{
		Function:       "tenant/function/$latest",
		InstanceID:     "instance-a",
		TenantID:       "tenant-a",
		TraceID:        "trace-a",
		RouteAddress:   "legacy-route-must-not-propagate",
		AcceptHeader:   httpconstant.AcceptEventStream,
		ResponseWriter: w,
		Args: []*api.Arg{{
			Type: api.Value,
			Data: []byte("arg"),
		}},
	})
	require.NoError(t, err)
	got, err := client.Invoke(req)

	require.NoError(t, err)
	require.Equal(t, []byte("final"), got)
	require.Equal(t, 1, transport.streamCalls)
	require.Zero(t, transport.unaryCalls)
	require.Equal(t, "instance-a", transport.request.instanceID)
	require.Nil(t, transport.request.options.CreateOpt)
	require.Equal(t, httpconstant.AcceptEventStream, transport.request.options.CustomExtensions["Accept"])
	require.Empty(t, transport.request.options.InvokeLabels)
	require.Len(t, transport.request.args, 1)
	require.Equal(t, "tenant-a", transport.request.args[0].TenantID)
}

func TestDirectProxyClientRejectsObjectReferenceArguments(t *testing.T) {
	transport := &directInvokeTransportStub{}

	_, err := NewDirectInvokeRequest(InvokeRequest{
		Function:   "tenant/function/$latest",
		InstanceID: "instance-a",
		Args: []*api.Arg{{
			Type: api.ArgType(1),
			Data: []byte("object-id"),
		}},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "ObjectRef or nested refs")
	require.Zero(t, transport.unaryCalls)
	require.Zero(t, transport.streamCalls)
}
