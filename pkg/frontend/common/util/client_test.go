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

package util

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	commontype "frontend/pkg/common/faas_common/types"
	mockUtils "frontend/pkg/common/faas_common/utils"
	"frontend/pkg/common/uuid"
	"frontend/pkg/frontend/common/httpconstant"
)

const sseEOFWaitCheckTimeout = 100 * time.Millisecond

func TestNewClientLibruntime(t *testing.T) {
	mock := &mockUtils.FakeLibruntimeSdkClient{}
	Convey("TestNewClientLibruntime", t, func() {
		testInstID := uuid.New().String()
		returnObjID := uuid.New().String()
		result := []byte(uuid.New().String())
		req := InvokeRequest{
			Function:   "test",
			Args:       nil,
			InstanceID: testInstID,
			InstanceSession: &commontype.InstanceSessionConfig{
				SessionID:   "session-1",
				SessionTTL:  30,
				Concurrency: 1,
			},
		}

		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(mock), "GetAsync",
				func(_ *mockUtils.FakeLibruntimeSdkClient, objectID string, cb api.GetAsyncCallback) {
					cb(result, nil)
					return
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mock), "GetEvent",
				func(_ *mockUtils.FakeLibruntimeSdkClient, objectID string, cb api.GetEventCallback) {
					cb(result, nil)
					return
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mock), "DeleteGetEventCallback",
				func(_ *mockUtils.FakeLibruntimeSdkClient, objectID string) {
					return
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mock), "InvokeByFunctionName",
				func(_ *mockUtils.FakeLibruntimeSdkClient, funcMeta api.FunctionMeta, args []api.Arg,
					invokeOpt api.InvokeOptions) (string, error) {
					return testInstID, nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mock), "InvokeByInstanceId",
				func(_ *mockUtils.FakeLibruntimeSdkClient, funcMeta api.FunctionMeta, instanceID string, args []api.Arg,
					invokeOpt api.InvokeOptions) (string, error) {
					So(instanceID, ShouldEqual, testInstID)
					So(invokeOpt.InstanceSession, ShouldNotBeNil)
					So(invokeOpt.InstanceSession.SessionID, ShouldEqual, "session-1")
					So(invokeOpt.InstanceSession.SessionTTL, ShouldEqual, 30)
					So(invokeOpt.InstanceSession.Concurrency, ShouldEqual, 1)
					return returnObjID, nil
				}),
		}
		Reset(func() {
			for _, patch := range patches {
				patch.Reset()
			}
		})

		client := newDefaultClientLibruntime(mock)
		So(client, ShouldNotBeNil)
		res, err := client.InvokeByName(req)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, result)

		res, err = client.Invoke(req)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, result)
	})
}

func Test_defaultClient_AcquireInstance(t *testing.T) {
	Convey("test AcquireInstance", t, func() {
		Convey("baseline", func() {
			mock := &mockUtils.FakeLibruntimeSdkClient{}
			client := newDefaultClientLibruntime(mock)
			instance, err := client.AcquireInstance("func", commontype.AcquireOption{
				DesignateInstanceID: "id",
				FuncSig:             "aaa",
				ResourceSpecs: map[string]int64{
					constant.ResourceCPUName:    1000,
					constant.ResourceMemoryName: 1000,
				},
				Timeout:        100,
				TrafficLimited: false,
			})
			So(err, ShouldBeNil)
			So(instance, ShouldNotBeNil)
		})
	})
}

func Test_defaultClient_getRes(t *testing.T) {
	Convey("Test (c *defaultClient) getRes", t, func() {
		mock := &getResRuntime{}
		c := newDefaultClientLibruntime(mock)
		clientDisconnectChan := make(chan struct{})
		req := InvokeRequest{
			ResponseWriter: &mockResponseWriter{
				clientDisconnectChan: clientDisconnectChan,
				sseWriteFunc: func(data []byte) (int, error) {
					return len(data), nil
				},
			},
		}
		result := []byte("response")
		mock.getEvent = func(objectID string, cb api.GetEventCallback) {
			cb([]byte("{}"), nil)
		}

		Convey("When request is not SSE", func() {
			mock.getAsync = func(objectID string, cb api.GetAsyncCallback) {
				cb(result, nil)
			}
			req.AcceptHeader = "application/json"
			res, err := c.getRes("obj1", req)
			So(err, ShouldBeNil)
			So(string(res), ShouldEqual, "response")
		})

		Convey("When request is SSE", func() {
			mock.getAsync = func(objectID string, cb api.GetAsyncCallback) {
				cb(result, errors.New("test error"))
			}
			req.AcceptHeader = httpconstant.AcceptEventStream
			res, err := c.getRes("obj1", req)
			So(err, ShouldNotBeNil)
			So(string(res), ShouldEqual, "response")
		})

		Convey("When request is SSE and event EOF arrives before async result", func() {
			var written []byte
			asyncDone := make(chan struct{})
			mock.getAsync = func(objectID string, cb api.GetAsyncCallback) {
				<-asyncDone
				cb(result, nil)
			}
			mock.getEvent = func(objectID string, cb api.GetEventCallback) {
				go func() {
					cb([]byte("stream-data"), nil)
					cb([]byte("yuanrong_event_EOF"), nil)
				}()
			}
			req.AcceptHeader = httpconstant.AcceptEventStream
			req.ResponseWriter = &mockResponseWriter{
				clientDisconnectChan: clientDisconnectChan,
				sseWriteFunc: func(data []byte) (int, error) {
					written = append([]byte{}, data...)
					return len(data), nil
				},
			}

			done := make(chan struct{})
			var res []byte
			var err error
			go func() {
				res, err = c.getRes("obj1", req)
				close(done)
			}()
			select {
			case <-done:
				t.Fatal("getRes should wait for async result after SSE EOF")
			case <-time.After(sseEOFWaitCheckTimeout):
			}
			close(asyncDone)
			select {
			case <-done:
				So(err, ShouldBeNil)
				So(string(res), ShouldEqual, "response")
				So(string(written), ShouldEqual, "stream-data")
			case <-time.After(time.Second):
				t.Fatal("getRes should return after async result is ready")
			}
		})
	})
}

type getResRuntime struct {
	mockUtils.FakeLibruntimeSdkClient
	getAsync func(objectID string, cb api.GetAsyncCallback)
	getEvent func(objectID string, cb api.GetEventCallback)
}

func (g *getResRuntime) GetAsync(objectID string, cb api.GetAsyncCallback) {
	g.getAsync(objectID, cb)
}

func (g *getResRuntime) GetEvent(objectID string, cb api.GetEventCallback) {
	g.getEvent(objectID, cb)
}

type mockResponseWriter struct {
	clientDisconnectChan <-chan struct{}
	sseWriteFunc         func([]byte) (int, error)
}

func (m *mockResponseWriter) ClientDisconnectChan() <-chan struct{} {
	return m.clientDisconnectChan
}

func (m *mockResponseWriter) SSEWrite(data []byte) (int, error) {
	return m.sseWriteFunc(data)
}

func Test_defaultClient_handleEvent(t *testing.T) {
	Convey("Test (c *defaultClient) handleEvent", t, func() {
		mock := &mockUtils.FakeLibruntimeSdkClient{}
		c := newDefaultClientLibruntime(mock)
		clientDisconnectChan := make(chan struct{})
		req := InvokeRequest{
			ResponseWriter: &mockResponseWriter{
				clientDisconnectChan: clientDisconnectChan,
				sseWriteFunc: func(data []byte) (int, error) {
					return len(data), nil
				},
			},
		}
		defer gomonkey.ApplyMethod(reflect.TypeOf(mock), "DeleteGetEventCallback",
			func(_ *mockUtils.FakeLibruntimeSdkClient, objectID string) {
				return
			}).Reset()
		Convey("When handling an event with error", func() {
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 1),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			eventErr := errors.New("some error")
			sseChan.Event <- sseEvent{Data: []byte(`{"key": "value"}`), Err: eventErr}
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
			So(sseChan.EventErr, ShouldEqual, eventErr)
		})
		Convey("When handling an event with yuanrong_event_EOF", func() {
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 1),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			sseChan.Event <- sseEvent{Data: []byte(`yuanrong_event_EOF`)}
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
		})
		Convey("When handling an event with valid data", func() {
			req := InvokeRequest{
				ResponseWriter: &mockResponseWriter{
					clientDisconnectChan: clientDisconnectChan,
					sseWriteFunc: func(data []byte) (int, error) {
						return 0, errors.New("write error")
					},
				},
			}
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 1),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			sseChan.Event <- sseEvent{Data: []byte(`{"key": "value"}`)}
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
			So(sseChan.EventErr, ShouldNotBeNil)
		})
		Convey("When handling a non-json stream event", func() {
			var written []byte
			req := InvokeRequest{
				ResponseWriter: &mockResponseWriter{
					clientDisconnectChan: clientDisconnectChan,
					sseWriteFunc: func(data []byte) (int, error) {
						written = append([]byte{}, data...)
						return len(data), nil
					},
				},
			}
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 2),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			sseChan.Event <- sseEvent{Data: []byte(`plain stream data`)}
			sseChan.Event <- sseEvent{Data: []byte(`yuanrong_event_EOF`)}
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
			So(sseChan.EventErr, ShouldBeNil)
			So(string(written), ShouldEqual, "plain stream data")
		})
		Convey("When early close StopSSEHandle", func() {
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 1),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			close(stopSSEHandle)
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
		})
		Convey("When handle an event with a disconnected client", func() {
			close(clientDisconnectChan)
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 1),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
			So(sseChan.EventErr, ShouldNotBeNil)
		})
		Convey("When GetEvent callback returns an error with non-json payload", func() {
			sseChan := &SSEChan{
				Event:     make(chan sseEvent, 1),
				WaitEvent: make(chan struct{}, 1),
			}
			stopSSEHandle := make(chan struct{})
			eventErr := errors.New("instance has already exited")
			sseChan.Event <- sseEvent{Data: []byte(`instance has already exited`), Err: eventErr}
			c.handleEvent("objID", sseChan, req, stopSSEHandle)
			So(<-sseChan.WaitEvent, ShouldNotBeNil)
			So(sseChan.EventErr, ShouldEqual, eventErr)
		})
	})
}

func Test_convertCommonInvokeOption(t *testing.T) {
	Convey("Test convertCommonInvokeOption", t, func() {
		Convey("check covert common invoke options", func() {
			req := InvokeRequest{
				InvokeTag: map[string]string{
					"tagKey": "tagValue",
				},
				TraceID:       "id2",
				TraceParent:   "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01",
				InvokeTimeout: 60,
				AcceptHeader:  httpconstant.AcceptEventStream,
				IsInterrupted: true,
				SessionCtxID:  "session-context",
			}
			res := convertCommonInvokeOption(req)
			So(res.TraceID, ShouldNotBeEmpty)
			So(res.Timeout, ShouldNotEqual, 0)
			So(res.InvokeLabels, ShouldNotBeNil)
			So(res.InvokeLabels["accept"], ShouldNotBeNil)
			So(res.CustomExtensions["tagKey"], ShouldEqual, "tagValue")
			So(res.CustomExtensions[traceParentExtensionKey], ShouldEqual, req.TraceParent)
			So(res.IsInterrupted, ShouldBeTrue)
			So(res.SessionCtxID, ShouldEqual, req.SessionCtxID)
		})

		Convey("check route address option", func() {
			req := InvokeRequest{
				RouteAddress:     "scheduler-proxy",
				BypassDataSystem: true,
			}
			res := convertCommonInvokeOption(req)
			So(res.CreateOpt["YR_ROUTE"], ShouldEqual, "scheduler-proxy")
		})
	})
}

func TestConvertAcquireOption(t *testing.T) {
	Convey("Test convertAcquireOption", t, func() {
		req := commontype.AcquireOption{
			TraceID:       "id3",
			TraceParent:   "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01",
			SchedulerID:   "scheduler-id",
			ResourceSpecs: map[string]int64{"cpu": 1},
			Timeout:       60,
			SessionCtxID:  "session-context",
		}

		res := convertAcquireOption(req)
		So(res.TraceID, ShouldEqual, req.TraceID)
		So(res.CustomExtensions, ShouldNotBeNil)
		So(res.CustomExtensions[traceParentExtensionKey], ShouldEqual, req.TraceParent)
		So(res.SessionCtxID, ShouldEqual, req.SessionCtxID)
	})
}
