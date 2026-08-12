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
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	commontype "frontend/pkg/common/faas_common/types"
	mockUtils "frontend/pkg/common/faas_common/utils"
	"frontend/pkg/common/uuid"
	"frontend/pkg/frontend/common/httpconstant"
)

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
		req := InvokeRequest{}
		result := []byte("response")
		mock.getAsync = func(objectID string, cb api.GetAsyncCallback) {
			cb(result, nil)
		}
		res, err := c.getRes("obj1", req)
		So(err, ShouldBeNil)
		So(string(res), ShouldEqual, "response")
	})
}

type getResRuntime struct {
	mockUtils.FakeLibruntimeSdkClient
	getAsync func(objectID string, cb api.GetAsyncCallback)
}

func (g *getResRuntime) GetAsync(objectID string, cb api.GetAsyncCallback) {
	g.getAsync(objectID, cb)
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
