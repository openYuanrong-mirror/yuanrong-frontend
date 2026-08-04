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

package leaseadaptor

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"yuanrong.org/kernel/runtime/libruntime/api"

	commonconstant "frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/tls"
	commontypes "frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/types"
)

func TestCreateAcquireArgs(t *testing.T) {
	Convey("Test createAcquireArgs", t, func() {
		option := &commontypes.AcquireOption{
			DesignateInstanceID: "",
			TraceID:             "trace-123",
			PoolLabel:           "pool-test",
			ResourceSpecs: map[string]int64{
				"CPU":    1,
				"memory": 5,
			},
			InstanceLabel: "123",
			InstanceSession: &commontypes.InstanceSessionConfig{
				SessionID:   "s1",
				SessionTTL:  4,
				Concurrency: 5,
			},
		}
		funcKey := "test-func"
		defer gomonkey.ApplyFunc(getPodName, func() string {
			return "podname1"
		}).Reset()

		args, err := createAcquireArgs(option, funcKey)
		ShouldBeNil(err)
		So(len(args), ShouldEqual, 3)
		So(string(args[0].Data), ShouldEqual, "acquire#test-func")
		So(string(args[1].Data), ShouldEqual, "{\"instanceCallerPodName\":\"cG9kbmFtZTE=\",\"instanceInvokeLabel\":\"eyJYLUluc3RhbmNlLUxhYmVsIjoiMTIzIn0=\",\"instanceSessionConfig\":\"eyJzZXNzaW9uSUQiOiJzMSIsInNlc3Npb25UVEwiOjQsImNvbmN1cnJlbmN5Ijo1fQ==\",\"poolLabel\":\"cG9vbC10ZXN0\",\"resourcesData\":\"eyJDUFUiOjEsIm1lbW9yeSI6NX0=\"}")
		So(string(args[2].Data), ShouldEqual, "trace-123")
	})

	Convey("Test createAcquireArgs with enableSessionCtx", t, func() {
		defer gomonkey.ApplyFunc(getPodName, func() string {
			return "podname1"
		}).Reset()

		Convey("enableSessionCtx=true writes sessionCtxID to extraData", func() {
			option := &commontypes.AcquireOption{
				TraceID:          "trace-123",
				ResourceSpecs:    map[string]int64{"CPU": 1, "memory": 5},
				EnableSessionCtx: true,
				SessionCtxID:     "useraaa",
			}

			args, err := createAcquireArgs(option, "test-func")
			So(err, ShouldBeNil)

			var extraData map[string][]byte
			err = json.Unmarshal(args[1].Data, &extraData)
			So(err, ShouldBeNil)
			So(extraData, ShouldContainKey, commonconstant.SessionCtxID)
			So(string(extraData[commonconstant.SessionCtxID]), ShouldEqual, "useraaa")
		})

		Convey("enableSessionCtx=false does not write sessionCtxID", func() {
			option := &commontypes.AcquireOption{
				TraceID:          "trace-123",
				ResourceSpecs:    map[string]int64{"CPU": 1, "memory": 5},
				EnableSessionCtx: false,
				SessionCtxID:     "useraaa",
			}

			args, err := createAcquireArgs(option, "test-func")
			So(err, ShouldBeNil)

			var extraData map[string][]byte
			err = json.Unmarshal(args[1].Data, &extraData)
			So(err, ShouldBeNil)
			So(extraData, ShouldNotContainKey, commonconstant.SessionCtxID)
		})

		Convey("enableSessionCtx=true with empty sessionCtxID writes empty string", func() {
			option := &commontypes.AcquireOption{
				TraceID:          "trace-123",
				ResourceSpecs:    map[string]int64{"CPU": 1, "memory": 5},
				EnableSessionCtx: true,
				SessionCtxID:     "",
			}

			args, err := createAcquireArgs(option, "test-func")
			So(err, ShouldBeNil)

			var extraData map[string][]byte
			err = json.Unmarshal(args[1].Data, &extraData)
			So(err, ShouldBeNil)
			So(extraData, ShouldContainKey, commonconstant.SessionCtxID)
			So(string(extraData[commonconstant.SessionCtxID]), ShouldEqual, "")
		})
	})
}

func TestCreateBatchRetainArgs(t *testing.T) {
	Convey("Test createBatchRetainArgs", t, func() {
		batch := &BatchRetainLeaseInfos{
			targetName: "l1,l2,l3",
			infos: map[string]*BatchRetainLeaseInfo{
				"l1": {
					ProcReqNum:    1,
					AvgProcTime:   2,
					MaxProcTime:   3,
					IsAbnormal:    false,
					ReacquireData: nil,
					FunctionKey:   "123456/hello/latest",
					PoolKey:       "",
				},
				"l2": {
					ProcReqNum:    2,
					AvgProcTime:   3,
					MaxProcTime:   4,
					IsAbnormal:    false,
					ReacquireData: nil,
					FunctionKey:   "123456/hello/latest",
					PoolKey:       "",
				},
				"l3": {
					ProcReqNum:    3,
					AvgProcTime:   4,
					MaxProcTime:   5,
					IsAbnormal:    false,
					ReacquireData: nil,
					FunctionKey:   "123456/hello/latest",
					PoolKey:       "",
				},
			},
			SchedulerAddress: "127.0.0.1:8889",
		}

		args, err := createBatchRetainArgs(batch, "traceId-123456")
		So(err == nil, ShouldBeTrue)
		So(len(args), ShouldEqual, 3)
		So(string(args[0].Data), ShouldEqual, "batchRetain#l1,l2,l3")
		So(string(args[2].Data), ShouldEqual, "traceId-123456")
	})
}

func TestDoAcquireInvoke(t *testing.T) {
	Convey("Test doAcquireInvoke", t, func() {
		var (
			option   *commontypes.AcquireOption
			funcKey  string
			testIP   string
			testBody []byte
		)

		option = &commontypes.AcquireOption{
			TraceID: "test-trace",
		}
		funcKey = "test-func"
		testIP = "127.0.0.1"
		testBody, _ = json.Marshal(&commontypes.InstanceResponse{
			InstanceAllocationInfo: commontypes.InstanceAllocationInfo{
				InstanceID: "testInstanceId",
			},
			ErrorCode:     commonconstant.InsReqSuccessCode,
			ErrorMessage:  "",
			SchedulerTime: 0,
		})

		Convey("Should return instance when request success", func() {
			defer gomonkey.ApplyFunc(prepareSchedulerRequest, func(_ *fasthttp.Request, _ string, _ []*api.Arg, _, _ string) error {
				return nil
			}).Reset()

			defer gomonkey.ApplyFunc(requestScheduler, func(_ *fasthttp.Request, resp *fasthttp.Response, _ int64) error {
				resp.SetBody(testBody)
				return nil
			}).Reset()

			instance, err := doAcquireInvoke(option, testIP, funcKey, 5)
			So(err, ShouldBeNil)
			So(instance.InstanceID, ShouldEqual, "testInstanceId")
		})

		Convey("Should return error when prepare request failed", func() {
			defer gomonkey.ApplyFunc(prepareSchedulerRequest, func(_ *fasthttp.Request, _ string, _ []*api.Arg, _, _ string) error {
				return errors.New("prepare failed")
			}).Reset()

			_, err := doAcquireInvoke(option, testIP, funcKey, 5)
			So(err, ShouldNotBeNil)
		})

		Convey("Should return error when request failed", func() {
			defer gomonkey.ApplyFunc(prepareSchedulerRequest, func(_ *fasthttp.Request, _ string, _ []*api.Arg, _, _ string) error {
				return nil
			}).Reset()

			defer gomonkey.ApplyFunc(requestScheduler, func(_ *fasthttp.Request, _ *fasthttp.Response, _ int64) error {
				return errors.New("request failed")
			}).Reset()

			_, err := doAcquireInvoke(option, testIP, funcKey, 5)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestDoReleaseInvoke(t *testing.T) {
	Convey("Test doReleaseInvoke", t, func() {
		Convey("Should not panic when scheduler not found", func() {
			defer gomonkey.ApplyMethodFunc(schedulerproxy.Proxy, "GetByAcquireOption",
				func(_ string, _ *commontypes.AcquireOption,
					_ api.FormatLogger) (*schedulerproxy.SchedulerNodeInfo, error) {
					return nil, errors.New("not found")
				}).Reset()

			So(func() {
				leasePool := LeasePool{}
				leasePool.doReleaseInvoke("test-func", "lease-123", &commontypes.AcquireOption{}, &InstanceReport{}, "")
			}, ShouldNotPanic)
		})

		Convey("Should call requestScheduler when scheduler exist", func() {
			called := false
			defer gomonkey.ApplyMethodFunc(schedulerproxy.Proxy, "GetByAcquireOption",
				func(_ string, _ *commontypes.AcquireOption,
					_ api.FormatLogger) (*schedulerproxy.SchedulerNodeInfo, error) {
					return &schedulerproxy.SchedulerNodeInfo{
						InstanceInfo: &commontypes.InstanceInfo{
							Address: "127.0.0.1",
						},
					}, nil
				}).Reset()

			defer gomonkey.ApplyFunc(requestScheduler, func(_ *fasthttp.Request, _ *fasthttp.Response, _ int64) error {
				called = true
				return nil
			}).Reset()

			leasePool := LeasePool{}

			leasePool.doReleaseInvoke("test-func", "lease-123", &commontypes.AcquireOption{}, &InstanceReport{}, "")

			So(called, ShouldBeTrue)
		})

	})
}

func TestResolveLeaseSchedulerPrefersRecordedOwner(t *testing.T) {
	Convey("recorded scheduler bypasses owner recalculation", t, func() {
		owner := &schedulerproxy.SchedulerNodeInfo{InstanceInfo: &commontypes.InstanceInfo{}}
		defer gomonkey.ApplyMethodReturn(
			schedulerproxy.Proxy, "GetSchedulerByInstanceId", owner).Reset()
		defer gomonkey.ApplyMethodFunc(schedulerproxy.Proxy, "GetByAcquireOption",
			func(_ string, _ *commontypes.AcquireOption,
				_ api.FormatLogger) (*schedulerproxy.SchedulerNodeInfo, error) {
				t.Fatal("unexpected owner recalculation")
				return nil, nil
			}).Reset()

		got, recalculated, err := resolveLeaseScheduler(
			"test-func", &commontypes.AcquireOption{}, "owner", log.GetLogger())

		So(err, ShouldBeNil)
		So(recalculated, ShouldBeFalse)
		So(got, ShouldEqual, owner)
	})
}

func TestDoBatchRetainInvoke(t *testing.T) {
	Convey("Test doBatchRetainInvoke", t, func() {
		Convey("Should not panic when scheduler not found", func() {
			defer gomonkey.ApplyMethodFunc(schedulerproxy.Proxy, "GetByAcquireOption",
				func(_ string, _ *commontypes.AcquireOption,
					_ api.FormatLogger) (*schedulerproxy.SchedulerNodeInfo, error) {
					return nil, errors.New("not found")
				}).Reset()

			So(func() {
				leasePool := LeasePool{}
				leasePool.doReleaseInvoke("test-func", "lease-123", &commontypes.AcquireOption{}, &InstanceReport{}, "")
			}, ShouldNotPanic)
		})

		Convey("Should call requestScheduler when scheduler exist", func() {
			called := false
			defer gomonkey.ApplyMethodFunc(schedulerproxy.Proxy, "GetByAcquireOption",
				func(_ string, _ *commontypes.AcquireOption,
					_ api.FormatLogger) (*schedulerproxy.SchedulerNodeInfo, error) {
					return &schedulerproxy.SchedulerNodeInfo{
						InstanceInfo: &commontypes.InstanceInfo{
							Address: "127.0.0.1",
						},
					}, nil
				}).Reset()

			defer gomonkey.ApplyFunc(requestScheduler, func(_ *fasthttp.Request, _ *fasthttp.Response, _ int64) error {
				called = true
				return nil
			}).Reset()

			doBatchRetainInvoke(&BatchRetainLeaseInfos{}, "traceId-123456")
			So(called, ShouldBeTrue)
		})
	})
}

func TestRequestScheduler(t *testing.T) {
	Convey("Test requestScheduler", t, func() {
		mockReq := &fasthttp.Request{}
		testClient := &fasthttp.Client{}

		Convey("Should return body when request success", func() {
			defer gomonkey.ApplyFunc(httputil.GetSchedulerClient, func() *fasthttp.Client {
				return testClient
			}).Reset()

			defer gomonkey.ApplyMethodFunc(testClient, "DoTimeout", func(_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
				resp.SetStatusCode(http.StatusOK)
				resp.SetBody([]byte("test-body"))
				return nil
			}).Reset()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseResponse(resp)
			err := requestScheduler(mockReq, resp, 5)
			So(err, ShouldBeNil)
			So(string(resp.Body()), ShouldEqual, "test-body")
		})

		Convey("Should return error when request failed", func() {
			defer gomonkey.ApplyFunc(httputil.GetSchedulerClient, func() *fasthttp.Client {
				return testClient
			}).Reset()

			defer gomonkey.ApplyMethodFunc(testClient, "DoTimeout", func(_ *fasthttp.Request, _ *fasthttp.Response, _ time.Duration) error {
				return errors.New("request failed")
			}).Reset()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseResponse(resp)
			err := requestScheduler(mockReq, resp, 5)
			So(err, ShouldNotBeNil)
		})

		Convey("Should return error when status not OK", func() {
			defer gomonkey.ApplyFunc(httputil.GetSchedulerClient, func() *fasthttp.Client {
				return testClient
			}).Reset()

			defer gomonkey.ApplyMethodFunc(testClient, "DoTimeout", func(_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
				resp.SetStatusCode(http.StatusInternalServerError)
				return nil
			}).Reset()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseResponse(resp)
			err := requestScheduler(mockReq, resp, 5)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestPrepareSchedulerRequest(t *testing.T) {
	req := &fasthttp.Request{}
	dstHost := "scheduler.example.com"
	traceID := "abc123-def456"
	traceParent := "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01"
	args := []*api.Arg{
		{TenantID: "tenantID"},
	}
	originalConfig := config.GetConfig()
	defer func() {
		config.SetConfig(*originalConfig)
	}()
	testConfig := types.Config{
		HTTPSConfig: &tls.InternalHTTPSConfig{
			HTTPSEnable: true,
		},
	}
	config.SetConfig(testConfig)

	err := prepareSchedulerRequest(req, dstHost, args, traceID, traceParent)

	assert.NoError(t, err)
	assert.Equal(t, callSchedulerPath, string(req.URI().Path()))
	assert.Equal(t, "http", string(req.URI().Scheme()))
	assert.Equal(t, http.MethodPost, string(req.Header.Method()))
	assert.Equal(t, dstHost, string(req.Host()))
	assert.Equal(t, traceID, string(req.Header.Peek(commonconstant.HeaderTraceID)))
	assert.Equal(t, traceParent, string(req.Header.Peek(commonconstant.HeaderTraceParent)))
	expectedBody, _ := json.Marshal(args)
	assert.Equal(t, expectedBody, req.Body())
}
