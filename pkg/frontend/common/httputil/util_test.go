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

package httputil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"frontend/pkg/common/faas_common/localauth"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/snerror"
	"frontend/pkg/common/faas_common/statuscode"
	"frontend/pkg/common/faas_common/tls"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/responsehandler"
	"frontend/pkg/frontend/types"
)

func TestGetClient(t *testing.T) {
	defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
		return &types.Config{
			HTTPConfig:  &types.FrontendHTTP{},
			HTTPSConfig: &tls.InternalHTTPSConfig{},
		}
	}).Reset()
	convey.Convey("TestGetClient", t, func() {
		client := GetHeartbeatClient()
		convey.So(client, convey.ShouldNotBeNil)
		client = GetGlobalClient()
		convey.So(client, convey.ShouldNotBeNil)
	})
}

func TestTranslateInvokeMsgToCallReq(t *testing.T) {
	convey.Convey("TranslateInvokeMsgToCallReq", t, func() {
		defer gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
			return nil, errors.New("json marshal error")
		}).Reset()
		_, err := TranslateInvokeMsgToCallReq(&types.InvokeProcessContext{
			ReqHeader:  map[string]string{},
			RespHeader: map[string]string{},
		})
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("TranslateInvokeMsgToCallReq success", t, func() {
		defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
			return &types.Config{
				BusinessType: constant.BusinessTypeWiseCloud,
			}
		}).Reset()
		_, err := TranslateInvokeMsgToCallReq(&types.InvokeProcessContext{
			ReqHeader:  map[string]string{},
			RespHeader: map[string]string{},
		})
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestJudgeRetry(t *testing.T) {
	convey.Convey("test JudgeRetry", t, func() {
		convey.Convey("test failed", func() {
			processCtx := types.CreateInvokeProcessContext()
			err := errors.New("failed to test")
			JudgeRetry(err, processCtx)
			convey.So(processCtx.ShouldRetry, convey.ShouldEqual, false)
		})
		convey.Convey("test success 1", func() {
			processCtx := types.CreateInvokeProcessContext()
			err := errors.New("failed to invoke, code: 1007, message: exit")
			JudgeRetry(err, processCtx)
			convey.So(processCtx.ShouldRetry, convey.ShouldEqual, true)
		})
		convey.Convey("test success 2", func() {
			processCtx := types.CreateInvokeProcessContext()
			err := errors.New("failed to invoke, code: 3001, message: exit")
			JudgeRetry(err, processCtx)
			convey.So(processCtx.ShouldRetry, convey.ShouldEqual, true)
		})
		convey.Convey("test success 3", func() {
			processCtx := types.CreateInvokeProcessContext()
			err := errors.New("failed to get invoke response: invokeRsp is nil, XXX")
			JudgeRetry(err, processCtx)
			convey.So(processCtx.ShouldRetry, convey.ShouldEqual, true)
		})
	})
}

func TestInitTraceID(t *testing.T) {
	convey.Convey("InitTraceID", t, func() {
		convey.Convey("not empty", func() {
			defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
				return &types.Config{
					BusinessType: constant.BusinessTypeWiseCloud,
				}
			}).Reset()
			rw := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rw)
			ctx.Request, _ = http.NewRequest("", "", bytes.NewBuffer(nil))
			ctx.Request.Header.Add("constant.CaaSHeaderTraceID", "test-traceID")
			id := InitTraceID(ctx)
			convey.So(id, convey.ShouldNotEqual, "test-traceID")
		})

		convey.Convey("empty trace id", func() {
			rw := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rw)
			ctx.Request, _ = http.NewRequest("", "", bytes.NewBuffer(nil))
			ctx.Request.Header.Add(constant.HeaderRequestID, "")
			id := InitTraceID(ctx)
			convey.So(id, convey.ShouldNotEqual, "")
		})

		convey.Convey("long trace id", func() {
			rw := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rw)
			ctx.Request, _ = http.NewRequest("", "", bytes.NewBuffer(nil))
			traceID := ""
			for i := 0; i < constant.MaxTraceIDLength; i++ {
				traceID += strconv.Itoa(i)
			}
			fmt.Println("123:", len(traceID))
			ctx.Request.Header.Add(constant.HeaderRequestID, traceID)
			id := InitTraceID(ctx)
			convey.So(len(id), convey.ShouldEqual, constant.MaxTraceIDLength)
		})
	})
}

func TestGetSyncRequestTimeout(t *testing.T) {
	convey.Convey("GetSyncRequestTimeout", t, func() {
		requestTimeout := GetSyncRequestTimeout()
		convey.So(requestTimeout, convey.ShouldEqual, defaultSyncRequestTimeout)
	})
}

func TestParseDefaultSyncRequestTimeout(t *testing.T) {
	convey.Convey("TestParseDefaultSyncRequestTimeout", t, func() {
		os.Setenv("SYNC_REQUEST_TIMEOUT", "0")
		requestTimeout := parseDefaultSyncRequestTimeout()
		convey.So(requestTimeout, convey.ShouldEqual, defaultSyncRequestTimeout)
		os.Setenv("SYNC_REQUEST_TIMEOUT", "60")
		requestTimeout = parseDefaultSyncRequestTimeout()
		convey.So(requestTimeout, convey.ShouldEqual, 60)
		os.Unsetenv("SYNC_REQUEST_TIMEOUT")
	})
}

func TestGetSchedulerClient(t *testing.T) {
	convey.Convey("TestGetSchedulerClient", t, func() {
		gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
			return &types.Config{
				HTTPSConfig: &tls.InternalHTTPSConfig{
					HTTPSEnable: true,
				},
			}
		})
		client := GetSchedulerClient()
		convey.So(client.ReadTimeout, convey.ShouldEqual, readTimeout)
	})

}

func TestNewSchedulerClientWithNilConfig(t *testing.T) {
	patch := gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
		return nil
	})
	defer patch.Reset()

	assert.NotNil(t, newSchedulerClient())
}

func TestReadLimitedBody_Error(t *testing.T) {
	input := strings.NewReader("test input")
	maxReadSize := int64(5)

	patches := gomonkey.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
		return nil, fmt.Errorf("mocked read error")
	})
	defer patches.Reset()

	_, err := ReadLimitedBody(input, maxReadSize)

	assert.NotNil(t, err)
	assert.Equal(t, "read body from gin request error", err.Error())
}

func TestHandleCreateInstanceError(t *testing.T) {
	setCode := 0
	defer gomonkey.ApplyFunc(responsehandler.SetErrorInContext, func(ctx *types.InvokeProcessContext, innerCode int,
		message interface{}) {
		setCode = innerCode
	}).Reset()
	convey.Convey("TestHandleCreateInstanceError", t, func() {
		ctx := &types.InvokeProcessContext{}
		snErr := snerror.New(1234, "some error")
		flag := HandleCreateInstanceError(ctx, snErr)
		convey.So(flag, convey.ShouldBeTrue)
		convey.So(setCode, convey.ShouldEqual, 1234)

		err := errors.New("some error")
		flag = HandleCreateInstanceError(ctx, err)
		convey.So(flag, convey.ShouldBeTrue)
		convey.So(setCode, convey.ShouldEqual, statuscode.InternalErrorCode)

		err = errors.New(`{"errorCode": "invalid"}`)
		flag = HandleCreateInstanceError(ctx, err)
		convey.So(flag, convey.ShouldBeTrue)
		convey.So(setCode, convey.ShouldEqual, statuscode.InternalErrorCode)
	})
}

func TestAddAuthorizationHeaderForFG(t *testing.T) {
	defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
		return &types.Config{
			LocalAuth: &localauth.AuthConfig{},
		}
	}).Reset()
	convey.Convey("TestAddAuthorizationHeaderForFG", t, func() {
		request := &fasthttp.Request{}
		AddAuthorizationHeaderForFG(request)
		value := request.Header.Peek(constant.HeaderAuthTimestamp)
		convey.So(value, convey.ShouldNotBeEmpty)
		value = request.Header.Peek(constant.HeaderAuthorization)
		convey.So(value, convey.ShouldNotBeEmpty)
	})
}

func TestGetCompatibleHeader(t *testing.T) {
	convey.Convey("TestGetCompatibleHeader", t, func() {
		header := map[string]string{"primary": "aaa", "secondary": "bbb"}
		value := GetCompatibleHeader(header, "primary", "secondary")
		convey.So(value, convey.ShouldEqual, "aaa")

		header["primary"] = ""
		value = GetCompatibleHeader(header, "primary", "secondary")
		convey.So(value, convey.ShouldEqual, "bbb")
	})
}

func TestGetCompatibleGinHeader(t *testing.T) {
	convey.Convey("TestGetCompatibleGinHeader", t, func() {
		request := &http.Request{Header: map[string][]string{"Primary": {"aaa"}, "Secondary": {"bbb"}}}
		value := GetCompatibleGinHeader(request, "primary", "secondary")
		convey.So(value, convey.ShouldEqual, "aaa")

		request.Header["Primary"] = []string{}
		value = GetCompatibleGinHeader(request, "primary", "secondary")
		convey.So(value, convey.ShouldEqual, "bbb")
	})
}
