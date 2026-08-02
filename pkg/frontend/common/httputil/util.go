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

// Package httputil -
package httputil

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/valyala/fasthttp"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/localauth"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/snerror"
	"frontend/pkg/common/faas_common/statuscode"
	commontls "frontend/pkg/common/faas_common/tls"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/common/uuid"
	"frontend/pkg/frontend/common/httpconstant"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/responsehandler"
	"frontend/pkg/frontend/types"
)

const (
	createInstanceErrorMessageIndex = 2

	invokeErrorMsgFormat          = `failed to invoke, code: (\w+),msg: ([\s\S]*)$`
	invokeErrorMessageFormat      = `failed to invoke, code: (\w+), message: ([\s\S]*)$`
	invokeErrorEmptyResp          = "failed to get invoke response: invokeRsp is nil"
	invokeErrorAcquireTimeout     = "acquire instance timeout"
	invokeErrorCodeEmptyResp      = "3008"
	invokeErrorCodeAcquireTimeout = "3009"
	invokeUnknownCode             = "3010"
	invokeUnknownMsg              = "unknownMsg"
	invokeErrorCodeIndex          = 1
	invokeErrorMessageIndex       = 2
)

const (
	// MaxClientConcurrency -
	MaxClientConcurrency = 10000
	// WorkerInstanceMaxIdleConnDuration define max connction time
	WorkerInstanceMaxIdleConnDuration = 10 * time.Second
	// MaxConnsPerWorker limit the max connection
	MaxConnsPerWorker = 10240
	// HeartbeatDialTimeOut heartbeat dial timeout
	HeartbeatDialTimeOut = 2 * time.Second
	defaultWriteTimeout  = time.Duration(30) * time.Second
	// defaultGraphReadBufferSize is default readBuffer size is 6k (4k for log)
	defaultGraphReadBufferSize = 6 * 1024
	// DialTimeOut set timeout limit
	DialTimeOut = 10 * time.Second
)

const (
	// scheduler fast http client config
	readTimeout         = 600 * time.Second
	writeTimeout        = 600 * time.Second
	readBufSize         = 1 * 1024
	maxIdleConnDuration = 5 * time.Second
	dialTimeout         = 10 * time.Second
)

const defaultSyncRequestTimeout = 900

var (
	globalClient = struct {
		// common fast http client
		c *fasthttp.Client
		sync.Once
	}{}

	heartbeatClient = struct {
		// heartbeat fast http client
		hc *fasthttp.Client
		sync.Once
	}{}
	schedulerClient = struct {
		// heartbeat fast http client
		sc *fasthttp.Client
		sync.Once
	}{}
)

var tcpDialer = fasthttp.TCPDialer{Concurrency: MaxClientConcurrency}

// GetHeartbeatClient return heart beat client
func GetHeartbeatClient() *fasthttp.Client {
	heartbeatClient.Do(func() {
		heartbeatClient.hc = newFastHTTPClient(HeartbeatDialTimeOut)
	})
	return heartbeatClient.hc
}

// GetGlobalClient returns global client of fastHttp
func GetGlobalClient() *fasthttp.Client {
	globalClient.Do(func() {
		globalClient.c = newFastHTTPClient(DialTimeOut)
	})
	return globalClient.c
}

// GetSchedulerClient returns fastHttp client of scheduler
func GetSchedulerClient() *fasthttp.Client {
	schedulerClient.Do(func() {
		schedulerClient.sc = newSchedulerClient()
	})
	return schedulerClient.sc
}

func newSchedulerClient() *fasthttp.Client {
	var tlsConfig *tls.Config
	cfg := config.GetConfig()
	if cfg != nil && cfg.HTTPSConfig != nil && cfg.HTTPSConfig.HTTPSEnable {
		tlsConfig = commontls.GetClientTLSConfig()
		if tlsConfig != nil {
			tlsConfig.NextProtos = []string{"http/1.1"}
		}
	}
	return &fasthttp.Client{
		MaxIdleConnDuration: maxIdleConnDuration,
		MaxConnsPerHost:     MaxClientConcurrency,
		ReadTimeout:         readTimeout,
		WriteTimeout:        writeTimeout,
		ReadBufferSize:      readBufSize,
		TLSConfig:           tlsConfig,
		Dial: func(addr string) (net.Conn, error) {
			return tcpDialer.DialTimeout(addr, dialTimeout)
		},
	}
}

// NewFastHTTPClient create fasthttp client
func newFastHTTPClient(dialTimeout time.Duration) *fasthttp.Client {
	var tlsConfig *tls.Config
	if config.GetConfig().HTTPSConfig.HTTPSEnable {
		newCfg := *commontls.GetClientTLSConfig()
		newCfg.NextProtos = []string{"http/1.1"}
		tlsConfig = &newCfg
	}
	return &fasthttp.Client{
		MaxIdemponentCallAttempts: 1,
		MaxIdleConnDuration:       WorkerInstanceMaxIdleConnDuration,
		MaxConnsPerHost:           MaxConnsPerWorker,
		ReadTimeout:               time.Duration(config.GetConfig().HTTPConfig.WorkerInstanceReadTimeOut) * time.Second,
		WriteTimeout:              defaultWriteTimeout,
		ReadBufferSize:            defaultGraphReadBufferSize,
		TLSConfig:                 tlsConfig,
		Dial: func(addr string) (net.Conn, error) {
			return tcpDialer.DialTimeout(addr, dialTimeout)
		},
	}
}

// InitTraceID init trace ID
func InitTraceID(ctx *gin.Context) string {
	var traceID string
	if config.GetConfig().BusinessType == constant.BusinessTypeWiseCloud {
		traceID = ctx.Request.Header.Get(constant.CaaSHeaderTraceID)
	} else {
		traceID = ctx.Request.Header.Get(constant.HeaderTraceID)
		if traceID == "" {
			traceID = ctx.Request.Header.Get(constant.HeaderRequestID)
		}
	}
	switch {
	case traceID == "":
		traceID = uuid.New().String()
	case len(traceID) > constant.MaxTraceIDLength:
		traceID = traceID[:constant.MaxTraceIDLength]
	default:
	}
	ctx.Request.Header.Set(constant.HeaderTraceID, traceID)
	return traceID
}

// GetTimeFromResp -
func GetTimeFromResp(timeTaken float64) time.Duration {
	return time.Duration(timeTaken) * time.Millisecond
}

// TranslateInvokeMsgToCallReq is used to extract the http header
// and insert it into the body for executor use.
func TranslateInvokeMsgToCallReq(ctx *types.InvokeProcessContext) ([]byte, error) {
	var err error
	req := &types.CallReq{
		Header: make(map[string]string, len(ctx.ReqHeader)),
	}
	if len(ctx.ReqBody) == 0 {
		req.Body = json.RawMessage(`{}`)
	} else {
		req.Body = ctx.ReqBody
	}
	for key, value := range ctx.ReqHeader {
		req.Header[key] = value
	}
	if config.GetConfig().BusinessType == constant.BusinessTypeWiseCloud {
		req.Header[constant.CaaSHeaderRequestID] = ctx.TraceID
	}
	if config.GetConfig().BusinessType == constant.BusinessTypeFG {
		req.Header = decryptSensitiveHeader(req.Header)
		req.Header[constant.FGHeaderRequestID] = ctx.TraceID
	}
	req.Header["X-Trace-Id"] = ctx.TraceID
	req.Path = ctx.ReqPath
	req.Method = ctx.ReqMethod
	req.Query = ctx.ReqQuery
	reqMsg, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request data: %s", err)
	}
	return reqMsg, nil
}

func decryptSensitiveHeader(header map[string]string) map[string]string {
	decryptHeaderToExtra(header, constant.FGHeaderAccessKey)
	decryptHeaderToExtra(header, constant.FGHeaderSecretKey)
	decryptHeaderToExtra(header, constant.FGHeaderAuthToken)
	decryptHeaderToExtra(header, constant.FGHeaderSecurityAccessKey)
	decryptHeaderToExtra(header, constant.FGHeaderSecuritySecretKey)
	decryptHeaderToExtra(header, constant.FGHeaderSecurityToken)
	return header
}

func decryptHeaderToExtra(header map[string]string, headerName string) {
	if header == nil {
		return
	}
	if header[headerName] != "" {
		decryptHeaderValue, err := localauth.Decrypt(header[headerName])
		if err != nil {
			log.GetLogger().Warnf("failed to decrypt %s", headerName)
			header[headerName] = ""
		} else {
			header[headerName] = string(decryptHeaderValue)
		}
		utils.ClearByteMemory(decryptHeaderValue)
	}
}

// unmarshalInitResp -
func unmarshalInitResp(message []byte) (*types.InitResp, error) {
	// There is a possibility that the kernel returns slice[Message] or Message, which will be rectified later.
	// To avoid this problem, the first and last slice identifiers are removed.
	if bytes.HasPrefix(message, []byte("[")) && bytes.HasSuffix(message, []byte("]")) {
		// trim [ from the beginning and ] from the end
		message = bytes.TrimPrefix(message, []byte("["))
		message = bytes.TrimSuffix(message, []byte("]"))
	}

	respMsg := &types.InitResp{}
	err := json.Unmarshal(message, respMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal init response data: %s", err)
	}
	return respMsg, nil
}

// HandleCreateInstanceError -
func HandleCreateInstanceError(ctx *types.InvokeProcessContext, err error) bool {
	snErr, ok := err.(snerror.SNError)
	// if error code of sn error equals to InternalErrCode then it's a create error from kernel and should be processed
	// further in logic below
	if ok && snErr.Code() != statuscode.InternalErrorCode {
		responsehandler.SetErrorInContext(ctx, snErr.Code(), fmt.Sprintf(`"%s"`, strings.ReplaceAll(snErr.Error(),
			`"`, "")))
		return true
	}
	errMessage := err.Error()
	respMsg, err := unmarshalInitResp([]byte(errMessage))
	if err != nil {
		log.GetLogger().Errorf("failed to translate response data, traceID: %s, err: %s",
			ctx.TraceID, err.Error())
		responsehandler.SetErrorInContext(ctx, statuscode.InternalErrorCode, errMessage)
		return true
	}
	errorCode, err := strconv.Atoi(respMsg.ErrorCode)
	if err != nil {
		log.GetLogger().Errorf("failed to get error code, traceID: %s, err: %s",
			ctx.TraceID, err.Error())
		responsehandler.SetErrorInContext(ctx, statuscode.InternalErrorCode, errMessage)
		return true
	}
	responsehandler.SetErrorInContext(ctx, errorCode, respMsg.Message)
	return true
}

// HandleInvokeError -
func HandleInvokeError(ctx *types.InvokeProcessContext, err error) {
	switch {
	// failed to create an instance
	case HandleCreateInstanceError(ctx, err):
		return
	default:
		responsehandler.SetErrorInContext(ctx, statuscode.InternalErrorCode, err.Error())
		return
	}
}

// JudgeRetry -
func JudgeRetry(err error, ctx *types.InvokeProcessContext) {
	var errCode int
	if errInfo, ok := err.(api.ErrorInfo); ok {
		errCode = errInfo.Code
	} else {
		submatches := getMatches(err)
		if submatches == nil || len(submatches) <= invokeErrorMessageIndex ||
			submatches[createInstanceErrorMessageIndex] == "" {
			return
		}
		errCode, err = strconv.Atoi(submatches[invokeErrorCodeIndex])
		if err != nil {
			log.GetLogger().Warnf("failed to get error code from invoke ErrMsg, err: %s", err.Error())
			return
		}
	}
	switch errCode {
	case statuscode.ErrInstanceExitedCode, statuscode.ErrRequestBetweenRuntimeBusCode, statuscode.ErrInstanceNotFound,
		statuscode.ErrRequestBetweenRuntimeFrontendCode, statuscode.ErrAcquireTimeoutCode,
		statuscode.ErrInstanceCircuitCode, statuscode.ErrInstanceEvicted:
		ctx.ShouldRetry = true
	default:
		log.GetLogger().Warnf("unable to support handle errCode: %d", errCode)
	}
}

func getMatches(err error) []string {
	re := regexp.MustCompile(invokeErrorMessageFormat)
	submatches := re.FindStringSubmatch(err.Error())
	if submatches != nil && len(submatches) > invokeErrorMessageIndex &&
		submatches[createInstanceErrorMessageIndex] != "" {
		return submatches
	}
	re = regexp.MustCompile(invokeErrorMsgFormat)
	submatches = re.FindStringSubmatch(err.Error())
	if submatches != nil && len(submatches) > invokeErrorMessageIndex &&
		submatches[createInstanceErrorMessageIndex] != "" {
		return submatches
	}
	if strings.Contains(err.Error(), invokeErrorEmptyResp) {
		return []string{"", invokeErrorCodeEmptyResp, invokeErrorEmptyResp}
	}
	if strings.Contains(err.Error(), invokeErrorAcquireTimeout) {
		return []string{"", invokeErrorCodeAcquireTimeout, invokeErrorAcquireTimeout}
	}
	return []string{"", invokeUnknownCode, invokeUnknownMsg}
}

// GetSyncRequestTimeout returns sync request timeout
func GetSyncRequestTimeout() int64 {
	return syncRequestTimeout
}

var syncRequestTimeout = parseDefaultSyncRequestTimeout()

func parseDefaultSyncRequestTimeout() int64 {
	env := os.Getenv("SYNC_REQUEST_TIMEOUT")
	if env == "" {
		return defaultSyncRequestTimeout
	}
	val, err := strconv.Atoi(env)
	if err != nil || val <= 0 {
		return defaultSyncRequestTimeout
	}
	return int64(val)
}

// AddAuthorizationHeaderForFG -
func AddAuthorizationHeaderForFG(proxyReq *fasthttp.Request) {
	authorization, timestamp := localauth.SignLocally(config.GetConfig().LocalAuth.AKey,
		config.GetConfig().LocalAuth.SKey, httpconstant.AppID, config.GetConfig().LocalAuth.Duration)
	proxyReq.Header.Set(constant.HeaderAuthTimestamp, timestamp)
	proxyReq.Header.Set(constant.HeaderAuthorization, authorization)
}

// SignForScheduler -
func SignForScheduler(req *fasthttp.Request) error {
	if config.GetConfig() == nil || config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.AccessKey == "" {
		return fmt.Errorf("sts accessKey is empty")
	}
	if config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.SecretKey == "" {
		return fmt.Errorf("sts secretKey is empty")
	}
	return localauth.SignWithHmacSha256(req, config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.AccessKey,
		config.GetConfig().RawStsConfig.SensitiveConfigs.Auth.SecretKey)
}

// ReadLimitedBody -
func ReadLimitedBody(inputStream io.Reader, maxReadSize int64) ([]byte, error) {
	reader := io.LimitReader(inputStream, maxReadSize+1)
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read body from gin request error")
	}
	if int64(len(body)) > maxReadSize {
		return nil, fmt.Errorf("body is beyond maximum: %d", maxReadSize)
	}
	return body, nil
}

// GetCompatibleHeader -
func GetCompatibleHeader(headers map[string]string, primaryHeader, secondaryHeader string) string {
	if value, ok := headers[primaryHeader]; ok && value != "" {
		return value
	}
	return headers[secondaryHeader]
}

// GetCompatibleGinHeader get invoke label from res key
func GetCompatibleGinHeader(req *http.Request, primaryHeader string, secondaryHeader string) string {
	headerValue := req.Header.Get(primaryHeader)
	if headerValue == "" {
		headerValue = req.Header.Get(secondaryHeader)
	}
	return headerValue
}
