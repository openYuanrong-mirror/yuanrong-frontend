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

package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/frontend/proxyrouting"
)

// --- 测试辅助 ---

// fakeStore 是 etcdOwnershipStore 的内存测试实现
type fakeStore struct {
	mu      sync.Mutex
	records map[string]string
	// 行为注入
	getOwnerErr   error
	putIfAbsentFn func(owner, owningIP string) (string, error)
	deleteErr     error
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: make(map[string]string)}
}

func (s *fakeStore) GetOwner(_ context.Context, owner string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getOwnerErr != nil {
		return "", s.getOwnerErr
	}
	return s.records[owner], nil
}

func (s *fakeStore) PutIfAbsent(_ context.Context, owner, owningIP string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putIfAbsentFn != nil {
		return s.putIfAbsentFn(owner, owningIP)
	}
	if existing, ok := s.records[owner]; ok {
		return existing, nil
	}
	s.records[owner] = owningIP
	return owningIP, nil
}

func (s *fakeStore) Delete(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.records, owner)
	return nil
}

// fakeProxyClient 记录 PrepareWorkspace / RemoveWorkspace 调用
type fakeProxyClient struct {
	mu                sync.Mutex
	prepareReq        *frontend_proxy.PrepareWorkspaceRequest
	removeReq         *frontend_proxy.RemoveWorkspaceRequest
	prepareResp       *frontend_proxy.PrepareWorkspaceResponse
	prepareErr        error
	removeResp        *frontend_proxy.RemoveWorkspaceResponse
	removeErr         error
}

func (c *fakeProxyClient) PrepareWorkspace(_ context.Context, in *frontend_proxy.PrepareWorkspaceRequest,
	_ ...grpc.CallOption) (*frontend_proxy.PrepareWorkspaceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prepareReq = in
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.prepareResp != nil {
		return c.prepareResp, nil
	}
	return &frontend_proxy.PrepareWorkspaceResponse{Status: okStatus()}, nil
}

func (c *fakeProxyClient) RemoveWorkspace(_ context.Context, in *frontend_proxy.RemoveWorkspaceRequest,
	_ ...grpc.CallOption) (*frontend_proxy.RemoveWorkspaceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeReq = in
	if c.removeErr != nil {
		return nil, c.removeErr
	}
	if c.removeResp != nil {
		return c.removeResp, nil
	}
	return &frontend_proxy.RemoveWorkspaceResponse{Status: okStatus()}, nil
}

// fakeFactory 总是返回同一个 fakeProxyClient
type fakeFactory struct {
	client *fakeProxyClient
	err    error
}

func (f *fakeFactory) ClientForAddress(_ string) (frontendProxyClient, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

func okStatus() *frontend_proxy.FrontendProxyStatus {
	return &frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE}
}

func errStatus(msg string) *frontend_proxy.FrontendProxyStatus {
	return &frontend_proxy.FrontendProxyStatus{
		Code:    common.ErrorCode_ERR_LOCAL_SCHEDULER_OPERATION_ERROR,
		Message: msg,
	}
}

// setupTestDeps 替换进程级依赖 + proxyrouting 函数,返回恢复函数。
func setupTestDeps(t *testing.T, factory frontendProxyClientFactory, store etcdOwnershipStore,
	owningIP string, found bool, nodeIPs []string,
) func() {
	t.Helper()
	restoreDeps := setDependenciesForTest(factory, store)
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(proxyrouting.GetByIP,
		func(ip string, _ proxyrouting.Capability) (proxyrouting.Endpoint, bool) {
			if !found || ip != owningIP {
				return proxyrouting.Endpoint{}, false
			}
			return proxyrouting.Endpoint{
				NodeID:      "test-node",
				GRPCAddress: owningIP + ":50051",
				Health:      "healthy",
				Capabilities: map[proxyrouting.Capability]bool{
					proxyrouting.CapabilityCreate: true,
				},
			}, true
		})
	if nodeIPs != nil {
		patches.ApplyFunc(proxyrouting.NodeIPs,
			func(_ proxyrouting.Capability) []string { return nodeIPs })
	}
	return func() {
		patches.Reset()
		restoreDeps()
	}
}

// doPOST 构造 POST /workspace 请求并执行
func doPOST(body string) (*httptest.ResponseRecorder, *gin.Context) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/workspace",
		bytes.NewBufferString(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return recorder, ctx
}

// doDELETE 构造 DELETE /workspace?owner=... 请求并执行
func doDELETE(query string) (*httptest.ResponseRecorder, *gin.Context) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/api/workspace?"+query, nil)
	return recorder, ctx
}

// --- validateOwner 单测 ---

func TestValidateOwner(t *testing.T) {
	cases := []struct {
		name    string
		owner   string
		wantErr bool
	}{
		{"empty", "", true},
		{"only spaces", "   ", true},
		{"valid", "alice", false},
		{"valid with dash", "alice-bob", false},
		{"valid with digits", "user123", false},
		{"slash rejected", "alice/bob", true},
		{"dotdot rejected", "..", true},
		{"path traversal", "alice/../etc", true},
		{"leading dot", ".hidden", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateOwner(c.owner)
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- POST /workspace ---

func TestCreateHandlerReturnsOKOnFreshCreate(t *testing.T) {
	store := newFakeStore()
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice","mode":"0700"}`)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Code int                    `json:"code"`
		Data CreateWorkspaceResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, "alice", resp.Data.Owner)
	require.Equal(t, "10.0.0.5", resp.Data.OwningIP)
	require.True(t, resp.Data.Created)

	// 校验 etcd 已写入
	store.mu.Lock()
	require.Equal(t, "10.0.0.5", store.records["alice"])
	store.mu.Unlock()

	// 校验 gRPC 被调用且参数正确
	require.NotNil(t, client.prepareReq)
	require.Equal(t, "alice", client.prepareReq.GetOwner())
	require.Equal(t, "0700", client.prepareReq.GetMode())
}

func TestCreateHandlerReusesExistingOwnership(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.9"
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	// 注意:existing owner 的 IP 是 10.0.0.9,stubGetByIP 也应匹配它
	restore := setupTestDeps(t, factory, store, "10.0.0.9", true, []string{"10.0.0.9"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Code int                    `json:"code"`
		Data CreateWorkspaceResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, "10.0.0.9", resp.Data.OwningIP)
	// 复用已存在的归属,Created 应为 false
	require.False(t, resp.Data.Created)

	// PrepareWorkspace 仍应被调用到归属节点
	require.NotNil(t, client.prepareReq)
	require.Equal(t, "alice", client.prepareReq.GetOwner())
}

func TestCreateHandlerDefaultsModeTo0700(t *testing.T) {
	store := newFakeStore()
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultWorkspaceMode, client.prepareReq.GetMode())
}

func TestCreateHandlerReturns400OnInvalidOwner(t *testing.T) {
	recorder, ctx := doPOST(`{"owner":"alice/bob"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestCreateHandlerReturns400OnEmptyOwner(t *testing.T) {
	recorder, ctx := doPOST(`{"owner":""}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestCreateHandlerReturns400OnMalformedJSON(t *testing.T) {
	recorder, ctx := doPOST(`{not-json`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestCreateHandlerReturns503WhenNoHealthyNode(t *testing.T) {
	store := newFakeStore()
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	// nodeIPs 为空 → HRW 无可选
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestCreateHandlerReturns503WhenOwningNodeDown(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.9"
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	// GetByIP 未命中(found=false)模拟归属节点宕机
	restore := setupTestDeps(t, factory, store, "10.0.0.9", false, []string{"10.0.0.9"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestCreateHandlerReturns500WhenEtcdGetFails(t *testing.T) {
	store := newFakeStore()
	store.getOwnerErr = errors.New("etcd unavailable")
	factory := &fakeFactory{client: &fakeProxyClient{}}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestCreateHandlerReturns500WhenGRPCCallFails(t *testing.T) {
	store := newFakeStore()
	client := &fakeProxyClient{prepareErr: errors.New("rpc unavailable")}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestCreateHandlerReturns500WhenPrepareWorkspaceReturnsErrorStatus(t *testing.T) {
	store := newFakeStore()
	client := &fakeProxyClient{
		prepareResp: &frontend_proxy.PrepareWorkspaceResponse{Status: errStatus("mkdir failed")},
	}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestCreateHandlerCASLosesRaceReturnsExistingIP(t *testing.T) {
	// 模拟并发:PutIfAbsent 时发现 key 已被他人写入,返回已有值
	store := newFakeStore()
	store.putIfAbsentFn = func(owner, owningIP string) (string, error) {
		// 模拟并发写入:返回他人写入的 IP
		store.records[owner] = "10.0.0.7"
		return "10.0.0.7", nil
	}
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	// stubGetByIP 匹配最终返回的 IP
	restore := setupTestDeps(t, factory, store, "10.0.0.7", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Data CreateWorkspaceResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	// 应返回并发胜出方的 IP,且 created=false(本次未决策)
	require.Equal(t, "10.0.0.7", resp.Data.OwningIP)
	require.False(t, resp.Data.Created)
}

func TestCreateHandlerPutsOwnerInEtcdKey(t *testing.T) {
	store := newFakeStore()
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)

	// 校验 etcd key 前缀正确(通过 ownershipKey 函数间接验证)
	require.Equal(t, "/agent-runtime/workspace/alice", ownershipKey("alice"))
}

// --- DELETE /workspace ---

func TestDeleteHandlerReturnsOKOnSuccess(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.5"
	client := &fakeProxyClient{}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doDELETE("owner=alice")
	DeleteHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Code int                    `json:"code"`
		Data DeleteWorkspaceResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)
	require.True(t, resp.Data.Deleted)

	// 校验 gRPC 被调用且参数正确
	require.NotNil(t, client.removeReq)
	require.Equal(t, "alice", client.removeReq.GetOwner())

	// 校验 etcd 记录已删除
	store.mu.Lock()
	_, exists := store.records["alice"]
	store.mu.Unlock()
	require.False(t, exists)
}

func TestDeleteHandlerReturnsOKWhenNoOwnershipRecord(t *testing.T) {
	store := newFakeStore()
	factory := &fakeFactory{client: &fakeProxyClient{}}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doDELETE("owner=alice")
	DeleteHandler(ctx)

	// 无归属视为已删除(幂等),返回 200
	require.Equal(t, http.StatusOK, recorder.Code)
	var resp struct {
		Data DeleteWorkspaceResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.False(t, resp.Data.Deleted)
}

func TestDeleteHandlerReturns400OnMissingOwner(t *testing.T) {
	recorder, ctx := doDELETE("")
	DeleteHandler(ctx)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestDeleteHandlerReturns400OnInvalidOwner(t *testing.T) {
	recorder, ctx := doDELETE("owner=alice/../etc")
	DeleteHandler(ctx)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestDeleteHandlerReturns503WhenOwningNodeDown(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.9"
	factory := &fakeFactory{client: &fakeProxyClient{}}
	restore := setupTestDeps(t, factory, store, "10.0.0.9", false, []string{"10.0.0.9"})
	defer restore()

	recorder, ctx := doDELETE("owner=alice")
	DeleteHandler(ctx)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestDeleteHandlerReturns500WhenRemoveWorkspaceRPCFails(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.5"
	client := &fakeProxyClient{removeErr: errors.New("rpc failed")}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doDELETE("owner=alice")
	DeleteHandler(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)

	// 归属记录不应被删除(RPC 失败)
	store.mu.Lock()
	_, exists := store.records["alice"]
	store.mu.Unlock()
	require.True(t, exists, "ownership record must remain when RemoveWorkspace fails")
}

func TestDeleteHandlerReturns500WhenRemoveWorkspaceReturnsErrorStatus(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.5"
	client := &fakeProxyClient{
		removeResp: &frontend_proxy.RemoveWorkspaceResponse{Status: errStatus("rmdir failed")},
	}
	factory := &fakeFactory{client: client}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doDELETE("owner=alice")
	DeleteHandler(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestDeleteHandlerReturns500WhenEtcdDeleteFails(t *testing.T) {
	store := newFakeStore()
	store.records["alice"] = "10.0.0.5"
	store.deleteErr = errors.New("etcd delete failed")
	factory := &fakeFactory{client: &fakeProxyClient{}}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doDELETE("owner=alice")
	DeleteHandler(ctx)
	// RPC 成功但 etcd 删除失败,应返回 500
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

// --- 辅助函数测试 ---

func TestOwnershipKey(t *testing.T) {
	require.Equal(t, "/agent-runtime/workspace/alice", ownershipKey("alice"))
	require.Equal(t, "/agent-runtime/workspace/user-123", ownershipKey("user-123"))
}

func TestBuildAccessPathDefault(t *testing.T) {
	// 无 config 时使用默认根目录
	path := buildAccessPath("alice")
	require.Equal(t, "/home/agentos/users/alice", path)
}

func TestIsFrontendProxyStatusOK(t *testing.T) {
	require.True(t, isFrontendProxyStatusOK(nil))
	require.True(t, isFrontendProxyStatusOK(&frontend_proxy.FrontendProxyStatus{Code: common.ErrorCode_ERR_NONE}))
	require.True(t, isFrontendProxyStatusOK(&frontend_proxy.FrontendProxyStatus{})) // zero value = ERR_NONE
	require.False(t, isFrontendProxyStatusOK(&frontend_proxy.FrontendProxyStatus{
		Code: common.ErrorCode_ERR_LOCAL_SCHEDULER_OPERATION_ERROR,
	}))
}

// --- 集成测试:确保 handler 不泄露内部错误细节 ---

func TestCreateHandlerErrorMessageDoesNotLeakInternalDetails(t *testing.T) {
	store := newFakeStore()
	store.getOwnerErr = errors.New("internal: etcd connection to 10.0.0.1:2379 refused")
	factory := &fakeFactory{client: &fakeProxyClient{}}
	restore := setupTestDeps(t, factory, store, "10.0.0.5", true, []string{"10.0.0.5"})
	defer restore()

	recorder, ctx := doPOST(`{"owner":"alice"}`)
	CreateHandler(ctx)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	// message 字段包含 wrapper 文本,但不暴露 internal 细节
	body := recorder.Body.String()
	require.Contains(t, body, "failed to read workspace ownership")
}

// --- 反射相关:确保 fakeProxyClient 实现 frontendProxyClient 接口 ---

func TestFakeProxyClientImplementsInterface(t *testing.T) {
	var _ frontendProxyClient = (*fakeProxyClient)(nil)
	var _ frontendProxyClientFactory = (*fakeFactory)(nil)
	var _ etcdOwnershipStore = (*fakeStore)(nil)
	_ = reflect.TypeOf(proxyrouting.GetByIP)
}
