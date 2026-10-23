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

// Package workspace 提供 workspace 创建/删除的 HTTP handler。
//
// 方案 A:亲和归属由 frontend 在创建 workspace 时决策,固化进 etcd;
// function_proxy 在归属节点本地 mkdir / rmdir 完成实际目录操作。
// 管理面只调本接口,不感知内部实现。
package workspace

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/grpc/pb/common"
	"frontend/pkg/common/faas_common/grpc/pb/frontend_proxy"
	"frontend/pkg/common/faas_common/logger/log"
	fronttls "frontend/pkg/common/faas_common/tls"
	"frontend/pkg/frontend/common/httputil"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/proxyrouting"
)

// workspaceOwnershipKeyPrefix 是 etcd 中保存 workspace 归属记录的 key 前缀。
// 与 proxyrouting.defaultWorkspaceOwnershipKeyPrefix 保持一致;完整 key 为
// "<prefix>/<owner>"。
const workspaceOwnershipKeyPrefix = "/agent-runtime/workspace"

// defaultWorkspaceAccessRoot 是访问路径的默认根目录(对应 --workspace_access_root)。
const defaultWorkspaceAccessRoot = "/home/agentos/users"

// 默认操作超时
const (
	defaultWorkspaceEtcdTimeout      = 5 * time.Second
	defaultWorkspaceGRPCTimeout      = 30 * time.Second
	defaultWorkspaceKeepaliveTimeout = 10 * time.Second
	defaultWorkspaceMode             = "0700"
)

// CreateWorkspaceRequest 是 POST /workspace 的请求体
type CreateWorkspaceRequest struct {
	// Owner 是 workspace 身份(用户名);handler 用其拼 etcd key、定向归属节点
	Owner string `json:"owner"`
	// Mode 是目录权限位字符串,空则取默认 0700
	Mode string `json:"mode,omitempty"`
}

// CreateWorkspaceResponse 是 POST /workspace 的响应体
type CreateWorkspaceResponse struct {
	Owner      string `json:"owner"`
	OwningIP   string `json:"owning_ip"`
	Created    bool   `json:"created"`
	AccessPath string `json:"access_path,omitempty"`
}

// DeleteWorkspaceResponse 是 DELETE /workspace 的响应体
type DeleteWorkspaceResponse struct {
	Owner   string `json:"owner"`
	Deleted bool   `json:"deleted"`
}

// frontendProxyClient 是 frontend → function_proxy gRPC 调用所需的最小接口,
// 便于测试替换。
type frontendProxyClient interface {
	PrepareWorkspace(ctx context.Context, in *frontend_proxy.PrepareWorkspaceRequest,
		opts ...grpc.CallOption) (*frontend_proxy.PrepareWorkspaceResponse, error)
	RemoveWorkspace(ctx context.Context, in *frontend_proxy.RemoveWorkspaceRequest,
		opts ...grpc.CallOption) (*frontend_proxy.RemoveWorkspaceResponse, error)
}

// frontendProxyClientFactory 按 gRPC 地址获取 frontend_proxy client。
type frontendProxyClientFactory interface {
	ClientForAddress(address string) (frontendProxyClient, error)
}

// etcdOwnershipStore 抽象 etcd 上 workspace 归属记录的读写,便于测试替换。
type etcdOwnershipStore interface {
	// GetOwner 返回 owner 的归属节点 IP;无记录返回 ("", nil)。
	GetOwner(ctx context.Context, owner string) (string, error)
	// PutIfAbsent 仅当 key 不存在时写入 owningIP,返回最终归属 IP(可能已被并发写入)。
	PutIfAbsent(ctx context.Context, owner, owningIP string) (string, error)
	// Delete 删除 owner 的归属记录;key 不存在视为已删(幂等)。
	Delete(ctx context.Context, owner string) error
}

// --- 进程级默认实现 ---

// grpcClientPool 复用 gRPC 连接,按 GRPCAddress 缓存。
type grpcClientPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

var (
	grpcClientPoolOnce sync.Once
	grpcClientPoolInst *grpcClientPool
)

func getGRPCClientPool() *grpcClientPool {
	grpcClientPoolOnce.Do(func() {
		grpcClientPoolInst = &grpcClientPool{conns: make(map[string]*grpc.ClientConn)}
	})
	return grpcClientPoolInst
}

// ClientForAddress 按 GRPCAddress 复用或新建 gRPC 连接,返回 FrontendProxyService client。
func (p *grpcClientPool) ClientForAddress(address string) (frontendProxyClient, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("frontend proxy address is empty")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[address]; ok {
		return frontend_proxy.NewFrontendProxyServiceClient(conn), nil
	}
	transportCredentials, err := frontendProxyTransportCredentials()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Hour,
			Timeout: defaultWorkspaceKeepaliveTimeout,
		}),
	)
	if err != nil {
		return nil, err
	}
	p.conns[address] = conn
	return frontend_proxy.NewFrontendProxyServiceClient(conn), nil
}

// frontendProxyTransportCredentials 按 config 选择 mTLS 或明文,与 simple_runtime_proxy_client 保持一致。
func frontendProxyTransportCredentials() (credentials.TransportCredentials, error) {
	conf := config.GetConfig()
	if conf == nil || !conf.ComponentMTLSEnable {
		return insecure.NewCredentials(), nil
	}
	if conf.HTTPSConfig == nil {
		return nil, fmt.Errorf("frontend HTTPS config is required for component certificate paths")
	}
	tlsConfig, err := fronttls.NewComponentClientTLSConfig(*conf.HTTPSConfig)
	if err != nil {
		return nil, fmt.Errorf("load frontend proxy mTLS credentials: %w", err)
	}
	return credentials.NewTLS(tlsConfig), nil
}

// metaEtcdOwnershipStore 基于 metaEtcd 客户端实现 etcdOwnershipStore。
type metaEtcdOwnershipStore struct{}

func (metaEtcdOwnershipStore) client() *etcd3.EtcdClient {
	return etcd3.GetMetaEtcdClient()
}

func (s metaEtcdOwnershipStore) GetOwner(ctx context.Context, owner string) (string, error) {
	c := s.client()
	if c == nil {
		return "", fmt.Errorf("meta etcd client is not initialized")
	}
	ctxInfo := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, defaultWorkspaceEtcdTimeout)
	resp, err := c.Get(ctxInfo, ownershipKey(owner))
	if err != nil {
		return "", fmt.Errorf("failed to get workspace ownership for %q: %w", owner, err)
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// PutIfAbsent 用 txn 实现 CAS:仅当 key 不存在(mod_revision == 0)时写入 owningIP。
// 返回最终归属 IP(若被并发写入,返回当前值)。
func (s metaEtcdOwnershipStore) PutIfAbsent(ctx context.Context, owner, owningIP string) (string, error) {
	c := s.client()
	if c == nil {
		return "", fmt.Errorf("meta etcd client is not initialized")
	}
	key := ownershipKey(owner)
	ctxInfo := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, defaultWorkspaceEtcdTimeout)
	// txn: If(key 不存在) Then Put(key, owningIP) Else Get(key)
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	putOp := clientv3.OpPut(key, owningIP)
	getOp := clientv3.OpGet(key)
	resp, err := c.Client.Txn(ctxInfo.Ctx).If(cmp).Then(putOp).Else(getOp).Commit()
	if err != nil {
		return "", fmt.Errorf("failed to CAS put workspace ownership for %q: %w", owner, err)
	}
	if resp.Succeeded {
		return owningIP, nil
	}
	// 并发写入已成功,取返回的当前值
	rangeResp := resp.Responses[0].GetResponseRange()
	if rangeResp == nil || len(rangeResp.Kvs) == 0 {
		// 兜底:直接 Get
		getCtx := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, defaultWorkspaceEtcdTimeout)
		getResp, gErr := c.Get(getCtx, key)
		if gErr != nil {
			return "", fmt.Errorf("failed to read back workspace ownership for %q: %w", owner, gErr)
		}
		if getResp == nil || len(getResp.Kvs) == 0 {
			return "", fmt.Errorf("workspace ownership for %q disappeared after CAS", owner)
		}
		return string(getResp.Kvs[0].Value), nil
	}
	return string(rangeResp.Kvs[0].Value), nil
}

func (s metaEtcdOwnershipStore) Delete(ctx context.Context, owner string) error {
	c := s.client()
	if c == nil {
		return fmt.Errorf("meta etcd client is not initialized")
	}
	ctxInfo := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, defaultWorkspaceEtcdTimeout)
	if err := c.Delete(ctxInfo, ownershipKey(owner)); err != nil {
		return fmt.Errorf("failed to delete workspace ownership for %q: %w", owner, err)
	}
	return nil
}

// ownershipKey 拼接完整 etcd key:前缀 + "/" + owner
func ownershipKey(owner string) string {
	return workspaceOwnershipKeyPrefix + "/" + owner
}

// --- 进程级可替换的依赖 ---

var (
	dependencyMu      sync.RWMutex
	dependencyFactory frontendProxyClientFactory = getGRPCClientPool()
	dependencyStore   etcdOwnershipStore         = metaEtcdOwnershipStore{}
)

// setDependenciesForTest 替换进程级依赖,返回恢复函数。
func setDependenciesForTest(factory frontendProxyClientFactory, store etcdOwnershipStore) func() {
	dependencyMu.Lock()
	prevFactory := dependencyFactory
	prevStore := dependencyStore
	dependencyFactory = factory
	dependencyStore = store
	dependencyMu.Unlock()
	return func() {
		dependencyMu.Lock()
		dependencyFactory = prevFactory
		dependencyStore = prevStore
		dependencyMu.Unlock()
	}
}

func currentFactory() frontendProxyClientFactory {
	dependencyMu.RLock()
	defer dependencyMu.RUnlock()
	return dependencyFactory
}

func currentStore() etcdOwnershipStore {
	dependencyMu.RLock()
	defer dependencyMu.RUnlock()
	return dependencyStore
}

// --- 输入校验 ---

// validateOwner 校验 owner 名:非空、不含 "/"、 ".." + 前导 "." + 合法用户名风格。
func validateOwner(owner string) error {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return errors.New("owner is required")
	}
	if strings.Contains(owner, "/") {
		return fmt.Errorf("owner must not contain '/': %s", owner)
	}
	if owner == ".." || strings.Contains(owner, "..") {
		return fmt.Errorf("owner must not be a path traversal: %q", owner)
	}
	if strings.HasPrefix(owner, ".") {
		return fmt.Errorf("owner must not start with '.': %q", owner)
	}
	return nil
}

// --- HTTP handler ---

// CreateHandler 处理 POST /workspace:
// 1. 查 etcd discovery 获取节点 IP 列表
// 2. HRW 计算归属 IP
// 3. CAS 写 etcd /agent-runtime/workspace/<owner> = owning_ip
// 4. GetByIP(owning_ip, CapabilityCreate) 定向到归属节点
// 5. gRPC PrepareWorkspace(owner) 到归属节点 function_proxy
// 6. function_proxy 在本地 mkdir /home/agentos/.users/<owner> + chown + chmod
// 7. 返回 200
func CreateHandler(ctx *gin.Context) {
	traceID := httputil.InitTraceID(ctx)
	var req CreateWorkspaceRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"code": http.StatusBadRequest,
			"message": fmt.Sprintf("invalid request body: %v", err)})
		return
	}
	if err := validateOwner(req.Owner); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"code": http.StatusBadRequest, "message": err.Error()})
		return
	}
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = defaultWorkspaceMode
	}

	owner := strings.TrimSpace(req.Owner)
	log.GetLogger().Infof("[workspace.create.enter] owner=%s trace_id=%s", owner, traceID)
	start := time.Now()
	defer func() {
		log.GetLogger().Infof("[workspace.create.exit] owner=%s result=HTTP:%d cost_ms=%d",
			owner, ctx.Writer.Status(), time.Since(start).Milliseconds())
	}()

	store := currentStore()
	parentCtx := ctx.Request.Context()

	// 1. 先查 etcd:若已有归属,直接复用;否则 HRW 决策 + CAS 写入
	owningIP, err := store.GetOwner(parentCtx, owner)
	if err != nil {
		log.GetLogger().Errorf("failed to get workspace ownership owner=%s: %v", owner, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "failed to read workspace ownership"})
		return
	}
	// ownerFreshlyDecided 标记本次是否新决策归属(用于响应中的 created 字段)
	ownerFreshlyDecided := false
	if owningIP == "" {
		// 2. HRW 决策归属 IP
		ip, err := selectOwningIP(parentCtx, owner)
		if err != nil {
			log.GetLogger().Errorf("failed to select owning ip owner=%s: %v", owner, err)
			// 节点列表为空等场景视为归属节点不可用;细节只记日志,不返回给客户端
			ctx.JSON(http.StatusServiceUnavailable, gin.H{"code": http.StatusServiceUnavailable,
				"message": "no healthy workspace owning node"})
			return
		}
		// 3. CAS 写入
		finalIP, err := store.PutIfAbsent(parentCtx, owner, ip)
		if err != nil {
			log.GetLogger().Errorf("failed to CAS put workspace ownership owner=%s: %v", owner, err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
				"message": "failed to persist workspace ownership"})
			return
		}
		owningIP = finalIP
		ownerFreshlyDecided = finalIP == ip
	}

	// rollbackFreshOwnership 在本次请求新决策归属后失败时尽力回滚 etcd 记录,
	// 避免归属节点不可用时 owner 永久指向故障节点。仅删除仍指向本次写入 IP
	// 的记录,防止误删并发请求写入的记录;回滚失败仅记日志,不影响错误响应。
	rollbackFreshOwnership := func() {
		if !ownerFreshlyDecided {
			return
		}
		current, err := store.GetOwner(parentCtx, owner)
		if err != nil {
			log.GetLogger().Errorf("failed to re-read workspace ownership before rollback owner=%s: %v", owner, err)
			return
		}
		if current != owningIP {
			return
		}
		if err := store.Delete(parentCtx, owner); err != nil {
			log.GetLogger().Errorf("failed to rollback workspace ownership owner=%s ip=%s: %v", owner, owningIP, err)
			return
		}
		log.GetLogger().Infof("rolled back freshly created workspace ownership owner=%s ip=%s after create failure",
			owner, owningIP)
	}

	// 4. GetByIP 定向到归属节点
	endpoint, ok := proxyrouting.GetByIP(owningIP, proxyrouting.CapabilityCreate)
	if !ok {
		log.GetLogger().Errorf("workspace owning node not healthy owner=%s ip=%s", owner, owningIP)
		rollbackFreshOwnership()
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"code": http.StatusServiceUnavailable,
			"message": fmt.Sprintf("workspace owning node %s is unavailable", owningIP)})
		return
	}
	if !proxyrouting.IsRoutableAddress(endpoint.GRPCAddress) {
		rollbackFreshOwnership()
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"code": http.StatusServiceUnavailable,
			"message": fmt.Sprintf("workspace owning node %s has no routable address", owningIP)})
		return
	}

	// 5. gRPC PrepareWorkspace 到归属节点
	factory := currentFactory()
	client, err := factory.ClientForAddress(endpoint.GRPCAddress)
	if err != nil {
		log.GetLogger().Errorf("failed to get frontend proxy client owner=%s addr=%s: %v",
			owner, endpoint.GRPCAddress, err)
		rollbackFreshOwnership()
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "failed to connect owning node"})
		return
	}
	rpcCtx, cancel := context.WithTimeout(parentCtx, defaultWorkspaceGRPCTimeout)
	defer cancel()
	resp, err := client.PrepareWorkspace(rpcCtx, &frontend_proxy.PrepareWorkspaceRequest{
		Context: &frontend_proxy.FrontendRequestContext{
			FrontendClientID: "frontend",
			RequestID:        traceID,
			TraceID:          traceID,
		},
		Owner: owner,
		Mode:  mode,
	})
	if err != nil {
		log.GetLogger().Errorf("PrepareWorkspace RPC failed owner=%s ip=%s: %v", owner, owningIP, err)
		rollbackFreshOwnership()
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "PrepareWorkspace failed"})
		return
	}
	if resp != nil && resp.Status != nil && !isFrontendProxyStatusOK(resp.Status) {
		log.GetLogger().Errorf("PrepareWorkspace returned error owner=%s ip=%s code=%d message=%s",
			owner, owningIP, resp.Status.GetCode(), resp.Status.GetMessage())
		rollbackFreshOwnership()
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "PrepareWorkspace failed"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK,
		"data": CreateWorkspaceResponse{
			Owner:      owner,
			OwningIP:   owningIP,
			Created:    ownerFreshlyDecided,
			AccessPath: buildAccessPath(owner),
		}})
}

// DeleteHandler 处理 DELETE /workspace?owner=alice:
// 1. 查 etcd /agent-runtime/workspace/<owner> 得 owning_ip
// 2. gRPC RemoveWorkspace(owner) 到归属节点 function_proxy
// 3. function_proxy 在本地 rmdir /home/agentos/.users/<owner>
// 4. 删 etcd 记录
// 5. 返回 200
func DeleteHandler(ctx *gin.Context) {
	traceID := httputil.InitTraceID(ctx)
	owner := strings.TrimSpace(ctx.Query("owner"))
	if err := validateOwner(owner); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"code": http.StatusBadRequest, "message": err.Error()})
		return
	}

	log.GetLogger().Infof("[workspace.delete.enter] owner=%s trace_id=%s", owner, traceID)
	start := time.Now()
	defer func() {
		log.GetLogger().Infof("[workspace.delete.exit] owner=%s result=HTTP:%d cost_ms=%d",
			owner, ctx.Writer.Status(), time.Since(start).Milliseconds())
	}()

	store := currentStore()
	parentCtx := ctx.Request.Context()

	// 1. 查归属 IP
	owningIP, err := store.GetOwner(parentCtx, owner)
	if err != nil {
		log.GetLogger().Errorf("failed to get workspace ownership owner=%s: %v", owner, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "failed to read workspace ownership"})
		return
	}
	// 无归属视为已删除(幂等)
	if owningIP == "" {
		ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK,
			"data": DeleteWorkspaceResponse{Owner: owner, Deleted: false}})
		return
	}

	// 2. GetByIP 定向到归属节点
	endpoint, ok := proxyrouting.GetByIP(owningIP, proxyrouting.CapabilityCreate)
	if !ok {
		log.GetLogger().Errorf("workspace owning node not healthy owner=%s ip=%s", owner, owningIP)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"code": http.StatusServiceUnavailable,
			"message": fmt.Sprintf("workspace owning node %s is unavailable", owningIP)})
		return
	}
	if !proxyrouting.IsRoutableAddress(endpoint.GRPCAddress) {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"code": http.StatusServiceUnavailable,
			"message": fmt.Sprintf("workspace owning node %s has no routable address", owningIP)})
		return
	}

	// 3. gRPC RemoveWorkspace 到归属节点
	factory := currentFactory()
	client, err := factory.ClientForAddress(endpoint.GRPCAddress)
	if err != nil {
		log.GetLogger().Errorf("failed to get frontend proxy client owner=%s addr=%s: %v",
			owner, endpoint.GRPCAddress, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "failed to connect owning node"})
		return
	}
	rpcCtx, cancel := context.WithTimeout(parentCtx, defaultWorkspaceGRPCTimeout)
	defer cancel()
	resp, err := client.RemoveWorkspace(rpcCtx, &frontend_proxy.RemoveWorkspaceRequest{
		Context: &frontend_proxy.FrontendRequestContext{
			FrontendClientID: "frontend",
			RequestID:        traceID,
			TraceID:          traceID,
		},
		Owner: owner,
	})
	if err != nil {
		log.GetLogger().Errorf("RemoveWorkspace RPC failed owner=%s ip=%s: %v", owner, owningIP, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "RemoveWorkspace failed"})
		return
	}
	if resp != nil && resp.Status != nil && !isFrontendProxyStatusOK(resp.Status) {
		log.GetLogger().Errorf("RemoveWorkspace returned error owner=%s ip=%s code=%d message=%s",
			owner, owningIP, resp.Status.GetCode(), resp.Status.GetMessage())
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "RemoveWorkspace failed"})
		return
	}

	// 4. 删 etcd 记录
	if err := store.Delete(parentCtx, owner); err != nil {
		log.GetLogger().Errorf("failed to delete workspace ownership owner=%s: %v", owner, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"code": http.StatusInternalServerError,
			"message": "RemoveWorkspace succeeded but failed to delete ownership"})
		return
	}

	// 5. 返回 200
	ctx.JSON(http.StatusOK, gin.H{"code": http.StatusOK,
		"data": DeleteWorkspaceResponse{Owner: owner, Deleted: true}})
}

// selectOwningIP 用 AffinityPolicy 在 discovery 节点 IP 列表上为 owner 决策归属 IP。
func selectOwningIP(ctx context.Context, owner string) (string, error) {
	nodeIPs := proxyrouting.NodeIPs(proxyrouting.CapabilityCreate)
	if len(nodeIPs) == 0 {
		return "", errors.New("no healthy workspace node IP available from discovery")
	}
	policy := proxyrouting.DefaultAffinityPolicy()
	return policy.Select(owner, nodeIPs)
}

// buildAccessPath 拼接访问路径 = WorkspaceAccessRoot/owner
func buildAccessPath(owner string) string {
	root := ""
	if conf := config.GetConfig(); conf != nil {
		root = strings.TrimSpace(conf.WorkspaceAccessRoot)
	}
	if root == "" {
		root = defaultWorkspaceAccessRoot
	}
	return strings.TrimRight(root, "/") + "/" + owner
}

// isFrontendProxyStatusOK 判断 FrontendProxyStatus 是否成功。
// 约定:code 字段为数字枚举,0 = ERR_NONE。nil 也视为成功(向后兼容)。
func isFrontendProxyStatusOK(s *frontend_proxy.FrontendProxyStatus) bool {
	if s == nil {
		return true
	}
	return s.GetCode() == common.ErrorCode_ERR_NONE
}
