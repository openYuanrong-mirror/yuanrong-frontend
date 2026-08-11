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

// Package remoteclientlease -
package remoteclientlease

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/grpc/pb/commonargs"
	"frontend/pkg/common/faas_common/grpc/pb/lease"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/common/util"
	"frontend/pkg/frontend/config"
)

const invokeManagerTimeout = 10

// FaasManagerInfo faasManager Info
type FaasManagerInfo struct {
	funcKey        string
	instanceID     string
	InstanceStatus types.InstanceStatus
}

var (
	mtx        sync.RWMutex
	info       *FaasManagerInfo
	inUsedInfo = make(map[string]*FaasManagerInfo)
)

// UpdateFaasManager update faasManager Info
func UpdateFaasManager(event *etcd3.Event, in *types.InstanceInfo) {
	instanceInfo := &types.InstanceSpecification{}
	err := json.Unmarshal(event.Value, instanceInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal instance event, err: %s", err.Error())
		return
	}
	mtx.Lock()
	log.GetLogger().Infof(
		"Success to update faas-manager info in faas-frontend, functionName: %s, instanceName: %s",
		in.FunctionName, in.InstanceName,
	)
	if info == nil && instanceInfo.InstanceStatus.Code == int32(constant.KernelInstanceStatusRunning) {
		info = &FaasManagerInfo{
			funcKey:        in.FunctionName,
			instanceID:     in.InstanceName,
			InstanceStatus: instanceInfo.InstanceStatus,
		}
		mtx.Unlock()
		return
	}
	inUsedInfo[in.InstanceName] = &FaasManagerInfo{
		funcKey:        in.FunctionName,
		instanceID:     in.InstanceName,
		InstanceStatus: instanceInfo.InstanceStatus,
	}
	mtx.Unlock()

}

// DeleteFaasManager delete faasManager Info
func DeleteFaasManager(in *types.InstanceInfo) {
	mtx.Lock()
	defer mtx.Unlock()
	log.GetLogger().Infof(
		"Success to delete faas-manager info in faas-frontend, functionName: %s, instanceName: %s",
		in.FunctionName, in.InstanceName,
	)
	if info != nil && info.instanceID == in.InstanceName {
		info = nil
	}
	for k, v := range inUsedInfo {
		if k == in.InstanceName {
			delete(inUsedInfo, k)
			continue
		}
		if info == nil && v.InstanceStatus.Code == int32(constant.KernelInstanceStatusRunning) {
			log.GetLogger().Infof("reset faas-manager info instanceName: %s", v.instanceID)
			info = v
			delete(inUsedInfo, k)
			break
		}
	}
}

func invokeFaasManager(traceID, remoteClientID, op string) *lease.LeaseResponse {
	if config.GetConfig().LeaseBypass {
		log.GetLogger().Infof("lease bypass enabled, skip op=%s remoteClientID=%s traceID=%s",
			op, remoteClientID, traceID)
		return &lease.LeaseResponse{
			Code:    commonargs.ErrorCode_ERR_NONE,
			Message: "lease bypassed",
		}
	}
	args := []*api.Arg{
		{
			Type: api.Value,
			Data: []byte(op),
		},
		{
			Type: api.Value,
			Data: []byte(remoteClientID),
		},
		{
			Type: api.Value,
			Data: []byte(traceID),
		},
	}
	resp := &lease.LeaseResponse{
		Code:    commonargs.ErrorCode_ERR_NONE,
		Message: "success create lease",
	}
	mtx.RLock()
	if info == nil {
		err := setEventToEtcd(remoteClientID, op, traceID)
		if err != nil {
			log.GetLogger().Errorf("failed to invoke faasmanager and failed write to etcd,"+
				" FaasManagerInfo is empty, traceID: %s, err: %v", traceID, err)
			resp.Code = commonargs.ErrorCode_ERR_ETCD_OPERATION_ERROR
			resp.Message = "failed to invoke faasmanager and etcd save failed, info is empty"
		}
		mtx.RUnlock()
		return resp
	}
	log.GetLogger().Infof("Start to send request to faas-manager, traceID: %s", traceID)
	msg := util.InvokeRequest{
		Function:      info.funcKey,
		Args:          args,
		InstanceID:    info.instanceID,
		TraceID:       traceID,
		InvokeTimeout: invokeManagerTimeout,
	}
	mtx.RUnlock()
	if config.GetConfig().RetryConfig != nil && config.GetConfig().RetryConfig.InstanceExceptionRetry {
		msg.RetryTimes = config.GetConfig().InvokeMaxRetryTimes
	}
	respData, err := util.NewLeaseSchedulerClient().Invoke(msg)
	if err != nil {
		log.GetLogger().Errorf("failed to send request, err: %s, traceID: %s", err.Error(), traceID)
		err = setEventToEtcd(remoteClientID, op, traceID)
		if err != nil {
			resp.Code = commonargs.ErrorCode_ERR_ETCD_OPERATION_ERROR
			resp.Message = "failed to create new lease, err: " + err.Error()
		}
		return resp
	}
	respMsg := &types.CallHandlerResponse{}
	if err = json.Unmarshal(respData, respMsg); err != nil {
		log.GetLogger().Errorf("failed to unmarshal resp, err: %s, traceID: %s", err.Error(), traceID)
		resp.Code = commonargs.ErrorCode_ERR_INNER_SYSTEM_ERROR
		resp.Message = "failed to unmarshal resp, err: " + err.Error()
		return resp
	}
	if respMsg.Code != constant.InsReqSuccessCode {
		resp.Code = commonargs.ErrorCode_ERR_INNER_SYSTEM_ERROR
		resp.Message = fmt.Sprintf("code: %d, message: %s", respMsg.Code, respMsg.Message)
	}
	return resp
}

// NewLease the handler of new lease
func NewLease(remoteClientID string, traceID string) *lease.LeaseResponse {
	resp := invokeFaasManager(traceID, remoteClientID, constant.NewLease)
	return resp
}

// KeepAlive the handler of KeepAlive
func KeepAlive(remoteClientID string, traceID string) *lease.LeaseResponse {
	resp := invokeFaasManager(traceID, remoteClientID, constant.KeepAlive)
	return resp
}

// DelLease the handler of DelLease
func DelLease(remoteClientID string, traceID string) *lease.LeaseResponse {
	resp := invokeFaasManager(traceID, remoteClientID, constant.DelLease)
	return resp
}

func setEventToEtcd(remoteClientID string, op string, traceID string) error {
	client := etcd3.GetRouterEtcdClient()
	key := constant.LeasePrefix + "/" + remoteClientID
	event := types.LeaseEvent{
		Type:           op,
		RemoteClientID: remoteClientID,
		Timestamp:      time.Now().Unix(),
		TraceID:        traceID,
	}
	marshal, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), etcd3.DurationContextTimeout)
	defer cancel()
	_, err = client.Client.Put(ctx, key, string(marshal))
	if err != nil {
		return err
	}
	return nil
}
