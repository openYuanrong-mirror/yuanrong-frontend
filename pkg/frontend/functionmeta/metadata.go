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

// Package functionmeta function metadata sync with etcd
package functionmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/singleflight"
	"frontend/pkg/common/faas_common/trietree"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/urnutils"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/frontend/leaseadaptor"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/subscriber"
)

const (
	businessIndex = 4 + 2*iota
	tenantIndex
	funcNameIndex
	versionIndex
)

const (
	functionEtcdKeyLen = 11
	// set the key timeout interval in singleFlight after etcd access fails so that subsequent requests can be retried
	singleFlightKeyTTL = 10 * time.Second
)

var (
	funcSpecMap  sync.Map
	funcRouteMap sync.Map
	sf           = singleflight.NewSingleFlight()
	errNotFound  = errors.New("not found")
	trie         = trietree.NewTrie()

	// subject -
	subject = subscriber.NewSubject()

	enableAgentCRDRegistry = os.Getenv(constant.EnableAgentCRDRegistry) != ""
)

// GetFunctionMetaDataSubject -
func GetFunctionMetaDataSubject() *subscriber.Subject {
	return subject
}

type funcKeyInfo struct {
	tenantID string
	funcName string
	version  string
}

// LoadFuncSpecWithPath -
func LoadFuncSpecWithPath(path string, traceID string) (*types.FuncSpec, bool) {
	routePrefix := trie.LongestMatch(strings.Split(path, constant.URLSeparator))
	if routePrefix == "" {
		log.GetLogger().Errorf("route match failed, path: %s,traceID %s", path, traceID)
		return nil, false
	}
	value, ok := funcRouteMap.Load(routePrefix)
	if !ok {
		log.GetLogger().Errorf("function not found with path: %s,traceID %s", routePrefix, traceID)
		return nil, false
	}
	funcSpec, ok := value.(*types.FuncSpec)
	if !ok {
		return nil, false
	}
	return funcSpec, true
}

// LoadFuncSpec load funcSpec by function key
func LoadFuncSpec(funcKey string) (*types.FuncSpec, bool) {
	if funcSpec := loadFromCrSpecMap(funcKey); funcSpec != nil {
		return funcSpec, true
	}
	value, ok := funcSpecMap.Load(funcKey)
	if !ok {
		return fetchMetaEtcdWithSingleFlight(funcKey)
	}
	funcSpec, ok := value.(*types.FuncSpec)
	if !ok {
		return nil, false
	}
	return funcSpec, true
}

func loadFromCrSpecMap(funcKey string) *types.FuncSpec {
	if enableAgentCRDRegistry {
		value, ok := funcCrSpecMap.Load(funcKey)
		if !ok {
			return nil
		}
		funcSpec, ok := value.(*types.FuncSpec)
		if !ok {
			return nil
		}
		return funcSpec
	}
	return nil
}

// ProcessUpdate process update FuncSpec
func ProcessUpdate(etcdKey string, value []byte, etcdType string) error {
	functionKeyInfo, err := getFunctionKeyInfo(etcdKey)
	if err != nil {
		log.GetLogger().Errorf("get function key by key : %s type: %s with error: %s", etcdKey, etcdType, err.Error())
		return err
	}
	functionKey := urnutils.CombineFunctionKey(functionKeyInfo.tenantID,
		functionKeyInfo.funcName, functionKeyInfo.version)
	currFuncSpec, err := buildFuncSpec(functionKey, value, etcdType)
	if err != nil {
		return err
	}
	specValue, exist := funcSpecMap.Load(functionKey)
	preFuncSpec, ok := specValue.(*types.FuncSpec)
	if etcdType == etcd3.CAEMeta && exist && ok && preFuncSpec.ETCDType != etcd3.CAEMeta {
		// CAE ETCD 不能覆盖 FAAS ETCD 数据
		log.GetLogger().Infof("function from meta etcd exists, skip update from cae, key %s", functionKey)
		return nil
	}
	log.GetLogger().Infof("store new metadata: %s， etcdType %s", functionKey, etcdType)
	funcSpecMap.Store(functionKey, currFuncSpec)
	sf.Remove(functionKey)
	if currFuncSpec.FuncMetaData.BusinessType == constant.BusinessTypeServe {
		updateRoute(preFuncSpec, currFuncSpec)
	}
	subject.PublishEvent(subscriber.Update, currFuncSpec)
	return nil
}

func updateRoute(preFuncSpec *types.FuncSpec, currFuncSpec *types.FuncSpec) {
	var preRoutePrefix string
	var currRoutePrefix string
	if preFuncSpec != nil && len(preFuncSpec.ExtendedMetaData.ServeDeploySchema.Applications) != 0 {
		preRoutePrefix = preFuncSpec.ExtendedMetaData.ServeDeploySchema.
			Applications[constant.ApplicationIndex].RoutePrefix
	}
	if len(currFuncSpec.ExtendedMetaData.ServeDeploySchema.Applications) != 0 {
		currRoutePrefix = currFuncSpec.ExtendedMetaData.ServeDeploySchema.
			Applications[constant.ApplicationIndex].RoutePrefix
	}
	if preRoutePrefix != currRoutePrefix {
		trie.Delete(strings.Split(preRoutePrefix, constant.URLSeparator))
		funcRouteMap.Delete(preRoutePrefix)
	}
	funcRouteMap.Store(currRoutePrefix, currFuncSpec)
	trie.Insert(strings.Split(currRoutePrefix, constant.URLSeparator))
}

func buildFuncSpec(functionKey string, value []byte, etcdType string) (*types.FuncSpec, error) {
	funcMeta := &types.FunctionMetaInfo{}
	if err := json.Unmarshal(value, funcMeta); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the etcd event value, etcdType: %s", etcdType)
		return &types.FuncSpec{}, err
	}
	utils.SetFuncMetaDynamicConfEnable(funcMeta)
	funcSpec := &types.FuncSpec{
		ETCDType:          etcdType,
		FunctionKey:       functionKey,
		FuncMetaSignature: utils.GetFuncMetaSignature(funcMeta, false),
		FuncMetaData:      funcMeta.FuncMetaData,
		S3MetaData:        funcMeta.S3MetaData,
		CodeMetaData:      funcMeta.CodeMetaData,
		EnvMetaData:       funcMeta.EnvMetaData,
		StsMetaData:       funcMeta.StsMetaData,
		ResourceMetaData:  funcMeta.ResourceMetaData,
		InstanceMetaData:  funcMeta.InstanceMetaData,
		ExtendedMetaData:  funcMeta.ExtendedMetaData,
		RootfsSpecMeta:    funcMeta.RootfsSpecMeta,
		SandboxType:       funcMeta.SandboxType,
	}
	return funcSpec, nil
}

// ProcessDelete process delete FuncSpec
func ProcessDelete(etcdKey string, ETCDType string) error {
	functionKeyInfo, err := getFunctionKeyInfo(etcdKey)
	if err != nil {
		log.GetLogger().Errorf("get function key %s by %s type: with error: %s", etcdKey, ETCDType, err.Error())
		return err
	}
	functionKey := urnutils.CombineFunctionKey(functionKeyInfo.tenantID,
		functionKeyInfo.funcName, functionKeyInfo.version)

	specValue, exist := funcSpecMap.Load(functionKey)
	spec, ok := specValue.(*types.FuncSpec)
	if exist && ok {
		if ETCDType == etcd3.CAEMeta && spec.ETCDType != etcd3.CAEMeta {
			log.GetLogger().Infof("function from meta etcd exists, skip delete from cae, key %s", functionKey)
			return nil
		}
		funcSpecMap.Delete(functionKey)
		if spec.FuncMetaData.BusinessType == constant.BusinessTypeServe &&
			len(spec.ExtendedMetaData.ServeDeploySchema.Applications) != 0 {
			routePrefix := spec.ExtendedMetaData.ServeDeploySchema.Applications[constant.ApplicationIndex].RoutePrefix
			trie.Delete(strings.Split(routePrefix, constant.URLSeparator))
			funcRouteMap.Delete(routePrefix)
		}
		subject.PublishEvent(subscriber.Delete, spec)
	}
	sf.Remove(functionKey)
	schedulerproxy.Proxy.DeleteBalancer(functionKey)
	schedulerproxy.BlueProxy.DeleteBalancer(functionKey)
	log.GetLogger().Infof("delete function balancer :%s, type: %s", functionKey, ETCDType)
	leaseadaptor.GetInstanceManager().ClearFuncLeasePools(functionKey)
	leaseadaptor.GetBlueInstanceManager().ClearFuncLeasePools(functionKey)
	return nil
}

func getFunctionKeyInfo(etcdKey string) (funcKeyInfo, error) {
	keys := strings.Split(etcdKey, constant.ETCDEventKeySeparator)
	if len(keys) != functionEtcdKeyLen {
		return funcKeyInfo{}, errors.New("incorrect etcdKey length")
	}
	return funcKeyInfo{
		tenantID: keys[tenantIndex],
		funcName: keys[funcNameIndex],
		version:  keys[versionIndex],
	}, nil
}

func getFunctionWithVersion(funcName string, version string) string {
	name := getNoPrefixFuncName(funcName)
	if version == constant.DefaultURNVersion {
		return name
	}
	return fmt.Sprintf("%s:%s", name, version)
}

func getNoPrefixFuncName(name string) string {
	lastIndex := strings.LastIndex(name, "@")
	if lastIndex > 0 {
		return name[lastIndex+1:]
	}
	return name
}

func fetchMetaEtcdWithSingleFlight(funcKey string) (*types.FuncSpec, bool) {
	tenantID, funcName, funcVersion := utils.ParseFuncKey(funcKey)
	silentEtcdKey := fmt.Sprintf(constant.SilentFuncKey, tenantID, funcName, funcVersion)
	metaEtcdKey := fmt.Sprintf(constant.MetaFuncKey, tenantID, funcName, funcVersion)
	meta, err := sf.Do(funcKey, func() (interface{}, error) {
		metaClient := etcd3.GetMetaEtcdClient()
		if metaClient.Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			return nil, errors.New("failed to init meta ETCD client")
		}
		getRespValue, err := fetchEtcdWithKey(metaClient, silentEtcdKey, funcKey)
		if err != nil {
			if err != errNotFound {
				return nil, err
			}
			getRespValue, err = fetchEtcdWithKey(metaClient, metaEtcdKey, funcKey)
			if err != nil {
				return nil, err
			}
		}
		funcSpec, err := buildFuncSpec(funcKey, getRespValue, etcd3.Meta)
		if err != nil {
			return nil, err
		}
		log.GetLogger().Infof("fetch new metadata: %s", funcKey)
		funcSpecMap.Store(funcKey, funcSpec)
		return funcSpec, nil
	})
	if err != nil {
		return nil, false
	}
	return meta.(*types.FuncSpec), true
}

func fetchEtcdWithKey(metaClient *etcd3.EtcdClient, etcdKey string, funcKey string) ([]byte, error) {
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	getResp, err := metaClient.GetResponse(defaultEtcdCtx, etcdKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get function metadata from etcd, err: %s, key: %s",
			err.Error(), etcdKey)
		time.AfterFunc(singleFlightKeyTTL, func() {
			sf.Remove(funcKey)
		})
		return nil, err
	}
	if len(getResp.Kvs) == 0 {
		log.GetLogger().Warnf("function key is not exist,key: %s", etcdKey)
		return nil, errNotFound
	}
	return getResp.Kvs[0].Value, nil
}
