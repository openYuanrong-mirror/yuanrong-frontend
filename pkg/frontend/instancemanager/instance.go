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

// Package instancemanager -
package instancemanager

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/resspeckey"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/subscriber"
)

var gInstanceScheduler = &FunctionInstancesMap{
	lock:         sync.RWMutex{},
	instancesMap: make(map[string]*functionInstanceMap),
}

var instanceUpdates = struct {
	sync.Mutex
	ch chan struct{}
}{ch: make(chan struct{})}

const routeOnlyInstanceTTL = 10 * time.Minute

type routeOnlyInstance struct {
	instance *types.InstanceSpecification
	timer    *time.Timer
}

var routeOnlyInstances sync.Map // key: instanceID, value: *routeOnlyInstance

// evictingInstances only retains owner-routing metadata for instances that are
// in graceful eviction. These instances must not return to the ordinary
// scheduler, but ForceInvoke requests still need their owner route.
var evictingInstances sync.Map // key: instanceID, value: *types.InstanceSpecification

// RouteOnlyInstanceSnapshot is a payload-free, read-only view of the minimal
// route hint maintained by the frontend-proxy path. It is intentionally not an
// authentication or full instance-lifecycle record.
type RouteOnlyInstanceSnapshot struct {
	InstanceID      string `json:"instanceID"`
	Function        string `json:"function,omitempty"`
	FunctionProxyID string `json:"functionProxyID,omitempty"`
	Present         bool   `json:"present"`
}

// RouteOnlyInstanceChange captures the observable before/after state of a
// normal route-hint removal without exposing request payloads.
type RouteOnlyInstanceChange struct {
	Before RouteOnlyInstanceSnapshot `json:"before"`
	After  RouteOnlyInstanceSnapshot `json:"after"`
}

var subject = subscriber.NewSubject()

// GetInstanceSubject -
func GetInstanceSubject() *subscriber.Subject {
	return subject
}

// GetGlobalInstanceScheduler -
func GetGlobalInstanceScheduler() *FunctionInstancesMap {
	return gInstanceScheduler
}

// WaitInstanceByID waits until the etcd-watcher-backed cache contains instanceID.
func WaitInstanceByID(ctx context.Context, instanceID string) (*types.InstanceSpecification, error) {
	return waitInstanceByID(ctx, instanceID, false)
}

// WaitInstanceByIDForForceInvoke also accepts an instance in graceful
// eviction. It is intentionally separate from WaitInstanceByID so ordinary
// calls cannot route to an evicting instance.
func WaitInstanceByIDForForceInvoke(ctx context.Context, instanceID string) (*types.InstanceSpecification, error) {
	return waitInstanceByID(ctx, instanceID, true)
}

func waitInstanceByID(ctx context.Context, instanceID string,
	includeEvicting bool) (*types.InstanceSpecification, error) {
	for {
		instanceUpdates.Lock()
		if instance := getInstanceByID(instanceID, includeEvicting); instance != nil {
			instanceUpdates.Unlock()
			return instance, nil
		}
		ch := instanceUpdates.ch
		instanceUpdates.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case _, open := <-ch:
			if open {
				continue
			}
		}
	}
}

func getInstanceByID(instanceID string, includeEvicting bool) *types.InstanceSpecification {
	if instance := GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(instanceID); instance != nil {
		return instance
	}
	if includeEvicting {
		return GetEvictingInstanceByID(instanceID)
	}
	return nil
}

// GetEvictingInstanceByID returns an owner-routing record for ForceInvoke.
func GetEvictingInstanceByID(instanceID string) *types.InstanceSpecification {
	value, ok := evictingInstances.Load(instanceID)
	if !ok {
		return nil
	}
	instance, _ := value.(*types.InstanceSpecification)
	return instance
}

func recordEvictingInstance(instance *types.InstanceSpecification) {
	if instance == nil || instance.InstanceID == "" {
		return
	}
	route := &types.InstanceSpecification{
		InstanceID:      instance.InstanceID,
		Function:        instance.Function,
		FunctionProxyID: instance.FunctionProxyID,
		InstanceStatus:  instance.InstanceStatus,
	}
	evictingInstances.Store(instance.InstanceID, route)
	notifyInstanceUpdate()
}

func removeEvictingInstance(instanceID string) {
	if instanceID == "" {
		return
	}
	if _, loaded := evictingInstances.LoadAndDelete(instanceID); loaded {
		notifyInstanceUpdate()
	}
}

func notifyInstanceUpdate() {
	instanceUpdates.Lock()
	close(instanceUpdates.ch)
	instanceUpdates.ch = make(chan struct{})
	instanceUpdates.Unlock()
}

// FunctionInstancesMap -
type FunctionInstancesMap struct {
	lock         sync.RWMutex
	instancesMap map[string]*functionInstanceMap
}

type functionInstanceMap struct {
	lock           sync.RWMutex
	instanceQueues map[string]*functionInstanceQueue // key: resKey, value: *functionInstanceQueue
}

type functionInstanceQueue struct {
	lock      sync.RWMutex
	instances map[string]*types.InstanceSpecification // key: instanceId, value: *instance
}

// GetInstance -
func (g *FunctionInstancesMap) GetInstance(funcKey, resSpecKey, instanceId string) *types.InstanceSpecification {
	g.lock.RLock()
	defer g.lock.RUnlock()
	resInstancesMap, ok := g.instancesMap[funcKey]
	if !ok {
		return nil
	}
	return resInstancesMap.getInstance(resSpecKey, instanceId)
}

// GetInstanceByID returns an instance by function and instance id without
// requiring the caller to know the resource-spec queue key. It first checks the
// ordinary watcher-backed map, then route-only records written by the
// frontend-proxy path.
func (g *FunctionInstancesMap) GetInstanceByID(funcKey, instanceID string) *types.InstanceSpecification {
	if instanceID == "" {
		return nil
	}
	g.lock.RLock()
	resInstancesMap, ok := g.instancesMap[funcKey]
	g.lock.RUnlock()
	if ok {
		if instance := resInstancesMap.getInstanceByID(instanceID); instance != nil {
			return instance
		}
	}
	if value, ok := routeOnlyInstances.Load(instanceID); ok {
		entry, _ := value.(*routeOnlyInstance)
		var instance *types.InstanceSpecification
		if entry != nil {
			instance = entry.instance
		}
		if instance != nil && (funcKey == "" || instance.Function == "" || instance.Function == funcKey) {
			return instance
		}
	}
	return nil
}

// GetInstanceByIDAcrossFunctions returns an instance by id across all function
// queues. It is used by the new frontend-proxy route resolver when the request
// only carries instance id.
func (g *FunctionInstancesMap) GetInstanceByIDAcrossFunctions(instanceID string) *types.InstanceSpecification {
	if instanceID == "" {
		return nil
	}
	g.lock.RLock()
	for _, resInstancesMap := range g.instancesMap {
		if instance := resInstancesMap.getInstanceByID(instanceID); instance != nil {
			g.lock.RUnlock()
			return instance
		}
	}
	g.lock.RUnlock()
	if value, ok := routeOnlyInstances.Load(instanceID); ok {
		entry, _ := value.(*routeOnlyInstance)
		if entry != nil {
			return entry.instance
		}
	}
	return nil
}

// RecordRouteOnlyInstance stores the minimal route metadata returned by the
// frontend-proxy create path. It deliberately does not publish an instance
// watcher update because it is not a full InstanceSpecification lifecycle event.
func RecordRouteOnlyInstance(funcKey, instanceID, functionProxyAddress string) {
	recordRouteOnlyInstanceWithTTL(funcKey, instanceID, functionProxyAddress, routeOnlyInstanceTTL)
}

func recordRouteOnlyInstanceWithTTL(funcKey, instanceID, functionProxyAddress string, ttl time.Duration) {
	if instanceID == "" || functionProxyAddress == "" {
		return
	}
	entry := &routeOnlyInstance{instance: &types.InstanceSpecification{
		InstanceID: instanceID, Function: funcKey, FunctionProxyID: functionProxyAddress,
	}}
	entry.timer = time.AfterFunc(ttl, func() {
		routeOnlyInstances.CompareAndDelete(instanceID, entry)
	})
	if previous, loaded := routeOnlyInstances.Swap(instanceID, entry); loaded {
		if oldEntry, ok := previous.(*routeOnlyInstance); ok && oldEntry != nil && oldEntry.timer != nil {
			oldEntry.timer.Stop()
		}
	}
}

// SnapshotRouteOnlyInstance returns a payload-free view without mutating the
// route cache. Absence is represented explicitly with the requested instance.
func SnapshotRouteOnlyInstance(instanceID string) RouteOnlyInstanceSnapshot {
	snapshot := RouteOnlyInstanceSnapshot{InstanceID: instanceID}
	if instanceID == "" {
		return snapshot
	}
	value, ok := routeOnlyInstances.Load(instanceID)
	if !ok {
		return snapshot
	}
	entry, _ := value.(*routeOnlyInstance)
	if entry == nil || entry.instance == nil {
		return snapshot
	}
	instance := entry.instance
	snapshot.Function = instance.Function
	snapshot.FunctionProxyID = instance.FunctionProxyID
	snapshot.Present = true
	return snapshot
}

// RemoveRouteOnlyInstanceWithSnapshot removes a normal route-only hint and
// returns payload-free evidence of the state transition.
func RemoveRouteOnlyInstanceWithSnapshot(instanceID string) RouteOnlyInstanceChange {
	change := RouteOnlyInstanceChange{Before: SnapshotRouteOnlyInstance(instanceID)}
	if instanceID != "" {
		if value, loaded := routeOnlyInstances.LoadAndDelete(instanceID); loaded {
			if entry, ok := value.(*routeOnlyInstance); ok && entry != nil && entry.timer != nil {
				entry.timer.Stop()
			}
		}
	}
	change.After = SnapshotRouteOnlyInstance(instanceID)
	return change
}

// RemoveRouteOnlyInstance clears route-only metadata after frontend-proxy kill.
func RemoveRouteOnlyInstance(instanceID string) {
	_ = RemoveRouteOnlyInstanceWithSnapshot(instanceID)
}

func (g *FunctionInstancesMap) addInstance(funcKey string, instance *types.InstanceSpecification,
	logger api.FormatLogger) {
	g.lock.Lock()
	resInstancesMap, ok := g.instancesMap[funcKey]
	if !ok {
		resInstancesMap = &functionInstanceMap{
			lock:           sync.RWMutex{},
			instanceQueues: make(map[string]*functionInstanceQueue),
		}
		g.instancesMap[funcKey] = resInstancesMap
	}
	logger = logger.With(zap.Any("funcKey", funcKey))
	g.lock.Unlock()
	resInstancesMap.addInstance(instance, logger)
	notifyInstanceUpdate()
}

func (g *FunctionInstancesMap) delInstance(funcKey string, instance *types.InstanceSpecification,
	logger api.FormatLogger) {
	g.lock.Lock()
	resInstancesMap, ok := g.instancesMap[funcKey]
	logger = logger.With(zap.Any("funcKey", funcKey))
	if !ok {
		g.lock.Unlock()
		return
	}
	g.lock.Unlock()
	resInstancesMap.delInstance(instance, logger)
	notifyInstanceUpdate()
	if resInstancesMap.size() == 0 {
		g.lock.Lock()
		if resInstancesMap.size() == 0 {
			delete(g.instancesMap, funcKey)
			logger.Infof("no instances in funcKey, delete this funcKey map")
		}
		g.lock.Unlock()
	}
}

// GetRandomInstanceWithoutUnexpectedInstance -
func (g *FunctionInstancesMap) GetRandomInstanceWithoutUnexpectedInstance(
	funcKey string, resKey string, unexpectInstanceIds []string, logger api.FormatLogger) *types.InstanceSpecification {
	logger = logger.With(zap.Any("resKey", resKey))
	g.lock.RLock()
	resInstanceMap, ok := g.instancesMap[funcKey]
	g.lock.RUnlock()
	if !ok {
		logger.Errorf("the funcKey has no instance in instancesMap")
		return nil
	}
	resInstanceMap.lock.RLock()
	queue, ok := resInstanceMap.instanceQueues[resKey]
	resInstanceMap.lock.RUnlock()
	if !ok {
		logger.Errorf("the resKey has no instance in funcKey instanceMap")
		return nil
	}

	queue.lock.RLock()
	defer queue.lock.RUnlock()

	instances := make(map[string]*types.InstanceSpecification, len(queue.instances))
	for k, v := range queue.instances {
		instances[k] = v
	}
	for _, unexpectedInstanceId := range unexpectInstanceIds {
		delete(instances, unexpectedInstanceId)
	}

	for _, v := range instances {
		logger.Infof("choose instance: %s, total instance num: %d, unexpected instance num: %d", v.InstanceID,
			len(queue.instances), len(unexpectInstanceIds))
		return v
	}
	logger.Errorf("the resKey has no available instance in funcKey instanceMap, total instance num: %d, "+
		"unexpected instance num: %d", len(queue.instances), len(unexpectInstanceIds))
	return nil
}

func (f *functionInstanceMap) size() int {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return len(f.instanceQueues)
}

func (f *functionInstanceMap) getInstance(resSpecKey, instanceId string) *types.InstanceSpecification {
	f.lock.RLock()
	defer f.lock.RUnlock()
	q, ok := f.instanceQueues[resSpecKey]
	if !ok {
		return nil
	}
	return q.getInstance(instanceId)
}

func (f *functionInstanceMap) getInstanceByID(instanceID string) *types.InstanceSpecification {
	f.lock.RLock()
	defer f.lock.RUnlock()
	for _, queue := range f.instanceQueues {
		if instance := queue.getInstance(instanceID); instance != nil {
			return instance
		}
	}
	return nil
}

func (f *functionInstanceMap) addInstance(instance *types.InstanceSpecification, logger api.FormatLogger) {
	f.lock.Lock()
	resKey, err := resspeckey.GetResKeyFromStr(instance.CreateOptions[constant.ResourceSpecNote])
	if err != nil {
		f.lock.Unlock()
		logger.Warnf("add instance failed, parse resKey failed, err: %s, resKeyStr: %s", err.Error(),
			instance.CreateOptions[constant.ResourceSpecNote])
		return
	}
	logger = logger.With(zap.Any("resKey", resKey.String()))
	q, ok := f.instanceQueues[resKey.String()]
	if !ok {
		q = &functionInstanceQueue{
			lock:      sync.RWMutex{},
			instances: make(map[string]*types.InstanceSpecification),
		}
		f.instanceQueues[resKey.String()] = q
	}
	f.lock.Unlock()
	q.addInstance(instance, logger)
}

func (f *functionInstanceMap) delInstance(instance *types.InstanceSpecification, logger api.FormatLogger) {
	f.lock.Lock()
	resKey, err := resspeckey.GetResKeyFromStr(instance.CreateOptions[constant.ResourceSpecNote])
	if err != nil {
		f.lock.Unlock()
		logger.Warnf("no need delete instance, parse resKey failed, err: %s, resKeyStr: %s", err.Error(),
			instance.CreateOptions[constant.ResourceSpecNote])
		return
	}
	logger = logger.With(zap.Any("resKey", resKey))
	q, ok := f.instanceQueues[resKey.String()]
	if !ok {
		f.lock.Unlock()
		return
	}
	f.lock.Unlock()
	q.delInstance(instance, logger)
	if q.size() == 0 {
		f.lock.Lock()
		if q.size() == 0 {
			delete(f.instanceQueues, resKey.String())
			logger.Infof("no instances, delete this resKey map")
		}
		f.lock.Unlock()
	}
}

func (i *functionInstanceQueue) addInstance(instance *types.InstanceSpecification, logger api.FormatLogger) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.instances[instance.InstanceID] = instance
	subject.PublishEvent(subscriber.Update, instance)
	logger.Infof("add instance ok")
}

func (i *functionInstanceQueue) size() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return len(i.instances)
}

func (i *functionInstanceQueue) delInstance(instance *types.InstanceSpecification, logger api.FormatLogger) {
	i.lock.Lock()
	defer i.lock.Unlock()
	_, ok := i.instances[instance.InstanceID]
	if !ok {
		logger.Infof("no need delete unexist instance")
		return
	}
	delete(i.instances, instance.InstanceID)
	subject.PublishEvent(subscriber.Delete, instance)
	logger.Infof("delete instance ok")
}

func (i *functionInstanceQueue) getInstance(instanceId string) *types.InstanceSpecification {
	i.lock.RLock()
	defer i.lock.RUnlock()
	instance, ok := i.instances[instanceId]
	if !ok {
		return nil
	}
	return instance
}
