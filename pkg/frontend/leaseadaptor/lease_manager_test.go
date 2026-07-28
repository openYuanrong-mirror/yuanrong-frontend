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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/localauth"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/queue"
	"frontend/pkg/common/faas_common/snerror"
	"frontend/pkg/common/faas_common/statuscode"
	"frontend/pkg/common/faas_common/tls"
	commType "frontend/pkg/common/faas_common/types"
	"frontend/pkg/common/faas_common/utils"
	"frontend/pkg/frontend/config"
	"frontend/pkg/frontend/schedulerproxy"
	"frontend/pkg/frontend/types"
)

func TestInstanceLeasePool_releaseInstanceLease(t *testing.T) {
	convey.Convey("release instance lease test", t, func() {
		convey.Convey("baseline", func() {
			op := &commType.AcquireOption{
				DesignateInstanceID: "",
				SchedulerFuncKey:    "test-scheduler",
				SchedulerID:         "test-schedulerID",
				RequestID:           "123456789",
				TraceID:             "123456",
				ResourceSpecs:       nil,
				Timeout:             30,
				FuncSig:             "",
			}
			pool := newInstanceLeasePool("func1", op, &types.RingName{Name: constant.GreenRing})
			freeCalled := 0
			p := gomonkey.ApplyFunc((*InstanceLease).free, func(_ *InstanceLease, abnormal bool, record bool) {
				freeCalled++
			})
			defer p.Reset()
			pool.leaseMap["aaa"] = &InstanceLease{}
			pool.releaseInstanceLease("bbb", true)
			convey.So(freeCalled, convey.ShouldEqual, 0)
			pool.releaseInstanceLease("aaa", true)
			convey.So(freeCalled, convey.ShouldEqual, 1)
		})
	})
}

func TestInstanceManager_ReleaseInstanceAllocation(t *testing.T) {
	convey.Convey("release instance allocation test", t, func() {
		convey.Convey("release error", func() {
			flag := false
			im := GetInstanceManager()
			defer gomonkey.ApplyFunc((*FuncKeyLeasePools).releaseInstanceLease,
				func(_ *FuncKeyLeasePools, leaseID string, abnormal bool) {
					flag = true
				}).Reset()

			im.ReleaseInstanceAllocation(&commType.InstanceAllocationInfo{
				FuncKey:       "func1",
				FuncSig:       "",
				InstanceID:    "aaa",
				ThreadID:      "func1",
				LeaseInterval: 0,
			}, true, "")
			convey.ShouldBeFalse(flag)

			im.ReleaseInstanceAllocation(nil, true, "")
			convey.ShouldBeFalse(flag)
		})
		convey.Convey("release success", func() {
			flag := false
			im := GetInstanceManager()
			defer gomonkey.ApplyFunc((&FuncKeyLeasePools{}).releaseInstanceLease,
				func(leaseID string, abnormal bool) {
					flag = true
				}).Reset()
			im.globalFuncKeyLeasePools["func1"] = &FuncKeyLeasePools{}
			im.ReleaseInstanceAllocation(&commType.InstanceAllocationInfo{
				FuncKey:       "func1",
				FuncSig:       "",
				InstanceID:    "aaa",
				ThreadID:      "func1",
				LeaseInterval: 0,
			}, true, "")
			convey.ShouldBeTrue(flag)
		})
	})
}

func TestInstanceManager_AcquireInstance(t *testing.T) {
	convey.Convey("acquire instance test", t, func() {
		convey.Convey("acquire instance success", func() {
			im := GetInstanceManager()
			defer gomonkey.ApplyFunc((*FuncKeyLeasePools).loop,
				func(_ *FuncKeyLeasePools) {
					return
				}).Reset()
			defer gomonkey.ApplyFunc(makeAcquireOption,
				func(ctx *types.InvokeProcessContext, funcSpec *commType.FuncSpec) (
					*commType.AcquireOption, snerror.SNError) {
					return &commType.AcquireOption{
						DesignateInstanceID: "",
						SchedulerFuncKey:    "test-scheduler",
						SchedulerID:         "test-schedulerID",
						RequestID:           "123456789",
						TraceID:             "123456",
						ResourceSpecs:       map[string]int64{"cpu": 500},
						Timeout:             30,
						FuncSig:             "test-func",
						InvokeTag:           map[string]string{"tagKey": "tagValue"},
						InstanceSession: &commType.InstanceSessionConfig{
							SessionID:   "SessionID",
							SessionTTL:  1000,
							Concurrency: 1,
						},
					}, nil
				}).Reset()
			defer gomonkey.ApplyFunc((*LeasePool).acquireInstanceLease,
				func(_ *LeasePool, option *commType.AcquireOption) (*InstanceLease, snerror.SNError) {
					return &InstanceLease{
						InstanceAllocationInfo: &commType.InstanceAllocationInfo{
							FuncKey:  "test-func",
							ThreadID: "test-lease-1",
						},
						reportRecord:        &ReportRecord{},
						acquireOption:       &commType.AcquireOption{},
						claimTime:           time.Now(),
						available:           atomic.Bool{},
						stopCh:              make(chan struct{}),
						exited:              atomic.Bool{},
						beginRelease:        atomic.Bool{},
						schedulerInstanceId: "",
						reacquire:           false,
					}, nil
				}).Reset()
			info, err := im.AcquireInstance(&types.InvokeProcessContext{
				FuncKey: "test-func",
			}, &commType.FuncSpec{InstanceMetaData: commType.InstanceMetaData{MaxInstance: 5}}, log.GetLogger())
			convey.ShouldBeNil(err)
			convey.ShouldEqual(info.FuncKey, "test-func")
			convey.ShouldEqual(info.ThreadID, "test-lease-1")
		})
	})
}

func TestInstanceManager_ClearFuncLeasePools(t *testing.T) {
	convey.Convey("acquire instance test", t, func() {
		convey.Convey("acquire instance success", func() {
			im := GetInstanceManager()
			funcKey := "func-key-1"
			lp := &LeasePool{stopCh: make(chan struct{})}
			poolMap := make(map[string]*LeasePool)
			poolMap[funcKey] = lp
			im.globalFuncKeyLeasePools[funcKey] = &FuncKeyLeasePools{
				stopCh:     make(chan struct{}),
				leasePools: poolMap,
			}
			im.ClearFuncLeasePools("wrong-funcKey")
			convey.ShouldEqual(1, len(im.globalFuncKeyLeasePools))

			im.ClearFuncLeasePools(funcKey)
			convey.ShouldEqual(0, len(im.globalFuncKeyLeasePools))
		})
	})
}

func TestFuncKeyLeasePools_BatchRetainLeaseLoop(t *testing.T) {
	convey.Convey("test handleLeaseExpiredLoop", t, func() {
		convey.Convey("doBatchRetain success", func() {
			count := 0
			ch := make(chan time.Time, 1)
			defer gomonkey.ApplyFunc((*FuncKeyLeasePools).doBatchRetain,
				func(lp *FuncKeyLeasePools) {
					delete(lp.leasePools, "func1")
					count++
				}).Reset()
			defer gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{C: ch}
			}).Reset()
			defer gomonkey.ApplyMethod(reflect.TypeOf(&time.Ticker{}), "Reset", func(_ *time.Ticker,
				d time.Duration) {
				return
			}).Reset()
			funcKeyLeasePools := GetInstanceManager().newFuncKeyLeasePools("func1")
			funcKeyLeasePools.interval.Store(int64(10 * time.Millisecond))
			funcKeyLeasePools.leasePools["func1"] = &LeasePool{stopCh: make(chan struct{})}
			ch <- time.Time{}
			time.Sleep(10 * time.Millisecond)
			convey.So(count, convey.ShouldEqual, 1)
			convey.So(len(funcKeyLeasePools.leasePools), convey.ShouldEqual, 0)
			close(funcKeyLeasePools.stopCh)
		})
		convey.Convey("stop chan close", func() {
			count := 0
			ch := make(chan time.Time, 1)
			defer gomonkey.ApplyFunc((*FuncKeyLeasePools).doBatchRetain,
				func(lp *FuncKeyLeasePools) {
					delete(lp.leasePools, "func2")
					count++
				}).Reset()
			defer gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{C: ch}
			}).Reset()
			defer gomonkey.ApplyMethod(reflect.TypeOf(&time.Ticker{}), "Reset", func(_ *time.Ticker,
				d time.Duration) {
				return
			}).Reset()
			funcKeyLeasePools := GetInstanceManager().newFuncKeyLeasePools("func2")
			funcKeyLeasePools.interval.Store(int64(10 * time.Millisecond))
			funcKeyLeasePools.leasePools["func2"] = &LeasePool{stopCh: make(chan struct{})}
			close(funcKeyLeasePools.stopCh)
			time.Sleep(10 * time.Millisecond)
			convey.So(count, convey.ShouldEqual, 0)
			convey.So(len(funcKeyLeasePools.leasePools), convey.ShouldEqual, 1)
		})
		convey.Convey("pendingReqNum is not 0", func() {
			count := 0
			ch := make(chan time.Time, 1)
			ticker := &time.Ticker{C: ch}
			defer gomonkey.ApplyFunc((*FuncKeyLeasePools).doBatchRetain,
				func(lp *FuncKeyLeasePools) {
					leasePool, ok := lp.leasePools["func1"]
					if ok && leasePool.pendingNum.Load() == 0 {
						delete(lp.leasePools, "func1")
					}
					count++
				}).Reset()
			defer gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return ticker
			}).Reset()
			defer gomonkey.ApplyMethod(reflect.TypeOf(&time.Ticker{}), "Reset", func(_ *time.Ticker,
				d time.Duration) {
				return
			}).Reset()
			leasePool := &LeasePool{
				stopCh: make(chan struct{}),
			}
			funcKeyLeasePools := GetInstanceManager().newFuncKeyLeasePools("func1")
			funcKeyLeasePools.interval.Store(int64(10 * time.Millisecond))
			funcKeyLeasePools.leasePools["func1"] = leasePool
			leasePool.IncPendingReqNum()
			ch <- time.Time{}
			convey.So(len(funcKeyLeasePools.leasePools), convey.ShouldEqual, 1)
			leasePool.DecPendingReqNum()
			ch <- time.Time{}
			time.Sleep(10 * time.Millisecond)
			convey.So(len(funcKeyLeasePools.leasePools), convey.ShouldEqual, 0)
			close(funcKeyLeasePools.stopCh)
		})
	})
}

func TestNormalizeRetainInterval(t *testing.T) {
	convey.Convey("normalize retain interval", t, func() {
		convey.So(normalizeRetainInterval(-time.Millisecond), convey.ShouldEqual,
			defaultRetainLeaseTime*time.Millisecond)
		convey.So(normalizeRetainInterval(0), convey.ShouldEqual,
			defaultRetainLeaseTime*time.Millisecond)
		convey.So(normalizeRetainInterval(time.Millisecond-1), convey.ShouldEqual,
			defaultRetainLeaseTime*time.Millisecond)
		convey.So(normalizeRetainInterval(time.Millisecond), convey.ShouldEqual, time.Millisecond)
	})
}

func TestFuncKeyLeasePools_DoBatchRetain(t *testing.T) {
	convey.Convey("test handleLeaseExpiredLoop", t, func() {
		// 创建测试实例
		ilps := &FuncKeyLeasePools{
			leasePools:         make(map[string]*LeasePool),
			funcKey:            "test-func-key",
			funcSpec:           &commType.FuncSpec{},
			logger:             log.GetLogger(),
			stopCh:             make(chan struct{}),
			globalLeaseList:    make(map[string]*InstanceLease),
			leaseIdToLeasePool: make(map[string]*LeasePool),
			interval:           atomic.Int64{},
			ringName:           &types.RingName{Name: constant.GreenRing},
		}

		leaseId1 := "lease-1"
		leaseId2 := "lease-2"
		leaseId3 := "lease-3"

		lease1 := &InstanceLease{
			InstanceAllocationInfo: &commType.InstanceAllocationInfo{
				FuncKey:       "test-func-key",
				InstanceID:    "inst-1",
				ThreadID:      leaseId1,
				InstanceIP:    "192.168.1.1",
				InstancePort:  "8080",
				NodeIP:        "192.168.1.2",
				NodePort:      "9090",
				LeaseInterval: 30,
				CPU:           1,
				Memory:        1024,
				ForceInvoke:   false,
			},
			reportRecord: &ReportRecord{
				requestsCount: 100,
				totalDuration: 2000,
				maxDuration:   100,
				isAbnormal:    false,
			},
			acquireOption:       &commType.AcquireOption{},
			claimTime:           time.Now(),
			available:           atomic.Bool{},
			stopCh:              make(chan struct{}),
			exited:              atomic.Bool{},
			beginRelease:        atomic.Bool{},
			schedulerInstanceId: "scheduler-1",
			reacquire:           true,
			RWMutex:             sync.RWMutex{},
		}

		lease2 := &InstanceLease{
			InstanceAllocationInfo: &commType.InstanceAllocationInfo{
				FuncKey:       "test-func-key",
				InstanceID:    "inst-2",
				ThreadID:      leaseId2,
				InstanceIP:    "192.168.1.3",
				InstancePort:  "8081",
				NodeIP:        "192.168.1.4",
				NodePort:      "9091",
				LeaseInterval: 30,
				CPU:           1,
				Memory:        1024,
				ForceInvoke:   false,
			},
			reportRecord: &ReportRecord{
				requestsCount: 50,
				totalDuration: 1000,
				maxDuration:   50,
				isAbnormal:    false,
			},
			acquireOption:       &commType.AcquireOption{},
			claimTime:           time.Now(),
			available:           atomic.Bool{},
			stopCh:              make(chan struct{}),
			exited:              atomic.Bool{},
			beginRelease:        atomic.Bool{},
			schedulerInstanceId: "scheduler-2",
			reacquire:           false,
			RWMutex:             sync.RWMutex{},
		}

		lease3 := &InstanceLease{
			InstanceAllocationInfo: &commType.InstanceAllocationInfo{
				FuncKey:       "test-func-key",
				InstanceID:    "inst-3",
				ThreadID:      leaseId3,
				InstanceIP:    "192.168.1.5",
				InstancePort:  "8082",
				NodeIP:        "192.168.1.6",
				NodePort:      "9092",
				LeaseInterval: 0,
				CPU:           1,
				Memory:        1024,
				ForceInvoke:   false,
			},
			reportRecord: &ReportRecord{
				requestsCount: 0,
				totalDuration: 0,
				maxDuration:   0,
				isAbnormal:    false,
			},
			acquireOption:       &commType.AcquireOption{},
			claimTime:           time.Now(),
			available:           atomic.Bool{},
			stopCh:              make(chan struct{}),
			exited:              atomic.Bool{},
			beginRelease:        atomic.Bool{},
			schedulerInstanceId: "scheduler-3",
			reacquire:           false,
			RWMutex:             sync.RWMutex{},
		}

		pool := &LeasePool{
			resSpecStr: "cpu=1,memory=1024",
			session: &commType.InstanceSessionConfig{
				SessionID:   "session1",
				SessionTTL:  1000,
				Concurrency: 10,
			},
			invokeLabel: "label1",
			poolLabel:   "pool1",
		}
		ilps.leaseIdToLeasePool[leaseId1] = pool

		schedulerproxy.Proxy.Add(&schedulerproxy.SchedulerNodeInfo{InstanceInfo: &commType.InstanceInfo{
			FunctionName: "test-func-key",
			InstanceName: "scheduler-1",
			Address:      "127.0.0.1",
		}}, log.GetLogger())

		defer gomonkey.ApplyMethod(reflect.TypeOf(&schedulerproxy.ProxyManager{}),
			"GetSchedulerByInstanceId",
			func(_ *schedulerproxy.ProxyManager, instanceId string) *schedulerproxy.SchedulerNodeInfo {
				return &schedulerproxy.SchedulerNodeInfo{
					InstanceInfo: &commType.InstanceInfo{
						FunctionName: "test-func-key",
						InstanceName: "scheduler-1",
						Address:      "127.0.0.1",
					},
				}
			}).Reset()
		defer gomonkey.ApplyFunc(prepareSchedulerRequest, func(schedulerReq *fasthttp.Request, dstHost string,
			args []*api.Arg, traceID string) error {
			return nil
		}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}),
			"DoTimeout", func(c *fasthttp.Client, req *fasthttp.Request,
				resp *fasthttp.Response, timeout time.Duration) error {
				resp.SetBody([]byte(""))
				resp.SetStatusCode(200)
				return nil
			}).Reset()
		defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
			return &types.Config{
				HTTPSConfig: &tls.InternalHTTPSConfig{},
				LocalAuth:   &localauth.AuthConfig{},
			}
		}).Reset()

		convey.Convey("LeaseInterval is 0", func() {
			defer gomonkey.ApplyFunc(doBatchRetainInvoke, func(batch *BatchRetainLeaseInfos, traceId string) (
				*commType.BatchInstanceResponse, error) {
				return &commType.BatchInstanceResponse{}, nil
			}).Reset()
			ilps.globalLeaseList[leaseId3] = lease3
			ilps.doBatchRetain()
			convey.So(ilps.globalLeaseList[leaseId3], convey.ShouldBeNil)
		})

		convey.Convey("Leases exit", func() {
			defer gomonkey.ApplyFunc(doBatchRetainInvoke, func(batch *BatchRetainLeaseInfos, traceId string) (
				*commType.BatchInstanceResponse, error) {
				return &commType.BatchInstanceResponse{}, nil
			}).Reset()
			ilps.globalLeaseList[leaseId2] = lease2
			lease2.exited.Store(true)
			ilps.doBatchRetain()
			convey.So(len(ilps.globalLeaseList), convey.ShouldEqual, 0)
			convey.So(ilps.leaseIdToLeasePool[leaseId2], convey.ShouldBeNil)
			delete(ilps.globalLeaseList, leaseId2)
		})

		convey.Convey("schedulerInfo is nil", func() {
			defer gomonkey.ApplyFunc(doBatchRetainInvoke, func(batch *BatchRetainLeaseInfos, traceId string) (
				*commType.BatchInstanceResponse, error) {
				return &commType.BatchInstanceResponse{}, nil
			}).Reset()
			ilps.globalLeaseList[leaseId1] = lease1
			ilps.doBatchRetain()
			convey.So(ilps.leaseIdToLeasePool[leaseId2], convey.ShouldBeNil)
			delete(ilps.globalLeaseList, leaseId1)
		})
	})
}

func Test_ReleaseInstanceLease(t *testing.T) {
	convey.Convey("test handleLeaseExpiredLoop", t, func() {
		convey.Convey("do release success", func() {
			op := &commType.AcquireOption{
				DesignateInstanceID: "",
				SchedulerFuncKey:    "test-scheduler",
				SchedulerID:         "test-schedulerID",
				RequestID:           "123456789",
				TraceID:             "123456",
				ResourceSpecs:       nil,
				Timeout:             30,
				FuncSig:             "",
			}
			pool := newInstanceLeasePool("func1", op, &types.RingName{Name: constant.GreenRing})
			pool.leaseMap["leaseId"] = &InstanceLease{}
			freeCalled := 0
			defer gomonkey.ApplyFunc((*FuncKeyLeasePools).loop,
				func(_ *FuncKeyLeasePools) {
					return
				}).Reset()
			defer gomonkey.ApplyFunc((*InstanceLease).free, func(_ *InstanceLease, abnormal bool, record bool) {
				freeCalled++
			}).Reset()
			defer gomonkey.ApplyFunc(doBatchRetainInvoke, func(batch *BatchRetainLeaseInfos, traceId string) (
				*commType.BatchInstanceResponse, error) {
				return &commType.BatchInstanceResponse{}, nil
			}).Reset()
			defer gomonkey.ApplyFunc(prepareSchedulerRequest, func(schedulerReq *fasthttp.Request, dstHost string,
				args []*api.Arg, traceID string) error {
				return nil
			}).Reset()
			defer gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}),
				"DoTimeout", func(c *fasthttp.Client, req *fasthttp.Request,
					resp *fasthttp.Response, timeout time.Duration) error {
					resp.SetBody([]byte(""))
					resp.SetStatusCode(200)
					return nil
				}).Reset()
			defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
				return &types.Config{
					HTTPSConfig: &tls.InternalHTTPSConfig{},
					LocalAuth:   &localauth.AuthConfig{},
				}
			}).Reset()
			funcKeyLeasePools := GetInstanceManager().newFuncKeyLeasePools("funcKey")

			funcKeyLeasePools.releaseInstanceLease("leaseId", true)
			convey.So(freeCalled, convey.ShouldEqual, 0)

			funcKeyLeasePools.globalLeaseList["leaseId"] = &InstanceLease{}
			funcKeyLeasePools.releaseInstanceLease("leaseId", true)
			convey.So(freeCalled, convey.ShouldEqual, 0)

			funcKeyLeasePools.globalLeaseList["leaseId"] = &InstanceLease{}
			funcKeyLeasePools.leaseIdToLeasePool["leaseId"] = pool
			funcKeyLeasePools.releaseInstanceLease("leaseId", true)
			convey.So(freeCalled, convey.ShouldEqual, 1)
		})
	})
}

func Test_AcquireRepeatedLease(t *testing.T) {
	convey.Convey("AcquireInstanceLease", t, func() {
		op := &commType.AcquireOption{
			DesignateInstanceID: "",
			SchedulerFuncKey:    "test-scheduler",
			SchedulerID:         "test-schedulerID",
			RequestID:           "123456789",
			TraceID:             "123456",
			ResourceSpecs:       nil,
			Timeout:             30,
			FuncSig:             "test-func",
		}

		leasePool := newInstanceLeasePool("test-function", op, &types.RingName{Name: constant.GreenRing})
		schedulerproxy.Proxy.Add(&schedulerproxy.SchedulerNodeInfo{InstanceInfo: &commType.InstanceInfo{
			FunctionName: "test-scheduler",
			InstanceName: "test-schedulerID",
			Address:      "127.0.0.1",
		}}, log.GetLogger())
		resp := &commType.InstanceResponse{
			InstanceAllocationInfo: commType.InstanceAllocationInfo{ThreadID: "lease1-1",
				InstanceID: "lease1", LeaseInterval: 0},
			ErrorCode:     constant.InsReqSuccessCode,
			ErrorMessage:  "",
			SchedulerTime: 0,
		}
		body, _ := json.Marshal(resp)
		defer gomonkey.ApplyFunc(prepareSchedulerRequest, func(schedulerReq *fasthttp.Request, dstHost string,
			args []*api.Arg, traceID string) error {
			return nil
		}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}),
			"DoTimeout", func(c *fasthttp.Client, req *fasthttp.Request,
				resp *fasthttp.Response, timeout time.Duration) error {
				resp.SetBody(body)
				resp.SetStatusCode(200)
				return nil
			}).Reset()
		defer gomonkey.ApplyFunc(config.GetConfig, func() *types.Config {
			return &types.Config{
				HTTPSConfig: &tls.InternalHTTPSConfig{},
				LocalAuth:   &localauth.AuthConfig{},
			}
		}).Reset()
		defer gomonkey.ApplyFunc((*LeasePool).handleLeaseExpiredLoop,
			func(_ *LeasePool, lease *InstanceLease) {
				return
			}).Reset()
		defer gomonkey.ApplyFunc(doAcquireInvoke,
			func(option *commType.AcquireOption, ip string, funcKey string, timeout int64) (
				*commType.InstanceResponse, error) {
				return resp, nil
			}).Reset()

		convey.Convey("new lease", func() {
			lease, err := leasePool.acquireInstanceLease(op)
			convey.So(err, convey.ShouldBeNil)
			convey.So(lease.ThreadID, convey.ShouldEqual, "lease1-1")
			convey.So(len(leasePool.leaseMap), convey.ShouldEqual, 1)
			convey.So(leasePool.idleLeaseList.Len(), convey.ShouldEqual, 0)
		})

		convey.Convey("get idleLeaseList lease", func() {
			lease, err := leasePool.acquireInstanceLease(op)
			convey.So(err, convey.ShouldBeNil)
			convey.So(lease.ThreadID, convey.ShouldEqual, "lease1-1")
			lease.available.Store(true)
			leasePool.idleLeaseList.PushBack(lease)

			lease2, err := leasePool.acquireInstanceLease(op)
			convey.So(err, convey.ShouldBeNil)
			convey.So(lease2.ThreadID, convey.ShouldEqual, "lease1-1")
			convey.So(leasePool.idleLeaseList.Len(), convey.ShouldEqual, 0)
		})

		convey.Convey("lease already exist", func() {
			lease, err := leasePool.acquireInstanceLease(op)
			convey.So(err, convey.ShouldBeNil)
			leasePool.idleLeaseList.PushBack(lease)

			_, err = leasePool.acquireInstanceLease(op)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Code(), convey.ShouldEqual, constant.InsAcquireLeaseExistErrorCode)
			convey.So(err.Error(), convey.ShouldContainSubstring, "lease already exist")
		})

		convey.Convey("acquireResponseErr not nil", func() {
			convey.Convey("case 1: all scheduler unavailable", func() {
				defer gomonkey.ApplyMethodReturn(schedulerproxy.Proxy, "GetWithoutUnexpectedSchedulerInfos",
					nil, errors.New(constant.AllSchedulerUnavailableErrorMessage)).Reset()
				lease, err := leasePool.acquireInstanceLease(op)
				convey.So(lease, convey.ShouldBeNil)
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Code(), convey.ShouldEqual, statuscode.ErrAllSchedulerUnavailable)
				convey.So(err.Error(), convey.ShouldEqual, constant.AllSchedulerUnavailableErrorMessage)
			})

			convey.Convey("other errors", func() {
				defer gomonkey.ApplyMethodReturn(schedulerproxy.Proxy, "GetWithoutUnexpectedSchedulerInfos",
					nil, errors.New("test error")).Reset()
				lease, err := leasePool.acquireInstanceLease(op)
				convey.So(lease, convey.ShouldBeNil)
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Code(), convey.ShouldEqual, statuscode.ErrInnerCommunication)
				convey.So(err.Error(), convey.ShouldEqual, "test error")
			})
		})
	})
}

func TestInstanceLeasePool_handleLeaseLifeCycle(t *testing.T) {
	convey.Convey("test handleLeaseExpiredLoop", t, func() {
		convey.Convey("do release success", func() {
			lease := &InstanceLease{
				InstanceAllocationInfo: &commType.InstanceAllocationInfo{
					FuncKey:  "test-func",
					ThreadID: "test-lease-1",
				},
				reportRecord:        &ReportRecord{},
				acquireOption:       &commType.AcquireOption{},
				claimTime:           time.Now(),
				available:           atomic.Bool{},
				stopCh:              make(chan struct{}),
				exited:              atomic.Bool{},
				beginRelease:        atomic.Bool{},
				schedulerInstanceId: "",
				reacquire:           false,
			}
			lease.available.Store(false)

			pool := &LeasePool{
				funcKey:       "test-func",
				leaseMap:      make(map[string]*InstanceLease),
				idleLeaseList: queue.NewFifoQueue(nil),
				stopCh:        make(chan struct{}),
				logger:        log.GetLogger(),
			}
			pool.leaseMap[lease.ThreadID] = lease

			timerC := make(chan time.Time, 1)
			timer := time.NewTimer(100 * time.Millisecond)
			timer.C = timerC
			tickerC := make(chan time.Time, 1)
			ticker := time.NewTicker(100 * time.Millisecond)
			ticker.C = tickerC
			patch := gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			})
			defer patch.Reset()
			patch.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
				return ticker
			})
			defer patch.Reset()
			patch.ApplyFunc((*LeasePool).doReleaseInvoke, func(_ *LeasePool, funcKey string, leaseId string,
				option *commType.AcquireOption, report *InstanceReport) {
				return
			})
			defer patch.Reset()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				pool.handleLeaseExpiredLoop(lease)
			}()

			_, exists := pool.leaseMap[lease.ThreadID]
			assert.True(t, exists)

			select {
			case tickerC <- time.Now():
			default:
			}
			time.Sleep(10 * time.Millisecond)
			assert.False(t, lease.beginRelease.Load())

			// goto reset releaseTimer
			lease.available.Store(true)
			select {
			case tickerC <- time.Now():
			default:
			}
			time.Sleep(10 * time.Millisecond)
			assert.True(t, lease.beginRelease.Load())

			select {
			case timerC <- time.Now():
			default:
			}
			time.Sleep(10 * time.Millisecond)
			wg.Wait()

			_, exists = pool.leaseMap[lease.ThreadID]
			assert.False(t, exists, "lease should be removed from leaseMap after successful release")
		})

		convey.Convey("abnormal scenarios", func() {
			lease := &InstanceLease{
				InstanceAllocationInfo: &commType.InstanceAllocationInfo{
					FuncKey:  "test-func",
					ThreadID: "test-lease-2",
				},
				reportRecord:        &ReportRecord{},
				acquireOption:       &commType.AcquireOption{},
				claimTime:           time.Now(),
				available:           atomic.Bool{},
				stopCh:              make(chan struct{}),
				exited:              atomic.Bool{},
				beginRelease:        atomic.Bool{},
				schedulerInstanceId: "",
				reacquire:           false,
			}
			lease.available.Store(false)

			pool := &LeasePool{
				funcKey:       "test-func",
				leaseMap:      make(map[string]*InstanceLease),
				idleLeaseList: queue.NewFifoQueue(nil),
				stopCh:        make(chan struct{}),
				logger:        log.GetLogger(),
			}
			pool.leaseMap[lease.ThreadID] = lease

			timerC := make(chan time.Time, 1)
			timer := time.NewTimer(100 * time.Millisecond)
			timer.C = timerC
			tickerC := make(chan time.Time, 1)
			ticker := time.NewTicker(100 * time.Millisecond)
			ticker.C = tickerC
			patch := gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			})
			defer patch.Reset()
			patch.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return ticker
			})
			defer patch.Reset()
			patch.ApplyFunc((*LeasePool).doReleaseInvoke, func(_ *LeasePool, funcKey string, leaseId string,
				option *commType.AcquireOption, report *InstanceReport) {
				return
			})
			defer patch.Reset()

			go pool.handleLeaseExpiredLoop(lease)
			close(lease.stopCh)
			time.Sleep(10 * time.Millisecond)
			_, exists := pool.leaseMap[lease.ThreadID]
			assert.False(t, exists)

			pool.leaseMap[lease.ThreadID] = lease
			go pool.handleLeaseExpiredLoop(lease)
			utils.SafeCloseChannel(pool.stopCh)
			time.Sleep(10 * time.Millisecond)
			_, exists = pool.leaseMap[lease.ThreadID]
			assert.False(t, exists)

			pool.leaseMap[lease.ThreadID] = lease
			go pool.handleLeaseExpiredLoop(lease)
			close(timerC)
			time.Sleep(10 * time.Millisecond)
			_, exists = pool.leaseMap[lease.ThreadID]
			assert.False(t, exists)
		})
	})
}

func TestFuncKeyLeasePools_ProcessBatchResponse(t *testing.T) {
	convey.Convey("test handleLeaseExpiredLoop", t, func() {
		mockLeaseID1 := "lease-1"
		mockLeaseID2 := "lease-2"
		mockLeaseID3 := "lease-3"
		var ilps *FuncKeyLeasePools
		var batch *BatchRetainLeaseInfos
		var resp *commType.BatchInstanceResponse

		setup := func() {
			ilps = &FuncKeyLeasePools{
				leasePools: make(map[string]*LeasePool),
				interval:   atomic.Int64{},
				ringName:   &types.RingName{Name: constant.GreenRing},
			}
			batch = &BatchRetainLeaseInfos{
				infos: make(map[string]*BatchRetainLeaseInfo),
			}
			resp = &commType.BatchInstanceResponse{
				InstanceAllocFailed: make(map[string]commType.InstanceAllocationFailedInfo),
				LeaseInterval:       1000,
			}
		}

		op := &commType.AcquireOption{
			DesignateInstanceID: "",
			SchedulerFuncKey:    "test-scheduler",
			SchedulerID:         "test-schedulerID",
			RequestID:           "123456789",
			TraceID:             "123456",
			ResourceSpecs:       map[string]int64{"cpu": 500},
			Timeout:             30,
			FuncSig:             "test-func",
			InvokeTag:           map[string]string{"tagKey": "tagValue"},
			InstanceSession: &commType.InstanceSessionConfig{
				SessionID:   "SessionID",
				SessionTTL:  1000,
				Concurrency: 1,
			},
		}
		funcKey := "func-key-1"
		poolKey := getPoolKey(funcKey, op)

		convey.Convey("When InstanceAllocFailed has LeaseIDNotFoundCode", func() {
			setup()
			batch.infos[mockLeaseID1] = &BatchRetainLeaseInfo{
				FunctionKey: funcKey,
				PoolKey:     poolKey,
			}
			resp.InstanceAllocFailed[mockLeaseID1] = commType.InstanceAllocationFailedInfo{
				ErrorCode:    statuscode.LeaseIDNotFoundCode,
				ErrorMessage: "lease not found",
			}
			pool := newInstanceLeasePool(funcKey, op, &types.RingName{Name: constant.GreenRing})
			ilps.leasePools[poolKey] = pool
			lease := &InstanceLease{
				InstanceAllocationInfo: &commType.InstanceAllocationInfo{
					ThreadID: mockLeaseID1,
				},
				schedulerInstanceId: "",
				reacquire:           false,
			}
			pool.leaseMap[mockLeaseID1] = lease

			ilps.processBatchResponse(batch, resp)
			convey.So(ilps.interval.Load(), convey.ShouldEqual, int64(500*time.Millisecond))
			convey.So(lease.reacquire, convey.ShouldEqual, true)
			convey.So(lease.schedulerInstanceId, convey.ShouldEqual, "")
		})

		convey.Convey("When InstanceAllocFailed has AcquireNonOwnerSchedulerErrorCode", func() {
			setup()
			batch.infos[mockLeaseID2] = &BatchRetainLeaseInfo{
				FunctionKey: funcKey,
				PoolKey:     poolKey,
			}
			resp.InstanceAllocFailed[mockLeaseID2] = commType.InstanceAllocationFailedInfo{
				ErrorCode:    statuscode.AcquireNonOwnerSchedulerErrorCode,
				ErrorMessage: "non-owner scheduler",
			}

			pool := newInstanceLeasePool(funcKey, op, &types.RingName{Name: constant.GreenRing})
			ilps.leasePools[poolKey] = pool
			lease := &InstanceLease{
				InstanceAllocationInfo: &commType.InstanceAllocationInfo{
					ThreadID: mockLeaseID2,
				},
				schedulerInstanceId: "",
				reacquire:           false,
			}
			pool.leaseMap[mockLeaseID2] = lease

			ilps.processBatchResponse(batch, resp)

			convey.So(ilps.interval.Load(), convey.ShouldEqual, int64(500*time.Millisecond))
			convey.So(lease.reacquire, convey.ShouldEqual, true)
			convey.So(lease.schedulerInstanceId, convey.ShouldEqual, "non-owner scheduler")
		})

		convey.Convey("When InstanceAllocFailed has other ErrorCode", func() {
			setup()
			batch.infos[mockLeaseID3] = &BatchRetainLeaseInfo{
				FunctionKey: funcKey,
				PoolKey:     poolKey,
			}
			resp.InstanceAllocFailed[mockLeaseID3] = commType.InstanceAllocationFailedInfo{
				ErrorCode:    999,
				ErrorMessage: "unknown error",
			}

			pool := newInstanceLeasePool(funcKey, op, &types.RingName{Name: constant.GreenRing})
			ilps.leasePools[poolKey] = pool
			lease := &InstanceLease{
				InstanceAllocationInfo: &commType.InstanceAllocationInfo{
					ThreadID: mockLeaseID3,
				},
				schedulerInstanceId: "",
				reacquire:           false,
			}
			pool.leaseMap[mockLeaseID3] = lease

			ilps.processBatchResponse(batch, resp)
			convey.So(ilps.interval.Load(), convey.ShouldEqual, int64(500*time.Millisecond))
			_, exists := pool.leaseMap[mockLeaseID3]
			convey.So(exists, convey.ShouldEqual, false)
		})

		convey.Convey("When LeaseInterval is zero", func() {
			setup()
			resp.LeaseInterval = 0

			ilps.processBatchResponse(batch, resp)

			convey.So(ilps.interval.Load(), convey.ShouldEqual,
				int64(defaultRetainLeaseTime*time.Millisecond))
		})
	})
}

func TestLeasePool_removeLease(t *testing.T) {
	convey.Convey("remove lease test", t, func() {
		convey.Convey("remove lease success", func() {
			op := &commType.AcquireOption{
				DesignateInstanceID: "",
				SchedulerFuncKey:    "test-scheduler",
				SchedulerID:         "test-schedulerID",
				RequestID:           "123456789",
				TraceID:             "123456",
				ResourceSpecs:       nil,
				Timeout:             30,
				FuncSig:             "",
			}
			pool := newInstanceLeasePool("func1", op, &types.RingName{Name: constant.GreenRing})
			pool.leaseMap["aaa"] = &InstanceLease{}
			pool.removeLease("bbb")
			convey.So(1, convey.ShouldEqual, len(pool.leaseMap))
			pool.removeLease("aaa")
			convey.So(0, convey.ShouldEqual, len(pool.leaseMap))
		})
	})
}

func Test_LeaseCanReuse(t *testing.T) {
	convey.Convey("remove lease test", t, func() {
		lease := &InstanceLease{InstanceAllocationInfo: &commType.InstanceAllocationInfo{
			LeaseInterval: 0,
		}}
		convey.ShouldBeFalse(leaseCanReuse(lease))
		lease.LeaseInterval = 10
		convey.ShouldBeTrue(leaseCanReuse(lease))
	})
}

func TestLeasePool_EmptyWithInFlightCount(t *testing.T) {
	convey.Convey("test lease pool empty considering inFlightCount", t, func() {
		op := &commType.AcquireOption{}
		convey.Convey("empty lease map and no inflight", func() {
			pool := newInstanceLeasePool("func_test", op, &types.RingName{Name: constant.GreenRing})
			convey.So(pool.empty(), convey.ShouldBeTrue)
		})

		convey.Convey("empty lease map but with inflight", func() {
			pool := newInstanceLeasePool("func_test", op, &types.RingName{Name: constant.GreenRing})
			pool.inFlightCount.Add(1)
			convey.So(pool.empty(), convey.ShouldBeFalse)
		})

		convey.Convey("has lease but no inflight", func() {
			pool := newInstanceLeasePool("func_test", op, &types.RingName{Name: constant.GreenRing})
			pool.leaseMap["fake_lease"] = &InstanceLease{}
			convey.So(pool.empty(), convey.ShouldBeFalse)
		})
	})
}
