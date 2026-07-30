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

package schedulerproxy

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"frontend/pkg/common/faas_common/loadbalance"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/types"
)

func mockSchedulerNodeInfo(instanceName, instanceId string, updateTime time.Time) *SchedulerNodeInfo {
	return &SchedulerNodeInfo{
		InstanceInfo: &types.InstanceInfo{
			InstanceName: instanceName,
			InstanceID:   instanceId,
		},
		UpdateTime: updateTime,
	}
}

func Test_schedulerProxy_Add(t *testing.T) {
	convey.Convey("Add", t, func() {
		Proxy.Add(mockSchedulerNodeInfo("instance1", "", time.Now()), log.GetLogger())
		_, ok := Proxy.faasSchedulers.Load("instance1")
		convey.So(ok, convey.ShouldEqual, true)

		convey.So(Proxy.Exist("instance1", ""), convey.ShouldBeTrue)
		convey.So(Proxy.ExistInstanceName("instance1"), convey.ShouldBeTrue)
		Proxy.Add(mockSchedulerNodeInfo("instance1", "1", time.Now()), log.GetLogger())
		convey.So(Proxy.Exist("instance1", "1"), convey.ShouldBeTrue)
		convey.So(Proxy.ExistInstanceName("instance1"), convey.ShouldBeTrue)
		convey.So(Proxy.Exist("instance1", ""), convey.ShouldBeFalse)
	})
}

func Test_schedulerProxy_Remove(t *testing.T) {
	convey.Convey("Remove", t, func() {
		Proxy.Add(mockSchedulerNodeInfo("instance1", "", time.Now()), log.GetLogger())
		_, ok := Proxy.faasSchedulers.Load("instance1")
		convey.So(ok, convey.ShouldEqual, true)

		Proxy.Remove(&types.InstanceInfo{InstanceName: "instance1"}, log.GetLogger())
		_, ok = Proxy.faasSchedulers.Load("instance1")
		convey.So(ok, convey.ShouldEqual, false)
		convey.So(Proxy.ExistInstanceName("instance1"), convey.ShouldBeFalse)
	})
}

func Test_schedulerProxy_Get(t *testing.T) {
	convey.Convey("Get", t, func() {
		convey.Convey("assert failed", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&loadbalance.SimpleCHGeneric{}), "Next",
					func(_ *loadbalance.SimpleCHGeneric, name string, move bool) interface{} {
						return 123
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()

			_, err := Proxy.Get("functionKey", log.GetLogger())
			convey.So(err, convey.ShouldBeError)
		})

		convey.Convey("no avaiable faas scheduler was found", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&loadbalance.SimpleCHGeneric{}), "Next",
					func(_ *loadbalance.SimpleCHGeneric, name string, move bool) interface{} {
						return ""
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()

			_, err := Proxy.Get("functionKey", log.GetLogger())
			convey.So(err, convey.ShouldBeError)
		})

		convey.Convey("failed to get the faas scheduler named", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&loadbalance.SimpleCHGeneric{}), "Next",
					func(_ *loadbalance.SimpleCHGeneric, name string, move bool) interface{} {
						return "faaSScheduler"
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()

			_, err := Proxy.Get("functionKey", log.GetLogger())
			convey.So(err, convey.ShouldBeError)
		})

		convey.Convey("success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&loadbalance.SimpleCHGeneric{}), "Next",
					func(_ *loadbalance.SimpleCHGeneric, name string, move bool) interface{} {
						return "instance1"
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			Proxy.Add(mockSchedulerNodeInfo("instance1", "", time.Now()), log.GetLogger())
			scheduler, err := Proxy.Get("functionKey", log.GetLogger())
			convey.So(err, convey.ShouldBeNil)
			convey.So(scheduler.InstanceInfo.InstanceName, convey.ShouldEqual, "instance1")
		})
	})
}

func Test_schedulerProxy_GetSchedulerByInstanceName(t *testing.T) {
	convey.Convey("GetSchedulerByInstanceName", t, func() {
		Proxy.Add(mockSchedulerNodeInfo("name1", "id1", time.Now()), log.GetLogger())
		getScheduler, err := Proxy.GetSchedulerByInstanceName("name1", "")
		convey.So(err, convey.ShouldBeNil)
		convey.So(getScheduler.InstanceInfo.InstanceID, convey.ShouldEqual, "id1")

		getScheduler, err = Proxy.GetSchedulerByInstanceName("name2", "")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_schedulerProxy_GetWithoutUnexpectedSchedulerInfos(t *testing.T) {
	convey.Convey("GetWithoutUnexpectedSchedulerInfos", t, func() {
		scheduler1 := mockSchedulerNodeInfo("instance1", "isntance1", time.Now())
		scheduler2 := mockSchedulerNodeInfo("instance2", "isntance2", time.Now())
		scheduler3 := mockSchedulerNodeInfo("instance3", "isntance3", time.Now())
		scheduler4 := mockSchedulerNodeInfo("instance4", "isntance4", time.Now())
		scheduler5 := mockSchedulerNodeInfo("instance5", "isntance5", time.Now())
		proxy := newSchedulerProxy(loadbalance.NewSimpleCHGeneric())
		proxy.Add(scheduler1, log.GetLogger())
		proxy.Add(scheduler2, log.GetLogger())
		proxy.Add(scheduler3, log.GetLogger())
		proxy.Add(scheduler4, log.GetLogger())
		proxy.Add(scheduler5, log.GetLogger())

		tests := []*struct {
			unexpectedSchedulers         []*SchedulerNodeInfo
			expectSchedulerInstanceNames []string
		}{
			{
				unexpectedSchedulers: []*SchedulerNodeInfo{
					scheduler1,
					scheduler2,
					scheduler4,
				},
				expectSchedulerInstanceNames: []string{
					scheduler3.InstanceInfo.InstanceName,
					scheduler5.InstanceInfo.InstanceName,
				},
			}, {
				unexpectedSchedulers: []*SchedulerNodeInfo{
					mockSchedulerNodeInfo(
						scheduler1.InstanceInfo.InstanceName,
						scheduler1.InstanceInfo.InstanceID,
						scheduler1.UpdateTime.Add(-time.Second)),
					scheduler2,
					mockSchedulerNodeInfo(
						scheduler4.InstanceInfo.InstanceName,
						scheduler4.InstanceInfo.InstanceID,
						scheduler4.UpdateTime.Add(time.Second)),
				},
				expectSchedulerInstanceNames: []string{
					scheduler1.InstanceInfo.InstanceName,
					scheduler3.InstanceInfo.InstanceName,
					scheduler5.InstanceInfo.InstanceName,
				},
			},
		}

		for i, tt := range tests {
			log.GetLogger().Infof("tt: %d", i)
			for i := 0; i < 1000; i++ {
				v, err := proxy.GetWithoutUnexpectedSchedulerInfos(strconv.Itoa(i), tt.unexpectedSchedulers, log.GetLogger())
				convey.So(err == nil, convey.ShouldBeTrue)
				convey.So(tt.expectSchedulerInstanceNames, convey.ShouldContain, v.InstanceInfo.InstanceName)
			}
		}
	})
}
