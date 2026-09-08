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

package watcher

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"frontend/pkg/common/faas_common/etcd3"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/metrics"
)

func Test_handler(t *testing.T) {
	instanceEtcdInfoBytes, _ := json.Marshal(&types.InstanceSpecification{InstanceStatus: types.InstanceStatus{}})
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name string
		args args
	}{
		{"case1 event put", args{event: &etcd3.Event{
			Type:  etcd3.PUT,
			Value: instanceEtcdInfoBytes,
		}}},
		{"case2 event delete", args{event: &etcd3.Event{
			Type: etcd3.DELETE,
			Key:  "/sn/instance/business/yrk/tenant/12/function/0-system-faasscheduler/version/$latest/defaultaz/requestID/123",
		}}},
		{"case3 event error", args{event: &etcd3.Event{
			Type: etcd3.ERROR,
		}}},
		{"case4 event default", args{event: &etcd3.Event{
			Type: etcd3.SYNCED,
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instanceInfoHandler(tt.args.event)
		})
	}
}

func Test_InstanceInfoFilter(t *testing.T) {
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"case1", args{event: &etcd3.Event{
			Type: etcd3.PUT,
			Key:  "/sn/instance/business/yrk/tenant/12/function/0-system-faasscheduler/version/$latest/defaultaz/requestID/123",
		}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := instanceInfoFilter(tt.args.event); got != tt.want {
				t.Errorf("filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInstanceWatchEventLabel(t *testing.T) {
	tests := []struct {
		name      string
		eventType int
		want      string
	}{
		{name: "put", eventType: etcd3.PUT, want: "put"},
		{name: "delete", eventType: etcd3.DELETE, want: "delete"},
		{name: "history delete", eventType: etcd3.HISTORYDELETE, want: "history_delete"},
		{name: "history update", eventType: etcd3.HISTORYUPDATE, want: "history_update"},
		{name: "synced", eventType: etcd3.SYNCED, want: "synced"},
		{name: "error", eventType: etcd3.ERROR, want: "error"},
		{name: "unknown", eventType: -1, want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := instanceWatchEventLabel(tt.eventType); got != tt.want {
				t.Fatalf("instanceWatchEventLabel(%d) = %q, want %q", tt.eventType, got, tt.want)
			}
		})
	}
}

func TestRecordInstanceWatchEventIncrementsMetric(t *testing.T) {
	before := instanceWatchMetricValue(t, "put")
	recordInstanceWatchEvent(etcd3.PUT)
	require.Equal(t, before+1, instanceWatchMetricValue(t, "put"))
}

func instanceWatchMetricValue(t *testing.T, eventType string) float64 {
	t.Helper()
	families, err := metrics.GetRegistry().Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != instanceWatchEventsMetricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			if instanceWatchMetricMatchesEvent(metric.GetLabel(), eventType) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func instanceWatchMetricMatchesEvent(labels []*io_prometheus_client.LabelPair, eventType string) bool {
	for _, label := range labels {
		if label.GetName() == "event_type" && label.GetValue() == eventType {
			return true
		}
	}
	return false
}
