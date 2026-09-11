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

package resolver

import (
	"context"
	"errors"
	"testing"

	"frontend/pkg/common/faas_common/etcd3"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type deletionReaderFunc func(etcd3.EtcdCtxInfo, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)

func (f deletionReaderFunc) GetResponse(ctx etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	defer ctx.Cancel()
	return f(ctx, key, opts...)
}

func TestConfirmInstanceDeleted(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		route, instance, partial, noRevision bool
		failRead                             int
		deleted                              bool
	}{
		{name: "absent", deleted: true}, {name: "route remains", route: true},
		{name: "instance remains", instance: true}, {name: "incomplete scan", partial: true},
		{name: "missing revision", noRevision: true}, {name: "route unavailable", failRead: 1},
		{name: "snapshot compacted", failRead: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			reader := deletionReaderFunc(func(_ etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				calls++
				if tc.failRead == calls {
					return nil, errors.New("read failed")
				}
				op := clientv3.OpGet(key, opts...)
				if op.IsSerializable() {
					t.Fatal("deletion read must be linearizable")
				}
				res := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}}
				if calls == 1 {
					if op.Rev() != 0 || key != instanceRoutePathPrefix+"/inst-abc" {
						t.Fatal("wrong route read")
					}
					if tc.route {
						res.Kvs = []*mvccpb.KeyValue{{Key: []byte(key)}}
					}
					if tc.noRevision {
						res.Header = nil
					}
				} else {
					if calls != 2 || op.Rev() != 42 {
						t.Fatal("instance scan must use route revision")
					}
					res.More = tc.partial
					res.Kvs = []*mvccpb.KeyValue{{Key: []byte(instanceKey + "-another")}}
					if tc.instance {
						res.Kvs = append(res.Kvs, &mvccpb.KeyValue{Key: []byte(instanceKey)})
					}
				}
				return res, nil
			})
			deleted, err := confirmInstanceDeleted(context.Background(), reader, "inst-abc")
			if deleted != tc.deleted {
				t.Fatalf("deleted=%v err=%v", deleted, err)
			}
			if (tc.failRead != 0 || tc.partial || tc.noRevision) != (err != nil) {
				t.Fatalf("unexpected err=%v", err)
			}
		})
	}
}
