/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package sshproxy

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"frontend/pkg/frontend/sandboxrouter/execendpoint"
)

func TestSSHServerConnectionLimit(t *testing.T) {
	srv := &server{slots: make(chan struct{}, 1)}

	require.True(t, srv.acquireConnection())
	require.False(t, srv.acquireConnection())
	srv.releaseConnection()
	require.True(t, srv.acquireConnection())
	srv.releaseConnection()
}

func TestResolveInstanceRejectsPausedBeforeTunnelRouting(t *testing.T) {
	const instanceID = "paused-ssh"
	execendpoint.Default().PutSummary(execendpoint.Summary{
		InstanceID: instanceID, NodeID: "InstanceManagerOwner", StatusCode: 13,
	})
	t.Cleanup(func() { execendpoint.Default().Delete(instanceID) })

	srv := &server{config: &serverConfig{routeWait: time.Second}}
	_, _, err := srv.resolveInstance(route{InstanceID: instanceID})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "instance paused-ssh is paused"), err)
}
