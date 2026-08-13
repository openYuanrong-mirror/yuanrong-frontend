/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package proxyrouting

import (
	"context"
	"fmt"
	"strings"
	"time"

	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/instancemanager"
)

const ownerPollInterval = 100 * time.Millisecond

// Transport selects the endpoint address used for an owner-routed operation.
type Transport int

const (
	// TransportGRPC uses the owner's frontend gRPC endpoint.
	TransportGRPC Transport = iota
	// TransportTCPTunnel uses the owner's TCP tunnel endpoint.
	TransportTCPTunnel
)

// OwnerRoute is the resolved route to the FunctionProxy owning an instance.
type OwnerRoute struct {
	Instance     *types.InstanceSpecification
	OwnerProxyID string
	Endpoint     Endpoint
	Address      string
}

// Resolve returns the currently cached owner route for an instance.
func Resolve(instanceID string, capability Capability, transport Transport) (OwnerRoute, error) {
	if strings.TrimSpace(instanceID) == "" {
		return OwnerRoute{}, fmt.Errorf("instance owner route requires non-empty instance id")
	}
	instance := instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(instanceID)
	if instance == nil {
		return OwnerRoute{}, fmt.Errorf("instance %s is not present in frontend route cache", instanceID)
	}
	return resolveFromInstance(instance, capability, transport)
}

// Wait waits until the instance and a healthy owner route are both available.
func Wait(ctx context.Context, instanceID string, capability Capability, transport Transport) (OwnerRoute, error) {
	return wait(ctx, instanceID, capability, transport, false)
}

// WaitForInvoke additionally resolves an evicting instance when forceInvoke is
// set. Other owner-routed operations continue to use Wait and Running instances.
func WaitForInvoke(ctx context.Context, instanceID string, capability Capability, transport Transport,
	forceInvoke bool) (OwnerRoute, error) {
	return wait(ctx, instanceID, capability, transport, forceInvoke)
}

func wait(ctx context.Context, instanceID string, capability Capability, transport Transport,
	includeEvicting bool) (OwnerRoute, error) {
	if ctx == nil {
		return OwnerRoute{}, fmt.Errorf("instance owner route requires non-nil context")
	}
	if strings.TrimSpace(instanceID) == "" {
		return OwnerRoute{}, fmt.Errorf("instance owner route requires non-empty instance id")
	}
	var instance *types.InstanceSpecification
	var err error
	if includeEvicting {
		instance, err = instancemanager.WaitInstanceByIDForForceInvoke(ctx, instanceID)
	} else {
		instance, err = instancemanager.WaitInstanceByID(ctx, instanceID)
	}
	if err != nil {
		return OwnerRoute{}, fmt.Errorf("wait for instance %s route: %w", instanceID, err)
	}
	ticker := time.NewTicker(ownerPollInterval)
	defer ticker.Stop()
	for {
		route, routeErr := resolveFromInstance(instance, capability, transport)
		if routeErr == nil {
			return route, nil
		}
		select {
		case <-ctx.Done():
			return OwnerRoute{}, fmt.Errorf("resolve owner proxy for instance %s capability %s: %w",
				instanceID, capability, ctx.Err())
		case <-ticker.C:
			current := instancemanager.GetGlobalInstanceScheduler().GetInstanceByIDAcrossFunctions(instanceID)
			if current == nil && includeEvicting {
				current = instancemanager.GetEvictingInstanceByID(instanceID)
			}
			if current != nil {
				instance = current
			}
		}
	}
}

func resolveFromInstance(
	instance *types.InstanceSpecification, capability Capability, transport Transport,
) (OwnerRoute, error) {
	if instance == nil {
		return OwnerRoute{}, fmt.Errorf("instance owner route requires non-nil instance")
	}
	if capability == "" {
		return OwnerRoute{}, fmt.Errorf("instance %s owner route requires capability", instance.InstanceID)
	}
	ownerID := strings.TrimSpace(instance.FunctionProxyID)
	if ownerID == "" {
		return OwnerRoute{}, fmt.Errorf("instance %s has no functionProxyID", instance.InstanceID)
	}
	if IsHostPort(ownerID) {
		return OwnerRoute{}, fmt.Errorf("instance %s functionProxyID must be a proxy node id, got address %q",
			instance.InstanceID, ownerID)
	}
	endpoint, ok := Lookup(ownerID, capability)
	if !ok {
		return OwnerRoute{}, fmt.Errorf("owner proxy %s for instance %s does not publish healthy capability %s",
			ownerID, instance.InstanceID, capability)
	}
	address := endpoint.GRPCAddress
	switch transport {
	case TransportGRPC:
	case TransportTCPTunnel:
		address = endpoint.TCPTunnelAddress
	default:
		return OwnerRoute{}, fmt.Errorf("unsupported proxy transport %d", transport)
	}
	if !IsRoutableAddress(address) {
		return OwnerRoute{}, fmt.Errorf("owner proxy %s has no routable address for capability %s", ownerID, capability)
	}
	return OwnerRoute{Instance: instance, OwnerProxyID: ownerID, Endpoint: endpoint, Address: address}, nil
}
