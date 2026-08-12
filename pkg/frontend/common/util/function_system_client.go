/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package util

import (
	"context"
	"fmt"

	"yuanrong.org/kernel/runtime/libruntime/api"
)

// FunctionSystemClient is the explicit libruntime boundary for FunctionSystem
// Raw/SDK entrypoints. It is intentionally unrelated to DirectProxyClient and
// is never used as a direct-call fallback.
type FunctionSystemClient interface {
	CreateRaw(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	InvokeRaw(context.Context, []byte, api.RawRequestOption) ([]byte, error)
	KillRaw(context.Context, []byte, api.RawRequestOption) ([]byte, error)
}

type functionSystemClient struct{}

// GetFunctionSystemClient returns the Raw/SDK libruntime boundary.
func GetFunctionSystemClient() FunctionSystemClient { return functionSystemClient{} }

func currentFunctionSystemRuntime() (invokerLibruntime, error) {
	if clientLibruntime == nil {
		return nil, fmt.Errorf("function system libruntime client is not initialized")
	}
	return clientLibruntime, nil
}

func (functionSystemClient) CreateRaw(_ context.Context, payload []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	runtime, err := currentFunctionSystemRuntime()
	if err != nil {
		return nil, err
	}
	return runtime.CreateInstanceRaw(payload, option)
}

func (functionSystemClient) InvokeRaw(_ context.Context, payload []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	runtime, err := currentFunctionSystemRuntime()
	if err != nil {
		return nil, err
	}
	return runtime.InvokeByInstanceIdRaw(payload, option)
}

func (functionSystemClient) KillRaw(_ context.Context, payload []byte,
	option api.RawRequestOption,
) ([]byte, error) {
	runtime, err := currentFunctionSystemRuntime()
	if err != nil {
		return nil, err
	}
	return runtime.KillRaw(payload, option)
}
