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

package posixws

import (
	"context"
	"encoding/base64"

	"yuanrong.org/kernel/runtime/libruntime/api"

	"frontend/pkg/frontend/common/util"
)

// Processor handles create/invoke operations
type Processor struct {
	client util.FunctionSystemClient
}

// NewProcessor creates a new Processor instance
func NewProcessor() *Processor {
	return &Processor{
		client: util.GetFunctionSystemClient(),
	}
}

// ProcessCreate handles create operation
func (p *Processor) ProcessCreate(ctx context.Context, payload string, headers map[string]string) (string, error) {
	rawPayload, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", err
	}

	option := api.RawRequestOption{
		TraceParent: headers["X-Trace-Parent"],
	}

	resp, err := p.client.CreateRaw(ctx, rawPayload, option)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(resp), nil
}

// ProcessInvoke handles invoke operation
func (p *Processor) ProcessInvoke(ctx context.Context, payload string, headers map[string]string) (string, error) {
	rawPayload, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", err
	}

	option := api.RawRequestOption{
		TraceParent: headers["X-Trace-Parent"],
	}

	resp, err := p.client.InvokeRaw(ctx, rawPayload, option)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(resp), nil
}

// ProcessCreateRaw handles create operation with raw binary payload (no base64)
func (p *Processor) ProcessCreateRaw(ctx context.Context, payload []byte, headers map[string]string) ([]byte, error) {
	option := api.RawRequestOption{
		TraceParent: headers["X-Trace-Parent"],
	}
	return p.client.CreateRaw(ctx, payload, option)
}

// ProcessInvokeRaw handles invoke operation with raw binary payload (no base64)
func (p *Processor) ProcessInvokeRaw(ctx context.Context, payload []byte, headers map[string]string) ([]byte, error) {
	option := api.RawRequestOption{
		TraceParent: headers["X-Trace-Parent"],
	}
	return p.client.InvokeRaw(ctx, payload, option)
}
