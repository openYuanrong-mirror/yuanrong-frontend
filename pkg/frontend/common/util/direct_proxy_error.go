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

package util

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"frontend/pkg/common/faas_common/grpc/pb/common"
)

const (
	directProxyPreDispatchReason  = "pre-dispatch"
	directProxyPostDispatchReason = "post-dispatch-unknown"
)

// DirectProxyErrorMetadata preserves the FunctionSystem error code and whether
// retrying may start user code. Retryable is true only when dispatch is known
// not to have succeeded.
type DirectProxyErrorMetadata struct {
	Code        int
	Retryable   bool
	RetryReason string
}

type directProxyClassifiedError interface {
	error
	directProxyErrorMetadata() DirectProxyErrorMetadata
}

type directProxyTransportError struct {
	operation string
	cause     error
	metadata  DirectProxyErrorMetadata
}

func (e *directProxyTransportError) Error() string {
	if e == nil {
		return "request failed"
	}
	if isDirectProxyTimeout(e.cause) {
		return "request timed out"
	}
	return fmt.Sprintf("request failed: %v", e.cause)
}

func isDirectProxyTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (e *directProxyTransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *directProxyTransportError) directProxyErrorMetadata() DirectProxyErrorMetadata {
	if e == nil {
		return DirectProxyErrorMetadata{}
	}
	return e.metadata
}

func newDirectProxyPreDispatchError(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return &directProxyTransportError{
		operation: operation,
		cause:     cause,
		metadata: DirectProxyErrorMetadata{
			Code:        int(common.ErrorCode_ERR_INNER_COMMUNICATION),
			Retryable:   true,
			RetryReason: directProxyPreDispatchReason,
		},
	}
}

func newDirectProxyPostDispatchError(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return &directProxyTransportError{
		operation: operation,
		cause:     cause,
		metadata: DirectProxyErrorMetadata{
			Code:        int(common.ErrorCode_ERR_INNER_SYSTEM_ERROR),
			Retryable:   false,
			RetryReason: directProxyPostDispatchReason,
		},
	}
}

// GetDirectProxyErrorMetadata extracts direct invocation retry semantics
// without coupling the direct client to libruntime's api.ErrorInfo.
func GetDirectProxyErrorMetadata(err error) (DirectProxyErrorMetadata, bool) {
	var classified directProxyClassifiedError
	if !errors.As(err, &classified) {
		return DirectProxyErrorMetadata{}, false
	}
	return classified.directProxyErrorMetadata(), true
}
