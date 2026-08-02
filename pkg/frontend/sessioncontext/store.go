/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package sessioncontext

import (
	"errors"
	"fmt"

	"frontend/pkg/common/faas_common/datasystemclient"
)

// Reader is the exact-key read boundary used by the query service.
type Reader interface {
	Get(key, tenantID, traceID string) ([]byte, error)
}

// DataSystemReader uses the Frontend's tenant-aware DataSystem client.
type DataSystemReader struct{}

func (DataSystemReader) Get(key, tenantID, traceID string) ([]byte, error) {
	value, err := datasystemclient.KVGetWithRetry(
		key, &datasystemclient.Option{TenantID: tenantID}, traceID)
	if errors.Is(err, datasystemclient.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, &ServiceError{Code: ErrDataSystem, Message: "failed to read DataSystem", Cause: err}
	}
	return value, nil
}

func readSequential[T any](
	reader Reader,
	scope Scope,
	sessionContextID string,
	traceID string,
	key func(Scope, string, int) string,
	validate func(*T, int, string) error,
) ([]T, error) {
	result := make([]T, 0)
	for index := 1; ; index++ {
		raw, err := reader.Get(key(scope, sessionContextID, index), scope.TenantID, traceID)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return result, nil
		}
		var value T
		if err = decodeJSON(raw, &value); err != nil {
			return nil, dataCorrupted(fmt.Sprintf("invalid record at index %d", index), err)
		}
		if err = validate(&value, index, sessionContextID); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
}
