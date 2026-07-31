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

import "fmt"

// ErrorCode identifies a SessionContext service error.
type ErrorCode string

const (
	// ErrInvalidQuery indicates that query parameters are invalid.
	ErrInvalidQuery ErrorCode = "INVALID_QUERY"
	// ErrSessionNotFound indicates that the requested SessionContext does not exist.
	ErrSessionNotFound ErrorCode = "SESSION_CONTEXT_NOT_FOUND"
	// ErrTurnNotFound indicates that the requested turn does not exist.
	ErrTurnNotFound ErrorCode = "TURN_NOT_FOUND"
	// ErrDataCorrupted indicates that persisted SessionContext data is invalid.
	ErrDataCorrupted ErrorCode = "SESSION_CONTEXT_DATA_CORRUPTED"
	// ErrDataSystem indicates that DataSystem is unavailable.
	ErrDataSystem ErrorCode = "DATASYSTEM_UNAVAILABLE"
)

// ServiceError describes an error returned by the SessionContext query service.
type ServiceError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

// Error returns the formatted service error.
func (e *ServiceError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func dataCorrupted(message string, cause error) error {
	return &ServiceError{Code: ErrDataCorrupted, Message: message, Cause: cause}
}
