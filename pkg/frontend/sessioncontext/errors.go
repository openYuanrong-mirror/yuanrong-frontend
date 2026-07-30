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

type ErrorCode string

const (
	ErrInvalidQuery    ErrorCode = "INVALID_QUERY"
	ErrSessionNotFound ErrorCode = "SESSION_CONTEXT_NOT_FOUND"
	ErrTurnNotFound    ErrorCode = "TURN_NOT_FOUND"
	ErrDataCorrupted   ErrorCode = "SESSION_CONTEXT_DATA_CORRUPTED"
	ErrDataSystem      ErrorCode = "DATASYSTEM_UNAVAILABLE"
)

type Error struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func dataCorrupted(message string, cause error) error {
	return &Error{Code: ErrDataCorrupted, Message: message, Cause: cause}
}
