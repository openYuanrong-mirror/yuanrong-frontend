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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const hashHexLength = 32

// Scope is the trusted function version scope of one query.
type Scope struct {
	TenantID       string
	RegisteredName string
	RuntimeName    string
	Version        string
	FunctionURN    string
}

// NewScope normalizes the registered function name to the name exposed by FunctionContext.
func NewScope(tenantID, registeredName, version, functionURN string) Scope {
	return Scope{
		TenantID:       tenantID,
		RegisteredName: registeredName,
		RuntimeName:    RuntimeFunctionName(registeredName),
		Version:        version,
		FunctionURN:    functionURN,
	}
}

// RuntimeFunctionName returns the leaf name exposed by FunctionContext.getFunctionName().
func RuntimeFunctionName(registeredName string) string {
	if index := strings.LastIndex(registeredName, "@"); index >= 0 {
		return registeredName[index+1:]
	}
	return registeredName
}

// RegistryKey indexes all versions of one registered function name.
func RegistryKey(registeredName string) string {
	return "ar:i:" + hashParts(registeredName)
}

// TurnKey returns the SDK-compatible key for a Turn record.
func TurnKey(scope Scope, sessionContextID string, turnIndex int) string {
	return fmt.Sprintf("%s:t%d", sessionPrefix(scope, sessionContextID), turnIndex)
}

// EventKey returns the SDK-compatible key for an Event record.
func EventKey(scope Scope, sessionContextID string, seq int) string {
	return fmt.Sprintf("%s:e%d", sessionPrefix(scope, sessionContextID), seq)
}

// ControlKey returns the shared lifecycle control record key.
func ControlKey(scope Scope, sessionContextID string) string {
	return sessionPrefix(scope, sessionContextID) + ":c"
}

func sessionPrefix(scope Scope, sessionContextID string) string {
	return fmt.Sprintf("ar:s:%s:%s",
		hashParts(scope.TenantID, scope.RuntimeName, scope.Version),
		hashParts(sessionContextID))
}

func hashParts(parts ...string) string {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(parts); err != nil {
		panic(fmt.Sprintf("encode session context key: %v", err))
	}
	sum := sha256.Sum256(bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}))
	return hex.EncodeToString(sum[:])[:hashHexLength]
}
