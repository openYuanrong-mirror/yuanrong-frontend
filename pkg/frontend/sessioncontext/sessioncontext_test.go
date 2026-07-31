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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	secondTurnIndex   = 2
	secondEventSeq    = 2
	thirdEventSeq     = 3
	fourthEventSeq    = 4
	fifthEventSeq     = 5
	sixthEventSeq     = 6
	seventhEventSeq   = 7
	testPageSize      = 50
	sessionPageSize   = 2
	expectedTurnCount = 2
)

type memoryReader map[string][]byte

func (m memoryReader) Get(key, _, _ string) ([]byte, error) {
	return m[key], nil
}

func TestKeysMatchPythonSDKVectors(t *testing.T) {
	scope := NewScope("default", "0@agentrt@e2e0729", "latest", "urn")
	require.Equal(t, "e2e0729", scope.RuntimeName)
	require.Equal(t, "ar:i:44224b5b608c24b01ed67d10e61110bc", RegistryKey(scope.RegisteredName))
	require.Equal(t,
		"ar:s:6c0f98c86e41fef4c4b7d698410a74b3:a35e382c3650daf4301f45bd242f9c3f:t1",
		TurnKey(scope, "sdk-e2e-session-202", 1))
	require.Equal(t,
		"ar:s:6c0f98c86e41fef4c4b7d698410a74b3:a35e382c3650daf4301f45bd242f9c3f:e1",
		EventKey(scope, "sdk-e2e-session-202", 1))
}

func TestHashPartsMatchesPythonSDKForSpecialCharacters(t *testing.T) {
	require.Equal(t, "78ff3361117058d209f96a2334ce7045", hashParts("a&b"))
	require.Equal(t, "8c144a7602db11518476ebee4a99fe58", hashParts("a<b"))
	require.Equal(t, "65bcb1a065d8b11ec83cecd06be6e3ae", hashParts("中文"))
	require.Equal(t, "011dd9da72bd83788f26f20d0b5b8fb2", hashParts("x\u2028y"))
}

func TestServiceAggregatesInputRequiredThenCompleteAndNextTurn(t *testing.T) {
	scope := NewScope("default", "0@agentrt@e2e0729", "latest", "function-urn")
	sessionID := "session-1"
	reader := memoryReader{}
	put := func(key string, value any) {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		reader[key] = raw
	}
	put(TurnKey(scope, sessionID, 1), TurnRecord{
		SessionContextID: sessionID, TurnIndex: 1, TurnID: "turn-000001",
		StartSeq: 1, CreatedAt: "2026-07-29T09:00:00Z", SchemaVersion: 1,
	})
	put(TurnKey(scope, sessionID, secondTurnIndex), TurnRecord{
		SessionContextID: sessionID, TurnIndex: secondTurnIndex, TurnID: "turn-000002",
		StartSeq: fifthEventSeq, CreatedAt: "2026-07-29T09:01:00Z", SchemaVersion: 1,
	})
	events := []Event{
		event(sessionID, "turn-000001", 1, "input.message", map[string]any{"message": "hello"}),
		event(sessionID, "turn-000001", secondEventSeq, "turn.input_required", map[string]any{"output": "confirm?"}),
		event(sessionID, "turn-000001", thirdEventSeq, "input.message", map[string]any{"message": true}),
		event(sessionID, "turn-000001", fourthEventSeq, "turn.completed", map[string]any{"output": "done"}),
		event(sessionID, "turn-000002", fifthEventSeq, "input.message", map[string]any{"message": "next"}),
		event(sessionID, "turn-000002", sixthEventSeq, "output.message", map[string]any{"message": "progress"}),
		event(sessionID, "turn-000002", seventhEventSeq, "turn.input_required", map[string]any{"output": "again?"}),
	}
	for _, value := range events {
		put(EventKey(scope, sessionID, value.Seq), value)
	}
	service := NewService(reader)

	list, err := service.ListTurns(scope, sessionID, testPageSize, "", "trace")
	require.NoError(t, err)
	require.Len(t, list.Turns, expectedTurnCount)
	require.Equal(t, "turn-000002", list.Turns[0].TurnID)
	require.Equal(t, "INPUT_REQUIRED", list.Turns[0].State)
	require.Equal(t, []any{"next"}, list.Turns[0].Inputs)
	require.Equal(t, []any{"progress"}, list.Turns[0].Outputs)
	require.Equal(t, "COMPLETED", list.Turns[1].State)
	require.Equal(t, []any{"hello", true}, list.Turns[1].Inputs)
	require.Equal(t, "done", list.Turns[1].Result)
}

func TestAggregateTurnsRejectsNonObjectEventData(t *testing.T) {
	record := TurnRecord{
		SessionContextID: "session-1", TurnIndex: 1, TurnID: "turn-1",
		StartSeq: 1, CreatedAt: "2026-07-31T00:00:00Z", SchemaVersion: schemaVersion,
	}
	eventTypes := []string{
		"input.message", "output.message", "turn.input_required", "turn.completed", "turn.failed",
	}
	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			_, err := aggregateTurns([]TurnRecord{record}, []Event{{
				SessionContextID: "session-1", TurnID: "turn-1", Seq: 1,
				EventID: "event-1", Source: "SDK", Type: eventType,
				Data: "invalid", SchemaVersion: schemaVersion, CreatedAt: "2026-07-31T00:00:00Z",
			}})

			var serviceErr *ServiceError
			require.True(t, errors.As(err, &serviceErr))
			require.Equal(t, ErrDataCorrupted, serviceErr.Code)
		})
	}
}

func TestListSessionsFiltersSortsAndPaginates(t *testing.T) {
	scope := NewScope("default", "0@agentrt@e2e0729", "latest", "function-urn")
	raw, err := json.Marshal(Registry{SchemaVersion: 1, SessionContexts: []RegistryEntry{
		{FunctionVersion: "latest", SessionContextID: "b", CreatedAt: "2026-07-29T09:00:00Z"},
		{FunctionVersion: "v2", SessionContextID: "ignored", CreatedAt: "2026-07-30T09:00:00Z"},
		{FunctionVersion: "latest", SessionContextID: "a", CreatedAt: "2026-07-29T09:00:00Z"},
		{FunctionVersion: "latest", SessionContextID: "new", CreatedAt: "2026-07-30T09:00:00Z"},
	}})
	require.NoError(t, err)
	service := NewService(memoryReader{RegistryKey(scope.RegisteredName): raw})

	first, err := service.ListSessions(scope, sessionPageSize, "", "trace")
	require.NoError(t, err)
	require.Equal(t, []string{"new", "a"}, []string{
		first.SessionContexts[0].SessionContextID, first.SessionContexts[1].SessionContextID,
	})
	require.NotNil(t, first.NextPageToken)
	second, err := service.ListSessions(scope, sessionPageSize, *first.NextPageToken, "trace")
	require.NoError(t, err)
	require.Equal(t, "b", second.SessionContexts[0].SessionContextID)
	require.Nil(t, second.NextPageToken)
}

func TestGetSessionReturnsEmptyDetailForRegisteredSessionWithoutHistory(t *testing.T) {
	scope := NewScope("default", "0@agentrt@agent", "latest", "function-urn")
	raw, err := json.Marshal(Registry{SchemaVersion: 1, SessionContexts: []RegistryEntry{{
		FunctionVersion: "latest", SessionContextID: "registered",
		CreatedAt: "2026-07-30T09:00:00Z",
	}}})
	require.NoError(t, err)
	service := NewService(memoryReader{RegistryKey(scope.RegisteredName): raw})

	detail, err := service.GetSession(scope, "registered", "trace")

	require.NoError(t, err)
	require.Equal(t, "registered", detail.SessionContextID)
	require.Zero(t, detail.TurnCount)
	require.Zero(t, detail.EventCount)
	require.Nil(t, detail.LastTurn)
}

func TestGetSessionReturnsNotFoundWhenHistoryAndRegistryAreMissing(t *testing.T) {
	scope := NewScope("default", "0@agentrt@agent", "latest", "function-urn")
	_, err := NewService(memoryReader{}).GetSession(scope, "missing", "trace")

	var serviceErr *ServiceError
	require.True(t, errors.As(err, &serviceErr))
	require.Equal(t, ErrSessionNotFound, serviceErr.Code)
}

func TestGetSessionReturnsCreatingControlWithoutHistoryOrRegistry(t *testing.T) {
	scope := NewScope("default", "0@agentrt@agent", "latest", "function-urn")
	raw, err := json.Marshal(ControlRecord{
		SchemaVersion: 1, State: "CREATING", ForkedFrom: "source", ForkTurnID: "turn-1",
	})
	require.NoError(t, err)
	detail, err := NewService(memoryReader{
		ControlKey(scope, "target"): raw,
	}).GetSession(scope, "target", "trace")

	require.NoError(t, err)
	require.Equal(t, "CREATING", detail.State)
	require.Equal(t, "source", detail.ForkedFrom)
	require.Equal(t, "turn-1", detail.ForkTurnID)
}

func event(sessionID, turnID string, seq int, eventType string, data any) Event {
	return Event{
		SessionContextID: sessionID, TurnID: turnID, Seq: seq,
		EventID: "event", Source: "PLATFORM", Type: eventType, Data: data,
		SchemaVersion: 1, CreatedAt: "2026-07-29T09:00:00Z",
	}
}
