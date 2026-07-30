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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

const schemaVersion = 1

type Service struct {
	reader Reader
}

func NewService(reader Reader) *Service {
	return &Service{reader: reader}
}

func (s *Service) ListSessions(scope Scope, limit int, pageToken, traceID string) (SessionList, error) {
	raw, err := s.reader.Get(RegistryKey(scope.RegisteredName), scope.TenantID, traceID)
	if err != nil {
		return SessionList{}, err
	}
	entries := make([]RegistryEntry, 0)
	if raw != nil {
		var registry Registry
		if err = decodeJSON(raw, &registry); err != nil {
			return SessionList{}, dataCorrupted("invalid session context registry", err)
		}
		if registry.SchemaVersion != schemaVersion {
			return SessionList{}, dataCorrupted("unsupported session context registry schemaVersion", nil)
		}
		for _, entry := range registry.SessionContexts {
			if entry.FunctionVersion == scope.Version {
				control, controlErr := s.readControl(scope, entry.SessionContextID, traceID)
				if controlErr != nil {
					return SessionList{}, controlErr
				}
				if control != nil {
					entry.State = control.State
					entry.ForkedFrom = control.ForkedFrom
					entry.ForkTurnID = control.ForkTurnID
				}
				entries = append(entries, entry)
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].CreatedAt == entries[j].CreatedAt {
			return entries[i].SessionContextID < entries[j].SessionContextID
		}
		return entries[i].CreatedAt > entries[j].CreatedAt
	})
	offset, err := decodePageToken(pageToken, pageScope(scope, "sessions", ""), len(entries))
	if err != nil {
		return SessionList{}, err
	}
	end := min(offset+limit, len(entries))
	var next *string
	if end < len(entries) {
		token := encodePageToken(pageScope(scope, "sessions", ""), end)
		next = &token
	}
	return SessionList{
		FunctionURN: scope.FunctionURN, SessionContexts: entries[offset:end], NextPageToken: next,
	}, nil
}

func (s *Service) GetSession(scope Scope, sessionContextID, traceID string) (SessionDetail, error) {
	turns, events, err := s.readHistory(scope, sessionContextID, traceID)
	if err != nil {
		return SessionDetail{}, err
	}
	control, err := s.readControl(scope, sessionContextID, traceID)
	if err != nil {
		return SessionDetail{}, err
	}
	if len(turns) == 0 && len(events) == 0 {
		registered, registryErr := s.isRegisteredSession(scope, sessionContextID, traceID)
		if registryErr != nil {
			return SessionDetail{}, registryErr
		}
		if !registered && control == nil {
			return SessionDetail{}, &Error{Code: ErrSessionNotFound, Message: "session context not found"}
		}
	}
	aggregated, err := aggregateTurns(turns, events)
	if err != nil {
		return SessionDetail{}, err
	}
	var last *LastTurnSummary
	if len(aggregated) != 0 {
		value := aggregated[len(aggregated)-1]
		last = &LastTurnSummary{
			TurnID: value.TurnID, TurnIndex: value.TurnIndex, State: value.State,
		}
	}
	detail := SessionDetail{
		FunctionURN: scope.FunctionURN, SessionContextID: sessionContextID,
		TurnCount: len(turns), EventCount: len(events), LastTurn: last,
	}
	if control != nil {
		detail.State = control.State
		detail.ForkedFrom = control.ForkedFrom
		detail.ForkTurnID = control.ForkTurnID
	}
	return detail, nil
}

func (s *Service) readControl(scope Scope, sessionContextID, traceID string) (*ControlRecord, error) {
	raw, err := s.reader.Get(ControlKey(scope, sessionContextID), scope.TenantID, traceID)
	if err != nil || raw == nil {
		return nil, err
	}
	var control ControlRecord
	if err = decodeJSON(raw, &control); err != nil {
		return nil, dataCorrupted("invalid SessionContext control record", err)
	}
	if control.SchemaVersion != schemaVersion {
		return nil, dataCorrupted("unsupported SessionContext control schemaVersion", nil)
	}
	return &control, nil
}

func (s *Service) isRegisteredSession(scope Scope, sessionContextID, traceID string) (bool, error) {
	raw, err := s.reader.Get(RegistryKey(scope.RegisteredName), scope.TenantID, traceID)
	if err != nil {
		return false, err
	}
	if raw == nil {
		return false, nil
	}
	var registry Registry
	if err = decodeJSON(raw, &registry); err != nil {
		return false, dataCorrupted("invalid session context registry", err)
	}
	if registry.SchemaVersion != schemaVersion {
		return false, dataCorrupted("unsupported session context registry schemaVersion", nil)
	}
	for _, entry := range registry.SessionContexts {
		if entry.FunctionVersion == scope.Version && entry.SessionContextID == sessionContextID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) ListTurns(
	scope Scope, sessionContextID string, limit int, pageToken, traceID string,
) (TurnList, error) {
	turns, events, err := s.readHistory(scope, sessionContextID, traceID)
	if err != nil {
		return TurnList{}, err
	}
	if len(turns) == 0 && len(events) == 0 {
		return TurnList{}, &Error{Code: ErrSessionNotFound, Message: "session context not found"}
	}
	aggregated, err := aggregateTurns(turns, events)
	if err != nil {
		return TurnList{}, err
	}
	reverseTurns(aggregated)
	tokenScope := pageScope(scope, "turns", sessionContextID)
	offset, err := decodePageToken(pageToken, tokenScope, len(aggregated))
	if err != nil {
		return TurnList{}, err
	}
	end := min(offset+limit, len(aggregated))
	var next *string
	if end < len(aggregated) {
		token := encodePageToken(tokenScope, end)
		next = &token
	}
	return TurnList{Turns: aggregated[offset:end], NextPageToken: next}, nil
}

func (s *Service) GetTurn(scope Scope, sessionContextID, turnID, traceID string) (Turn, error) {
	turns, events, err := s.readHistory(scope, sessionContextID, traceID)
	if err != nil {
		return Turn{}, err
	}
	aggregated, err := aggregateTurns(turns, events)
	if err != nil {
		return Turn{}, err
	}
	for _, turn := range aggregated {
		if turn.TurnID == turnID {
			return turn, nil
		}
	}
	return Turn{}, &Error{Code: ErrTurnNotFound, Message: "turn not found"}
}

func (s *Service) ListEvents(
	scope Scope, sessionContextID string, filter EventFilter, traceID string,
) (EventList, error) {
	if filter.AfterSeq < 0 || filter.Limit <= 0 {
		return EventList{}, &Error{Code: ErrInvalidQuery, Message: "invalid event query"}
	}
	result := make([]Event, 0, filter.Limit)
	nextAfterSeq := filter.AfterSeq
	for seq := filter.AfterSeq + 1; ; seq++ {
		raw, err := s.reader.Get(EventKey(scope, sessionContextID, seq), scope.TenantID, traceID)
		if err != nil {
			return EventList{}, err
		}
		if raw == nil {
			break
		}
		var event Event
		if err = decodeJSON(raw, &event); err != nil {
			return EventList{}, dataCorrupted(fmt.Sprintf("invalid Event e%d", seq), err)
		}
		if err = validateEvent(&event, seq, sessionContextID); err != nil {
			return EventList{}, err
		}
		nextAfterSeq = seq
		if (filter.TurnID == "" || event.TurnID == filter.TurnID) &&
			(filter.Source == "" || event.Source == filter.Source) &&
			(filter.EventType == "" || event.Type == filter.EventType) {
			result = append(result, event)
			if len(result) == filter.Limit {
				break
			}
		}
	}
	if nextAfterSeq == filter.AfterSeq && filter.AfterSeq == 0 {
		firstTurn, err := s.reader.Get(TurnKey(scope, sessionContextID, 1), scope.TenantID, traceID)
		if err != nil {
			return EventList{}, err
		}
		if firstTurn == nil {
			return EventList{}, &Error{Code: ErrSessionNotFound, Message: "session context not found"}
		}
	}
	return EventList{Events: result, NextAfterSeq: nextAfterSeq}, nil
}

func (s *Service) readHistory(
	scope Scope, sessionContextID, traceID string,
) ([]TurnRecord, []Event, error) {
	turns, err := readSequential(s.reader, scope, sessionContextID, traceID, TurnKey, validateTurn)
	if err != nil {
		return nil, nil, err
	}
	events, err := readSequential(s.reader, scope, sessionContextID, traceID, EventKey, validateEvent)
	if err != nil {
		return nil, nil, err
	}
	return turns, events, nil
}

func validateTurn(turn *TurnRecord, index int, sessionContextID string) error {
	if turn.SchemaVersion != schemaVersion || turn.TurnIndex != index ||
		turn.SessionContextID != sessionContextID || turn.TurnID == "" ||
		turn.StartSeq <= 0 || turn.CreatedAt == "" {
		return dataCorrupted(fmt.Sprintf("Turn t%d does not match its key or scope", index), nil)
	}
	return nil
}

func validateEvent(event *Event, seq int, sessionContextID string) error {
	if event.SchemaVersion != schemaVersion || event.Seq != seq ||
		event.SessionContextID != sessionContextID || event.TurnID == "" ||
		event.EventID == "" || event.Type == "" || event.CreatedAt == "" ||
		(event.Source != "PLATFORM" && event.Source != "SDK") {
		return dataCorrupted(fmt.Sprintf("Event e%d does not match its key or scope", seq), nil)
	}
	return nil
}

func decodeJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

type pageTokenValue struct {
	Scope  string `json:"scope"`
	Offset int    `json:"offset"`
}

func pageScope(scope Scope, resource, sessionContextID string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s", scope.FunctionURN, resource, scope.Version, sessionContextID)
}

func encodePageToken(scope string, offset int) string {
	raw, _ := json.Marshal(pageTokenValue{Scope: scope, Offset: offset})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePageToken(token, scope string, size int) (int, error) {
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, &Error{Code: ErrInvalidQuery, Message: "invalid pageToken", Cause: err}
	}
	var value pageTokenValue
	if err = json.Unmarshal(raw, &value); err != nil || value.Scope != scope ||
		value.Offset < 0 || value.Offset > size {
		return 0, &Error{Code: ErrInvalidQuery, Message: "pageToken does not match query scope"}
	}
	return value.Offset, nil
}

func reverseTurns(turns []Turn) {
	for left, right := 0, len(turns)-1; left < right; left, right = left+1, right-1 {
		turns[left], turns[right] = turns[right], turns[left]
	}
}
