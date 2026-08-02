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

// Registry stores all SessionContexts registered for a function.
type Registry struct {
	SchemaVersion   int             `json:"schemaVersion"`
	SessionContexts []RegistryEntry `json:"sessionContexts"`
}

// RegistryEntry describes one registered SessionContext.
type RegistryEntry struct {
	FunctionVersion  string `json:"functionVersion"`
	SessionContextID string `json:"sessionContextId"`
	CreatedAt        string `json:"createdAt"`
	State            string `json:"state,omitempty"`
	ForkedFrom       string `json:"forkedFrom,omitempty"`
	ForkTurnID       string `json:"forkTurnId,omitempty"`
}

// ControlRecord stores lifecycle metadata for a SessionContext.
type ControlRecord struct {
	SchemaVersion int    `json:"schemaVersion"`
	State         string `json:"state"`
	ForkedFrom    string `json:"forkedFrom,omitempty"`
	ForkTurnID    string `json:"forkTurnId,omitempty"`
}

// TurnRecord stores the persisted metadata of a turn.
type TurnRecord struct {
	SessionContextID string `json:"sessionContextId"`
	TurnIndex        int    `json:"turnIndex"`
	TurnID           string `json:"turnId"`
	StartSeq         int    `json:"startSeq"`
	CreatedAt        string `json:"createdAt"`
	SchemaVersion    int    `json:"schemaVersion"`
}

// Event describes a persisted SessionContext event.
type Event struct {
	SessionContextID string `json:"sessionContextId"`
	TurnID           string `json:"turnId"`
	Seq              int    `json:"seq"`
	EventID          string `json:"eventId"`
	Source           string `json:"source"`
	Type             string `json:"type"`
	Data             any    `json:"data"`
	SchemaVersion    int    `json:"schemaVersion"`
	CreatedAt        string `json:"createdAt"`
}

// Turn is the aggregated view of a turn and its events.
type Turn struct {
	TurnID    string `json:"turnId"`
	TurnIndex int    `json:"turnIndex"`
	State     string `json:"state"`
	Inputs    []any  `json:"inputs"`
	Outputs   []any  `json:"outputs"`
	Result    any    `json:"result,omitempty"`
	Error     any    `json:"error,omitempty"`
	StartSeq  int    `json:"startSeq"`
	EndSeq    int    `json:"endSeq"`
	CreatedAt string `json:"createdAt"`
}

// SessionList is a paginated list of SessionContexts.
type SessionList struct {
	FunctionURN     string          `json:"functionUrn"`
	SessionContexts []RegistryEntry `json:"sessionContexts"`
	NextPageToken   *string         `json:"nextPageToken"`
}

// SessionDetail summarizes a SessionContext and its history.
type SessionDetail struct {
	FunctionURN      string           `json:"functionUrn"`
	SessionContextID string           `json:"sessionContextId"`
	TurnCount        int              `json:"turnCount"`
	EventCount       int              `json:"eventCount"`
	LastTurn         *LastTurnSummary `json:"lastTurn"`
	State            string           `json:"state,omitempty"`
	ForkedFrom       string           `json:"forkedFrom,omitempty"`
	ForkTurnID       string           `json:"forkTurnId,omitempty"`
}

// LastTurnSummary summarizes the latest turn in a SessionContext.
type LastTurnSummary struct {
	TurnID    string `json:"turnId"`
	TurnIndex int    `json:"turnIndex"`
	State     string `json:"state"`
}

// TurnList is a paginated list of turns.
type TurnList struct {
	Turns         []Turn  `json:"turns"`
	NextPageToken *string `json:"nextPageToken"`
}

// EventList contains events and the next sequence cursor.
type EventList struct {
	Events       []Event `json:"events"`
	NextAfterSeq int     `json:"nextAfterSeq"`
}

// EventFilter selects events returned by ListEvents.
type EventFilter struct {
	AfterSeq  int
	Limit     int
	TurnID    string
	Source    string
	EventType string
}
