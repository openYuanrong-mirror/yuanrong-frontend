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

type Registry struct {
	SchemaVersion   int             `json:"schemaVersion"`
	SessionContexts []RegistryEntry `json:"sessionContexts"`
}

type RegistryEntry struct {
	FunctionVersion  string `json:"functionVersion"`
	SessionContextID string `json:"sessionContextId"`
	CreatedAt        string `json:"createdAt"`
	State            string `json:"state,omitempty"`
	ForkedFrom       string `json:"forkedFrom,omitempty"`
	ForkTurnID       string `json:"forkTurnId,omitempty"`
}

type ControlRecord struct {
	SchemaVersion int    `json:"schemaVersion"`
	State         string `json:"state"`
	ForkedFrom    string `json:"forkedFrom,omitempty"`
	ForkTurnID    string `json:"forkTurnId,omitempty"`
}

type TurnRecord struct {
	SessionContextID string `json:"sessionContextId"`
	TurnIndex        int    `json:"turnIndex"`
	TurnID           string `json:"turnId"`
	StartSeq         int    `json:"startSeq"`
	CreatedAt        string `json:"createdAt"`
	SchemaVersion    int    `json:"schemaVersion"`
}

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

type SessionList struct {
	FunctionURN     string          `json:"functionUrn"`
	SessionContexts []RegistryEntry `json:"sessionContexts"`
	NextPageToken   *string         `json:"nextPageToken"`
}

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

type LastTurnSummary struct {
	TurnID    string `json:"turnId"`
	TurnIndex int    `json:"turnIndex"`
	State     string `json:"state"`
}

type TurnList struct {
	Turns         []Turn  `json:"turns"`
	NextPageToken *string `json:"nextPageToken"`
}

type EventList struct {
	Events       []Event `json:"events"`
	NextAfterSeq int     `json:"nextAfterSeq"`
}

type EventFilter struct {
	AfterSeq  int
	Limit     int
	TurnID    string
	Source    string
	EventType string
}
