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

func aggregateTurns(records []TurnRecord, events []Event) ([]Turn, error) {
	result := make([]Turn, 0, len(records))
	eventIndex := 0
	for index, record := range records {
		endSeq := record.StartSeq - 1
		if len(events) != 0 {
			endSeq = events[len(events)-1].Seq
		}
		if index+1 < len(records) {
			endSeq = records[index+1].StartSeq - 1
		}
		if record.StartSeq > endSeq+1 {
			return nil, dataCorrupted(fmt.Sprintf("Turn %s has an invalid event range", record.TurnID), nil)
		}
		turn := Turn{
			TurnID: record.TurnID, TurnIndex: record.TurnIndex, State: "WORKING",
			Inputs: []any{}, Outputs: []any{}, StartSeq: record.StartSeq,
			EndSeq: max(record.StartSeq-1, endSeq), CreatedAt: record.CreatedAt,
		}
		for eventIndex < len(events) && events[eventIndex].Seq < record.StartSeq {
			eventIndex++
		}
		for eventIndex < len(events) && events[eventIndex].Seq <= endSeq {
			event := events[eventIndex]
			if event.TurnID != record.TurnID {
				return nil, dataCorrupted(
					fmt.Sprintf("Event e%d belongs to an unexpected Turn", event.Seq), nil)
			}
			if err := applyEvent(&turn, event); err != nil {
				return nil, err
			}
			eventIndex++
		}
		result = append(result, turn)
	}
	if eventIndex != len(events) {
		return nil, dataCorrupted("EventLog contains events outside known Turns", nil)
	}
	return result, nil
}

func applyEvent(turn *Turn, event Event) error {
	switch event.Type {
	case "input.message", "output.message", "turn.input_required", "turn.completed", "turn.failed":
	default:
		return nil
	}
	data, ok := event.Data.(map[string]any)
	if !ok {
		return dataCorrupted(fmt.Sprintf("Event e%d data must be an object", event.Seq), nil)
	}
	switch event.Type {
	case "input.message":
		turn.State = "WORKING"
		if message, ok := data["message"]; ok {
			turn.Inputs = append(turn.Inputs, message)
		}
	case "output.message":
		if message, ok := data["message"]; ok {
			turn.Outputs = append(turn.Outputs, message)
		}
	case "turn.input_required":
		turn.State = "INPUT_REQUIRED"
		turn.Result = data["output"]
		turn.Error = nil
	case "turn.completed":
		turn.State = "COMPLETED"
		turn.Result = data["output"]
		turn.Error = nil
	case "turn.failed":
		turn.State = "FAILED"
		turn.Result = nil
		turn.Error = data["error"]
	}
	return nil
}
