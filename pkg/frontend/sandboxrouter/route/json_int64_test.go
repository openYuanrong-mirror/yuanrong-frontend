/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

package route

import (
	"encoding/json"
	"testing"
)

func TestJSONInt64AcceptsProtobufQuotedAndNumericForms(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  JSONInt64
	}{
		{name: "protobuf quoted", input: `"42"`, want: 42},
		{name: "numeric", input: `42`, want: 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got JSONInt64
			if err := json.Unmarshal([]byte(test.input), &got); err != nil {
				t.Fatalf("Unmarshal(%s): %v", test.input, err)
			}
			if got != test.want {
				t.Fatalf("Unmarshal(%s) = %d, want %d", test.input, got, test.want)
			}
		})
	}
}

func TestJSONInt64RejectsNonInteger(t *testing.T) {
	for _, input := range []string{`""`, `"4.2"`, `null`, `{}`} {
		var got JSONInt64
		if err := json.Unmarshal([]byte(input), &got); err == nil {
			t.Fatalf("Unmarshal(%s) unexpectedly succeeded", input)
		}
	}
}
