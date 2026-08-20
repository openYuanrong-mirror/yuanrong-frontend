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
	"fmt"
	"strconv"
	"strings"
)

// JSONInt64 accepts both a JSON number and protobuf JSON's quoted int64.
// MessageToJsonString follows the protobuf JSON mapping and therefore emits
// version as a string even though the logical field is an int64.
type JSONInt64 int64

// UnmarshalJSON accepts protobuf JSON's quoted int64 and ordinary JSON numbers.
func (v *JSONInt64) UnmarshalJSON(data []byte) error {
	encoded := strings.TrimSpace(string(data))
	if len(encoded) >= 2 && encoded[0] == '"' && encoded[len(encoded)-1] == '"' {
		decoded, err := strconv.Unquote(encoded)
		if err != nil {
			return fmt.Errorf("decode quoted int64: %w", err)
		}
		encoded = decoded
	}
	parsed, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return fmt.Errorf("decode int64 %q: %w", encoded, err)
	}
	*v = JSONInt64(parsed)
	return nil
}
