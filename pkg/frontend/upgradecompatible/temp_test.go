/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

// Package upgradecompatible -
package upgradecompatible

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetAccessFaaSSchedulerType(t *testing.T) {
	Convey("Given empty accessFaaSSchedulerType", t, func() {
		Convey("When call GetAccessFaaSSchedulerType", func() {
			result := GetAccessFaaSSchedulerType()
			Convey("Then should return default string", func() {
				So(result, ShouldEqual, DefaultAccessSchedulerType)
			})
		})
	})

	Convey("Given initialized accessFaaSSchedulerType", t, func() {
		accessFaaSSchedulerType = "test-value"
		Convey("When call GetAccessFaaSSchedulerType", func() {
			result := GetAccessFaaSSchedulerType()
			Convey("Then should return correct value", func() {
				So(result, ShouldEqual, "test-value")
			})
		})
	})
}

func TestIsLiteSchedulerEnabled(t *testing.T) {
	t.Setenv(LiteSchedulerEnableEnv, "true")
	if !IsLiteSchedulerEnabled() {
		t.Fatal("expected LiteScheduler to be enabled")
	}
	t.Setenv(LiteSchedulerEnableEnv, "false")
	if IsLiteSchedulerEnabled() {
		t.Fatal("expected LiteScheduler to be disabled")
	}
	t.Setenv(LiteSchedulerEnableEnv, "invalid")
	if IsLiteSchedulerEnabled() {
		t.Fatal("expected an invalid value to preserve the disabled default")
	}
}

func TestLoadAccessFaaSSchedulerType(t *testing.T) {
	Convey("Given valid config file", t, func() {
		tempFile, err := os.CreateTemp("", "test-config-*.json")
		So(err, ShouldBeNil)
		defer os.Remove(tempFile.Name())

		testData := `{"accessFaaSSchedulerType": "test-type"}`
		err = os.WriteFile(tempFile.Name(), []byte(testData), 0644)
		So(err, ShouldBeNil)

		Convey("When call loadAccessFaaSSchedulerType", func() {
			loadAccessFaaSSchedulerType(tempFile.Name(), 0)

			Convey("Then should set correct value", func() {
				So(GetAccessFaaSSchedulerType(), ShouldEqual, "test-type")
			})
		})
	})

	Convey("Given invalid config file", t, func() {
		tempFile, err := os.CreateTemp("", "test-config-*.json")
		So(err, ShouldBeNil)
		defer os.Remove(tempFile.Name())

		testData := `invalid json`
		err = os.WriteFile(tempFile.Name(), []byte(testData), 0644)
		So(err, ShouldBeNil)

		originalValue := GetAccessFaaSSchedulerType()

		Convey("When call loadAccessFaaSSchedulerType", func() {
			loadAccessFaaSSchedulerType(tempFile.Name(), 0)

			Convey("Then should not change the value", func() {
				So(GetAccessFaaSSchedulerType(), ShouldEqual, originalValue)
			})
		})
	})
}
