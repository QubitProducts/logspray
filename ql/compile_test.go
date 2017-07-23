// Copyright 2016 Qubit Digital Ltd.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Package logspray is a collection of tools for streaming and indexing
// large volumes of dynamic logs.

package ql

import (
	"strconv"
	"testing"

	"github.com/QubitProducts/logspray/proto/logspray"
)

func TestQuery_Matches(t *testing.T) {
	tests := []struct {
		q        string
		hdr      *logspray.Message
		m        *logspray.Message
		compiles bool
		res      bool
	}{
		{
			"job=myjob",
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			"job=otherjob",
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			"job!=otherjob",
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			"job=*",
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			"job=* instance=servername",
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			"job=* instance=servername",
			&logspray.Message{Labels: map[string]string{
				"job":      "myjob",
				"instance": "otherserver",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			"job=myjob instance=servername instance=otherserver",
			&logspray.Message{Labels: map[string]string{
				"job":      "myjob",
				"instance": "otherserver",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			"job=myjob job=job2 instance=servername",
			&logspray.Message{Labels: map[string]string{
				"job":      "job3",
				"instance": "servername",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			`job=my.+`,
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			`job~my.+`,
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			`job~"my.+"`,
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			`job!~"my.+"`,
			&logspray.Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&logspray.Message{Labels: map[string]string{}},
			true,
			false,
		},
	}

	for i, st := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			mf, err := Compile(st.q)
			if err != nil && st.compiles {
				compiles := false
				t.Fatalf("%s: compile got %v, expected %v", t.Name(), compiles, st.compiles)
			}
			if err == nil && !st.compiles {
				compiles := true
				t.Fatalf("%s: compile got %v, expected %v", t.Name(), compiles, st.compiles)
			}

			if !st.compiles {
				return
			}

			if mf == nil {
				t.Fatalf("No error, but nil match function ")
			}

			if res := mf(st.hdr, st.m); res != st.res {
				t.Fatalf("%s: got res = %v, expected %v", t.Name(), res, st.res)
			}
		})
	}
}
