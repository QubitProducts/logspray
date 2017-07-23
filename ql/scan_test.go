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
	"bytes"
	"reflect"
	"strconv"
	"testing"
)

func TestScanner(t *testing.T) {
	type test struct {
		src string
		exp []Token
	}

	var tests = []test{
		{`job=test`,
			[]Token{
				Token{Type: 4, Line: 1, Text: "job"},
				Token{Type: 5, Line: 1, Text: "="},
				Token{Type: 4, Line: 1, Text: "test"}}},
		{`job!=test`,
			[]Token{
				Token{Type: 4, Line: 1, Text: "job"},
				Token{Type: 5, Line: 1, Text: "!="},
				Token{Type: 4, Line: 1, Text: "test"}}},
		{`job=~test`,
			[]Token{
				Token{Type: 4, Line: 1, Text: "job"},
				Token{Type: 5, Line: 1, Text: "=~"},
				Token{Type: 4, Line: 1, Text: "test"}}},
		{`job="test"`,
			[]Token{
				Token{Type: 4, Line: 1, Text: "job"},
				Token{Type: 5, Line: 1, Text: "="},
				Token{Type: 3, Line: 1, Text: "\"test\""}}},
	}

	for i, st := range tests {
		func(st test) {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				s := newScanner(bytes.NewBuffer([]byte(st.src)))

				ts := []Token{}
				for {
					l := s.Next()
					if l.Type == EOF {
						break
					}
					ts = append(ts, l)
				}
				if !reflect.DeepEqual(st.exp, ts) {
					t.Fatalf("\nexpected: %#v\ngot: %#v", st.exp, ts)
				}
			})
		}(st)
	}
}
