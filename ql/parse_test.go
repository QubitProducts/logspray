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
	"fmt"
	"strconv"
	"testing"
)

func TestParser(t *testing.T) {
	type test struct {
		src string
		exp string
		err string
	}

	var tests = []test{
		{`job=thing`, `[{job = thing}]`, ``},
		{`job = thing`, `[{job = thing}]`, ``},
		{`job = "thing"`, `[{job = thing}]`, ``},
		{`"job" = "thing"`, `[{job = thing}]`, ``},
		{`job!="thing"`, `[{job != thing}]`, ``},
		{`job ~ thing`, `[{job ~ thing}]`, ``},
		{`job !~ thing`, `[{job !~ thing}]`, ``},
		{`job !~ thing other = more`, `[{job !~ thing} {other = more}]`, ``},
		{`job !~ thing other`, `[]`, `expected label name, got EOF`},
		{`job == thing`, `[]`, `unknown operator, got "=="`},
	}

	for i, st := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s := newScanner(bytes.NewBuffer([]byte(st.src)))
			p := newParser(s)

			t0, err := p.readQueryTerms()
			if err != nil && st.err != err.Error() {
				t.Fatalf("\nexpected err: %#v\ngot: %#v", st.err, err.Error())
			}
			if err == nil && st.err != "" {
				t.Fatalf("\nexpected err:  %#v", st.err)
			}

			str := fmt.Sprintf("%v", t0)
			if str != st.exp {
				t.Fatalf("\nexpected: %#v\ngot: %#v", st.exp, str)
			}
		})
	}
}
