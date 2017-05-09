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

package logspray

import (
	"strconv"
	"testing"
)

func TestQuery_Matches(t *testing.T) {
	tests := []struct {
		q        *Query
		hdr      *Message
		m        *Message
		compiles bool
		res      bool
	}{
		{
			&Query{Labels: map[string]*SubQueryValueSet{}},
			&Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"otherjob",
				}},
			}},
			&Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"myjob",
					"otherjob",
				}},
			}},
			&Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"*",
				}},
			}},
			&Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"*",
				}},
				"instance": &SubQueryValueSet{Values: []string{
					"servername",
				}},
			}},
			&Message{Labels: map[string]string{
				"job": "myjob",
			}},
			&Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"*",
				}},
				"instance": &SubQueryValueSet{Values: []string{
					"someserver",
				}},
			}},
			&Message{Labels: map[string]string{
				"job":      "myjob",
				"instance": "otherserver",
			}},
			&Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"myjob",
				}},
				"instance": &SubQueryValueSet{Values: []string{
					"servername",
					"otherserver",
				}},
			}},
			&Message{Labels: map[string]string{
				"job":      "myjob",
				"instance": "otherserver",
			}},
			&Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			&Query{Labels: map[string]*SubQueryValueSet{
				"job": &SubQueryValueSet{Values: []string{
					"myjob",
					"job2",
				}},
				"instance": &SubQueryValueSet{Values: []string{
					"servername",
				}},
			}},
			&Message{Labels: map[string]string{
				"job":      "job3",
				"instance": "servername",
			}},
			&Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			&Query{
				ReLabels: map[string]*SubQueryValueSet{
					"job": &SubQueryValueSet{Values: []string{
						"+",
					}},
				},
			},
			&Message{Labels: map[string]string{
				"job":      "job3",
				"instance": "servername",
			}},
			&Message{Labels: map[string]string{}},
			false,
			false,
		},
		{
			&Query{
				ReLabels: map[string]*SubQueryValueSet{
					"job": &SubQueryValueSet{Values: []string{
						"jo.+3",
					}},
				},
			},
			&Message{Labels: map[string]string{
				"job":      "job3",
				"instance": "servername",
			}},
			&Message{Labels: map[string]string{}},
			true,
			true,
		},
		{
			&Query{
				ReLabels: map[string]*SubQueryValueSet{
					"jab": &SubQueryValueSet{Values: []string{
						"jo.+3",
					}},
				},
			},
			&Message{Labels: map[string]string{
				"job":      "jxb3",
				"instance": "servername",
			}},
			&Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			&Query{
				ReLabels: map[string]*SubQueryValueSet{
					"job": &SubQueryValueSet{Values: []string{
						"jo.+3",
					}},
				},
			},
			&Message{Labels: map[string]string{
				"job":      "jxb3",
				"instance": "servername",
			}},
			&Message{Labels: map[string]string{}},
			true,
			false,
		},
		{
			&Query{
				Grep: &GrepQuery{
					Regex: "+",
				},
			},
			&Message{Labels: map[string]string{
				"job":      "job3",
				"instance": "servername",
			}},
			&Message{Labels: map[string]string{}},
			false,
			false,
		},
		{
			&Query{
				Grep: &GrepQuery{
					Regex: "jo.+[0-9]",
				},
			},
			&Message{},
			&Message{Text: "job3"},
			true,
			true,
		},
		{
			&Query{
				Grep: &GrepQuery{
					Regex:  "jo.+[0-9]",
					Negate: true,
				},
			},
			&Message{},
			&Message{Text: "job3"},
			true,
			false,
		},
	}

	for i, st := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			mf, err := st.q.Compile()
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
