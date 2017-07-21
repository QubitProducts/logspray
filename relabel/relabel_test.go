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

package relabel

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/golang/protobuf/ptypes"

	yaml "gopkg.in/yaml.v2"
)

func TestRuleRegex_Marshal(t *testing.T) {
	test := struct {
		R *JSONRegexp
	}{
		&JSONRegexp{regexp.MustCompile("(.+)")},
	}
	got, err := json.Marshal(test)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"R":"(.+)"}`
	if string(got) != expected {
		t.Fatalf("bad marshal, expected = %q ; got = %q", expected, string(got))
	}
}

func TestRuleRegex_Unmarshal_Fail(t *testing.T) {
	test := []byte(`{"R":"(.+"}`)
	expect := "error parsing regexp: missing closing ): `(.+`"

	str := struct {
		R *JSONRegexp
	}{}

	err := json.Unmarshal(test, &str)
	if err == nil {
		t.Fatalf("Unmarshal of %q should fail", string(test))
	}

	if err.Error() != expect {
		t.Fatalf("Unmarshal should fail with err = %q , got err = %q", expect, err.Error())
	}
}

func TestRuleRegex_Unmarshal(t *testing.T) {
	test := []byte(`{"R":"(.+)"}`)
	expect := `(.+)`

	str := struct {
		R *JSONRegexp
	}{}

	err := json.Unmarshal(test, &str)
	if err != nil {
		t.Fatal(err)
	}

	if str.R == nil {
		t.Fatal("Unarshal gave nil regexp")
	}

	got := str.R.String()
	if got != "(.+)" {
		t.Fatalf("Wrong regexp returned; expected = %q , got = %q", expect, got)
	}
}

func TestRuleFunc_Marshal(t *testing.T) {
	test := struct {
		F ruleFunc
	}{
		(*Rule).applyKeep,
	}
	got, err := json.Marshal(test)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"F":"keep"}`
	if string(got) != expected {
		t.Fatalf("bad marshal, expected = %q ; got = %q", expected, string(got))
	}
}

func TestRuleFunc_Unmarshal(t *testing.T) {
	test := []byte(`{"F":"keep"}`)
	expect := actions["keep"]

	str := struct {
		F ruleFunc
	}{}

	err := json.Unmarshal(test, &str)
	if err != nil {
		t.Fatal(err)
	}

	if getFuncName(str.F) != getFuncName(expect) {
		t.Fatalf("Unmarhsl ruleFunc failed, got = %v", getFuncName(str.F))
	}

	if str.F == nil {
		t.Fatal("Unmarshal action failed")
	}
}

func TestRuleFunc_Unmarshal_Fail(t *testing.T) {
	test := []byte(`{"F":"unknown"}`)
	expect := `unkown relabel action "unknown"`

	str := struct {
		F ruleFunc
	}{}

	err := json.Unmarshal(test, &str)
	if err == nil {
		t.Fatal("Expected unmarhal of unknown action should fail")
	}

	if err.Error() != expect {
		t.Fatalf("Invorrect unmarshal error, expected = %q , got = %q", expect, err.Error())
	}
}

func TestRulesConfig_UnmarhalYAML(t *testing.T) {
	test := `
- action: "keep"
  source_labels:
  - job
  target_label: job
  regex: (.+)`

	res := []Rule{
		{
			Action: actions["drop"],
		},
	}
	err := yaml.Unmarshal([]byte(test), &res)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRulesConfig_UnmarhalYAML_UnkownField(t *testing.T) {
	test := `
- action: "keep"
  nosuch: "blah"
  regex: (.+)`

	res := []Rule{}
	if err := yaml.Unmarshal([]byte(test), &res); err == nil {
		t.Fatal("Expected error from rule config using unknown field")
	}
}

func TestRulesConfig_MarshalYAML(t *testing.T) {
	str := []Rule{
		{
			Action: actions["drop"],
			Regex:  &JSONRegexp{regexp.MustCompile("(.+)")},
		},
	}
	expect := "- action: '\"drop\"'\n  source_labels: []\n  target_label: \"\"\n  regex: '\"(.+)\"'\n  replacement: \"\"\n  separator: \"\"\n"

	bs, err := yaml.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}

	if string(bs) != expect {
		t.Fatalf("YAML marshal failed, expected = %q , got = %q", expect, string(bs))
	}
}

func testFailAction(*Rule, *logspray.Message) bool {
	return false
}
func TestRulesConfig_MarshalYAML_Fail(t *testing.T) {
	str := []Rule{
		{
			Action: testFailAction,
			Regex:  &JSONRegexp{regexp.MustCompile("(.+)")},
		},
	}

	_, err := yaml.Marshal(str)
	if err == nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(err.Error(), "testFailAction") {
		t.Fatal("Expected error for unsmarshal of unknown function")
	}
}

func TestApplyRules(t *testing.T) {
	tests := []struct {
		*Rule
		*logspray.Message
		expect       bool
		expectLabels map[string]string
	}{
		{
			Rule: &Rule{
				Action:    (*Rule).applyKeep,
				SrcLabels: []string{"mylabel"},
				Regex:     &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator: ";",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel": "a value",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"mylabel": "a value",
			},
		},
		{
			Rule: &Rule{
				Action:    (*Rule).applyDrop,
				SrcLabels: []string{"mylabel"},
				Regex:     &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator: ";",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel": "a value",
				},
			},
			expect: false,
			expectLabels: map[string]string{
				"mylabel": "a value",
			},
		},
		{
			Rule: &Rule{
				Action:    (*Rule).applyDrop,
				SrcLabels: []string{"mylabel2"},
				Regex:     &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator: ";",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel": "a value",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"mylabel": "a value",
			},
		},
		{
			Rule: &Rule{
				Action:    (*Rule).applyKeep,
				SrcLabels: []string{"mylabel"},
				Regex:     &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator: ";",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel2": "other",
				},
			},
			expect: false,
			expectLabels: map[string]string{
				"mylabel2": "other",
			},
		},
		{
			Rule: &Rule{
				Action:    (*Rule).applyKeep,
				SrcLabels: []string{"mylabel"},
				Regex:     &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator: ";",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel":  "a value",
					"mylabel2": "other",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"mylabel":  "a value",
				"mylabel2": "other",
			},
		},
		{
			Rule: &Rule{
				Action:    (*Rule).applyKeep,
				SrcLabels: []string{"mylabel", "otherlabel"},
				Regex:     &JSONRegexp{regexp.MustCompile("a value;other")},
				Separator: ";",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel":    "a value",
					"otherlabel": "other",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"mylabel":    "a value",
				"otherlabel": "other",
			},
		},
		{
			Rule: &Rule{
				Action: (*Rule).applyLabelKeep,
				Regex:  &JSONRegexp{regexp.MustCompile("mylabel")},
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel":    "a value",
					"otherlabel": "other",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"mylabel": "a value",
			},
		},
		{
			Rule: &Rule{
				Action: (*Rule).applyLabelDrop,
				Regex:  &JSONRegexp{regexp.MustCompile("mylabel")},
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel":    "a value",
					"otherlabel": "other",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"otherlabel": "other",
			},
		},
		{
			Rule: &Rule{
				Action:      (*Rule).applyLabelMap,
				Regex:       &JSONRegexp{regexp.MustCompile("^mylabel_(.+)$")},
				Replacement: "$1",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel":        "a value",
					"otherlabel":     "other",
					"mylabel_thing1": "move me",
					"mylabel_thing2": "move me too",
				},
			},
			expect: true,
			expectLabels: map[string]string{
				"mylabel":        "a value",
				"otherlabel":     "other",
				"mylabel_thing1": "move me",
				"mylabel_thing2": "move me too",
				"thing1":         "move me",
				"thing2":         "move me too",
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", t.Name(), i), func(t *testing.T) {
			if got := tt.Relabel(tt.Message); got != tt.expect {
				t.Fatalf("match failed, expected = %v , got = %v", tt.expect, got)
			}

			if !reflect.DeepEqual(tt.Message.Labels, tt.expectLabels) {
				t.Fatalf("match failed, expected labels = %v , got = %v", tt.expectLabels, tt.Message.Labels)
			}
		})
	}
}

func BenchmarkRulesConfigKeep(b *testing.B) {
	test := logspray.Message{
		Labels: map[string]string{
			"mylabel": "a value",
		},
	}
	match := Rule{
		Action:    (*Rule).applyKeep,
		SrcLabels: []string{"mylabel"},
		Regex:     &JSONRegexp{regexp.MustCompile("(.+)")},
		Separator: ";",
	}

	for i := 0; i < b.N; i++ {
		match.Relabel(&test)
	}
}

func TestReplace(t *testing.T) {
	tests := []struct {
		*Rule
		*logspray.Message
		expect          bool
		expectLabelName string
		expectLabelVal  string
		expectLabelOK   bool
	}{
		{
			Rule: &Rule{
				Action:      (*Rule).applyReplace,
				SrcLabels:   []string{"mylabel"},
				TargetLabel: "mylabel2",
				Regex:       &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator:   ";",
				Replacement: "$1",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"mylabel": "a value",
				},
			},
			expect:          true,
			expectLabelName: "mylabel2",
			expectLabelVal:  "a value",
			expectLabelOK:   true,
		},
		{
			Rule: &Rule{
				Action:      (*Rule).applyReplace,
				SrcLabels:   []string{"mylabel"},
				TargetLabel: "mylabel2",
				Regex:       &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator:   ";",
				Replacement: "$1",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"otherlabel": "a value",
				},
			},
			expect:          true,
			expectLabelName: "mylabel2",
			expectLabelVal:  "a value",
			expectLabelOK:   false,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", t.Name(), i), func(t *testing.T) {
			if got := tt.Relabel(tt.Message); got != tt.expect {
				t.Fatalf("match failed, expected = %v , got = %v", tt.expect, got)
			}

			lval, lok := tt.Message.Labels[tt.expectLabelName]
			if lok != tt.expectLabelOK {
				if tt.expectLabelOK {
					t.Fatalf("expected label %s not present", tt.expectLabelName)
				} else {
					t.Fatalf("unexpected label %s present", tt.expectLabelName)
				}
			}
			if lok && lval != tt.expectLabelVal {
				t.Fatalf("expected label has wrong value, wanted = %q , got = %q", tt.expectLabelVal, lval)
			}
		})
	}
}

func TestRulesConfig_Relabel(t *testing.T) {
	tests := []struct {
		cfg          string
		parse        bool
		expect       bool
		expectLabels map[string]string
	}{
		{
			cfg: `
- action: "keep"
  source_labels:
  regex: (.+`,
			parse:        false,
			expectLabels: map[string]string{},
		},
		{
			cfg: `
- action: "broken"
  source_labels:
  - job`,
			parse:        false,
			expectLabels: map[string]string{},
		},
		{
			cfg: `
- action: "broken"
  source_labels:
  - job`,
			parse:        false,
			expectLabels: map[string]string{},
		},
		{
			cfg: `
- action: "replace"
  target_label: "label2"
  source_labels:
  - job`,
			parse:  true,
			expect: true,
			expectLabels: map[string]string{
				"job":    "magicstuff",
				"env":    "staging",
				"lab1":   "val1",
				"lab2":   "val2",
				"label2": "magicstuff",
			},
		},
		{
			cfg: `
- action: "keep"
  regex: faveLabel
  source_labels:
  - job`,
			parse:  true,
			expect: false,
			expectLabels: map[string]string{
				"job":  "magicstuff",
				"env":  "staging",
				"lab1": "val1",
				"lab2": "val2",
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", t.Name(), i), func(t *testing.T) {
			res := Config{}
			if err := yaml.Unmarshal([]byte(tt.cfg), &res); err != nil && tt.parse {
				t.Fatalf("unexpected parse error = %q", err)
			} else {
				if err == nil && !tt.parse {
					t.Fatalf("expected parse error got nil")
				}
			}
			if !tt.parse {
				return
			}

			msg := logspray.Message{
				Labels: map[string]string{
					"job":  "magicstuff",
					"env":  "staging",
					"lab1": "val1",
					"lab2": "val2",
				},
			}

			got := res.Relabel(&msg)

			if tt.expect != got {
				t.Fatalf("match failed, expected = %v , got = %v", tt.expect, got)
			}

			if !reflect.DeepEqual(msg.Labels, tt.expectLabels) {
				t.Fatalf("Final labels incorrect:\n expect = %#v\n got = %#v\n", tt.expectLabels, msg.Labels)
			}
		})
	}
}

func TestLogfmt(t *testing.T) {
	tests := []struct {
		*Rule
		*logspray.Message
		expect          bool
		expectLabelName string
		expectLabelVal  string
		expectLabelOK   bool
	}{
		{
			Rule: &Rule{
				Action:      (*Rule).applyLogfmt,
				SrcLabels:   []string{"text"},
				Regex:       &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator:   ";",
				Replacement: "$1",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"text": `hello=world other="some things"`,
				},
			},
			expect:          true,
			expectLabelName: "other",
			expectLabelVal:  "some things",
			expectLabelOK:   true,
		},
		{
			Rule: &Rule{
				Action:      (*Rule).applyLogfmt,
				SrcLabels:   []string{"text"},
				Regex:       &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator:   ";",
				Replacement: "$1",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"text": `hello=world other="some things`,
				},
			},
			expect: false,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", t.Name(), i), func(t *testing.T) {
			if got := tt.Relabel(tt.Message); got != tt.expect {
				t.Fatalf("match failed, expected = %v , got = %v", tt.expect, got)
			}

			lval, lok := tt.Message.Labels[tt.expectLabelName]
			if lok != tt.expectLabelOK {
				if tt.expectLabelOK {
					t.Fatalf("expected label %s not present", tt.expectLabelName)
				} else {
					t.Fatalf("unexpected label %s present", tt.expectLabelName)
				}
			}
			if lok && lval != tt.expectLabelVal {
				t.Fatalf("expected label has wrong value, wanted = %q , got = %q", tt.expectLabelVal, lval)
			}
		})
	}
}

func TestStrptime(t *testing.T) {
	msgtime, _ := ptypes.TimestampProto(time.Unix(1, 0))
	t1 := time.Now()

	tests := []struct {
		*Rule
		*logspray.Message
		expect     bool
		expectTime time.Time
	}{
		{
			Rule: &Rule{
				Action:      (*Rule).applyStrptime,
				SrcLabels:   []string{"maybetime"},
				Regex:       &JSONRegexp{regexp.MustCompile("(.+)")},
				Separator:   ";",
				TargetLabel: time.RFC3339Nano,
				Replacement: "$1",
			},
			Message: &logspray.Message{
				Labels: map[string]string{
					"maybetime": t1.Format(time.RFC3339Nano),
				},
				Time: msgtime,
			},
			expect:     true,
			expectTime: t1,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", t.Name(), i), func(t *testing.T) {
			if got := tt.Relabel(tt.Message); got != tt.expect {
				t.Fatalf("match failed, expected = %v , got = %v", tt.expect, got)
			}

			mtime, err := ptypes.Timestamp(tt.Message.Time)
			if err != nil {
				t.Fatalf("failed reading message timestampt, err = %v", err)
			}

			if mtime.UnixNano() != tt.expectTime.UnixNano() {
				t.Fatalf("message has wrong timestampt, wanted = %v , got = %v", tt.expectTime, mtime)
			}
		})
	}
}
