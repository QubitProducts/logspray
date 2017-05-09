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
	"runtime"
	"strings"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

// Config is a collection of rules for updating the labels on a message.
type Config []*Rule

// Relabel transforms the labels on message m using the set of relabel rules.
func (rlc Config) Relabel(m *logspray.Message) bool {
	if m.Labels == nil {
		m.Labels = make(map[string]string, 1)
	}
	m.Labels["__text__"] = m.Text

	for _, r := range rlc {
		if !r.Relabel(m) {
			m.Text = m.Labels["__text__"]
			delete(m.Labels, "__text__")
			return false
		}
	}

	m.Text = m.Labels["__text__"]
	delete(m.Labels, "__text__")
	return true
}

type ruleFunc func(*Rule, *logspray.Message) bool

// XXX catches unknown Rule settings
type XXX map[string]interface{}

// Rule describes configuraiton for a rule to relabel a message.
type Rule struct {
	Action      ruleFunc    `json:"action" yaml:"action"`
	SrcLabels   []string    `json:"source_labels" yaml:"source_labels"`
	TargetLabel string      `json:"target_label" yaml:"target_label"`
	Regex       *JSONRegexp `json:"regex" yaml:"regex"`
	Replacement string      `json:"replacement" yaml:"replacement"`
	Separator   string      `json:"separator" yaml:"separator"`
	XXX         `json:",omitempty" yaml:",omitempty,inline"`
}

func defaultRule() Rule {
	return Rule{
		Action:      actions["keep"],
		Regex:       &JSONRegexp{regexp.MustCompile("(.+)")},
		Replacement: "$1",
		SrcLabels:   []string{"job"},
		TargetLabel: "job",
		Separator:   ";",
	}
}

type defdRelabelRule Rule

// UnmarshalYAML unmarshals yaml to a Relabel rule with appropriate defaults
func (r *Rule) UnmarshalYAML(unmarshal func(interface{}) error) error {
	rr := defdRelabelRule(defaultRule())
	if err := unmarshal(&rr); err != nil {
		return err
	}
	if len(rr.XXX) != 0 {
		unknowns := []string{}
		for k := range rr.XXX {
			unknowns = append(unknowns, k)
		}
		return fmt.Errorf("Unknown rule fields: %s", strings.Join(unknowns, ", "))
	}
	*r = Rule(rr)
	return nil
}

// UnmarshalJSON unmarshals yaml to a Relabel rule with appropriate defaults
func (r *Rule) UnmarshalJSON(bs []byte) error {
	rr := defdRelabelRule(defaultRule())
	if err := json.Unmarshal(bs, &rr); err != nil {
		return err
	}
	*r = Rule(rr)
	return nil
}

func (r *Rule) buildKey(m *logspray.Message) string {
	key := ""
	for i, k := range r.SrcLabels {
		if v, ok := m.Labels[k]; ok {
			if i == len(r.SrcLabels)-1 {
				key += v
			} else {
				key += v + r.Separator
			}
		}
	}
	return key
}

// Relabel the provided message using the described rule.
func (r *Rule) Relabel(m *logspray.Message) bool {
	return r.Action(r, m)
}

var actions = map[string]ruleFunc{
	"keep":      (*Rule).applyKeep,
	"drop":      (*Rule).applyDrop,
	"labelkeep": (*Rule).applyLabelKeep,
	"labeldrop": (*Rule).applyLabelDrop,
	"replace":   (*Rule).applyReplace,
	"labelmap":  (*Rule).applyLabelMap,
}

func (r *Rule) applyDrop(m *logspray.Message) bool {
	key := r.buildKey(m)
	return !r.Regex.Match([]byte(key))
}

func (r *Rule) applyKeep(m *logspray.Message) bool {
	key := r.buildKey(m)
	return r.Regex.Match([]byte(key))
}

func (r *Rule) applyLabelDrop(m *logspray.Message) bool {
	for k := range m.Labels {
		if r.Regex.Match([]byte(k)) {
			delete(m.Labels, k)
		}
	}
	return true
}

func (r *Rule) applyLabelKeep(m *logspray.Message) bool {
	for k := range m.Labels {
		if !r.Regex.Match([]byte(k)) {
			delete(m.Labels, k)
		}
	}
	return true
}

func (r *Rule) applyReplace(m *logspray.Message) bool {
	key := r.buildKey(m)
	matches := r.Regex.FindStringSubmatchIndex(key)
	if matches == nil {
		return true
	}
	target := model.LabelName(r.Regex.ExpandString([]byte{}, r.TargetLabel, key, matches))
	m.Labels[string(target)] = string(r.Regex.ExpandString([]byte{}, r.Replacement, key, matches))
	return true
}

func getFuncName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func (r *Rule) applyLabelMap(m *logspray.Message) bool {
	ls := make(map[string]string, len(m.Labels))
	for k, v := range m.Labels {
		ls[k] = v
		if r.Regex.Match([]byte(k)) {
			nl := r.Regex.ReplaceAllString(k, r.Replacement)
			ls[nl] = v
		}
	}
	m.Labels = ls
	return true
}

func (r ruleFunc) MarshalYAML() (interface{}, error) {
	bs, err := r.MarshalJSON()
	return string(bs), err
}

func (r ruleFunc) MarshalJSON() ([]byte, error) {
	for a, f := range actions {
		if getFuncName(f) == getFuncName(r) {
			return json.Marshal(a)
		}
	}

	return nil, errors.Errorf("No name known for relabel functoin %s", getFuncName(r))
}

func (r *ruleFunc) UnmarshalYAML(unmarshal func(interface{}) error) error {
	str := ""
	if err := unmarshal(&str); err != nil {
		return err
	}

	jstr := fmt.Sprintf("%q", str)
	return r.UnmarshalJSON([]byte(jstr))
}

func (r *ruleFunc) UnmarshalJSON(bs []byte) error {
	rstr := ""
	if err := json.Unmarshal(bs, &rstr); err != nil {
		return err
	}
	rf, ok := actions[rstr]
	if !ok {
		return errors.Errorf("unkown relabel action %q", rstr)
	}
	*r = rf
	return nil
}

// JSONRegexp provides a means of directly unmarshaling a regexp
type JSONRegexp struct {
	*regexp.Regexp
}

// MarshalYAML implements the yaml Marshaler interface for JSON Regex
func (r *JSONRegexp) MarshalYAML() (interface{}, error) {
	bs, err := r.MarshalJSON()
	return string(bs), err
}

// MarshalJSON implements the yaml Marshaler interface for JSON Regex
func (r *JSONRegexp) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", r.Regexp.String())), nil
}

// UnmarshalYAML implements the yaml Unmarshaler interface for JSON Regex
func (r *JSONRegexp) UnmarshalYAML(unmarshal func(interface{}) error) error {
	str := ""
	if err := unmarshal(&str); err != nil {
		return err
	}
	jstr := fmt.Sprintf("%q", str)
	return r.UnmarshalJSON([]byte(jstr))
}

// UnmarshalJSON implements the yaml Unmarshaler interface for JSON Regex
func (r *JSONRegexp) UnmarshalJSON(bs []byte) error {
	rstr := ""
	if err := json.Unmarshal(bs, &rstr); err != nil {
		return err
	}
	re, err := regexp.Compile(rstr)
	if err != nil {
		return err
	}
	*r = JSONRegexp{re}
	return nil
}
