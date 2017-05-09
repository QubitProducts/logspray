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

package logspray

import "strings"

// MatchFunc describes a function that can be used to
// accept/reject messages.
type MatchFunc func(hdr, m *Message) bool

func makeLabelMatch(label, value string) (MatchFunc, error) {
	return func(hdr, m *Message) bool {
		if lv, ok := m.Labels[label]; ok && lv == value {
			return true
		}
		if lv, ok := hdr.Labels[label]; ok && lv == value {
			return true
		}
		return false
	}, nil
}

func makeConjunctionMatch(fs ...MatchFunc) (MatchFunc, error) {
	return func(hdr, m *Message) bool {
		res := true
		for i := range fs {
			res = fs[i](hdr, m)
			if res == false {
				return res
			}
		}
		return res
	}, nil
}

func makeDisjunctionMatch(fs ...MatchFunc) (MatchFunc, error) {
	return func(hdr, m *Message) bool {
		res := false
		for i := range fs {
			res = fs[i](hdr, m)
			if res == true {
				return res
			}
		}
		return res
	}, nil
}

// Compile qstr to a matching function
func Compile(qstr string) (MatchFunc, error) {
	qs := strings.Split(qstr, " ")

	terms := []MatchFunc{}

	labelMatches := map[string][]MatchFunc{}
	for _, a := range qs {
		ts := strings.SplitN(a, "=", 2)
		if len(ts) != 2 {
			continue
		}
		currts := labelMatches[ts[0]]
		mf, _ := makeLabelMatch(ts[0], ts[1])
		labelMatches[ts[0]] = append(currts, mf)
	}

	for _, lqs := range labelMatches {
		lq, _ := makeDisjunctionMatch(lqs...)
		terms = append(terms, lq)
	}

	return makeConjunctionMatch(terms...)
}

/*
// Compile returns a function that checks if the given query
// matches the given message
func (q *Query) Compile() (MatchFunc, error) {
	var err error
	lres := map[string][]*regexp.Regexp{}
	for ql, qrestrs := range q.ReLabels {
		for _, qrestr := range qrestrs.Values {
			re, err := regexp.Compile(qrestr)
			if err != nil {
				return nil, err
			}

			lres[ql] = append(lres[ql], re)
		}
	}

	var tre *regexp.Regexp
	var treMatch bool

	if q.Grep != nil && q.Grep.Regex != "" {
		tre, err = regexp.Compile(q.Grep.Regex)
		if err != nil {
			return nil, err
		}
		treMatch = !q.Grep.Negate
	}

	return func(hdr, m *Message) bool {
	MatchLoop:
		for k, qvs := range q.Labels {
			var hok, mok bool
			var hv, mv string
			if hdr != nil {
				hv, hok = hdr.Labels[k]
			}
			if m != nil {
				mv, mok = m.Labels[k]
			}

			if !mok && !hok {
				return false
			}

			for _, qv := range qvs.Values {
				if (qv == mv || qv == hv) || qv == "*" {
					continue MatchLoop
				}
			}
			return false
		}

	REMatchLoop:
		for k, vres := range lres {
			var hok, mok bool
			var hv, mv string
			if hdr != nil {
				hv, hok = hdr.Labels[k]
			}
			if m != nil {
				mv, mok = m.Labels[k]
			}

			if !mok && !hok {
				return false
			}

			for _, vre := range vres {
				if vre.MatchString(hv) || vre.MatchString(mv) {
					continue REMatchLoop
				}
			}
			return false
		}

		if tre != nil && tre.MatchString(m.Text) != treMatch {
			return false
		}
		return true
	}, nil
}
*/
