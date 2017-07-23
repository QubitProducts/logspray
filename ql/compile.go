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

package ql

import (
	"bytes"

	"github.com/QubitProducts/logspray/proto/logspray"
	"github.com/pkg/errors"
)

// MatchFunc describes a function that can be used to
// accept/reject messages.
type MatchFunc func(hdr, m *logspray.Message) bool

func makeLabelMatch(op op, label string) (MatchFunc, error) {
	return func(hdr, m *logspray.Message) bool {
		if rv, ok := m.Labels[label]; ok {
			return op.match(rv)
		}
		if rv, ok := hdr.Labels[label]; ok {
			return op.match(rv)
		}
		return false
	}, nil
}

func makeConjunctionMatch(fs ...MatchFunc) (MatchFunc, error) {
	return func(hdr, m *logspray.Message) bool {
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
	return func(hdr, m *logspray.Message) bool {
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
	s := newScanner(bytes.NewBuffer([]byte(qstr)))
	p := newParser(s)

	qts, err := p.readQueryTerms()
	if err != nil {
		errors.Wrap(err, "failed ot read query")
	}

	terms := []MatchFunc{}

	labelMatches := map[string][]MatchFunc{}
	for _, a := range qts {
		currts := labelMatches[a.label]
		mf, _ := makeLabelMatch(a.operator, a.label)
		labelMatches[a.label] = append(currts, mf)
	}

	for _, lqs := range labelMatches {
		lq, _ := makeDisjunctionMatch(lqs...)
		terms = append(terms, lq)
	}

	return makeConjunctionMatch(terms...)
}
