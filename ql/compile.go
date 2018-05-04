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
// accept/reject messages. If headerOnly is passed, only
// the header message is matched against, and terms involving
// unmatched labels are ignored. headerOnly is used to filter
// files before filtering individual messages.
type MatchFunc func(hdr, m *logspray.Message, headerOnly bool) bool

func makeLabelMatch(op op, label string) (MatchFunc, error) {
	if label == "__text__" {
		return func(hdr, m *logspray.Message, headerOnly bool) bool {
			if headerOnly {
				return true
			}
			return op.match(m.Text)
		}, nil
	}
	return func(hdr, m *logspray.Message, headerOnly bool) bool {
		if rv, ok := hdr.Labels[label]; ok {
			return op.match(rv)
		}
		if headerOnly {
			return true
		}
		if rv, ok := m.Labels[label]; ok {
			return op.match(rv)
		}
		return false
	}, nil
}

func makeConjunctionMatch(fs ...MatchFunc) (MatchFunc, error) {
	return func(hdr, m *logspray.Message, headerOnly bool) bool {
		res := true
		for i := range fs {
			res = fs[i](hdr, m, headerOnly)
			if res == false {
				return res
			}
		}
		return res
	}, nil
}

func makeDisjunctionMatch(fs ...MatchFunc) (MatchFunc, error) {
	return func(hdr, m *logspray.Message, headerOnly bool) bool {
		res := false
		for i := range fs {
			res = fs[i](hdr, m, headerOnly)
			if res == true {
				return res
			}
		}
		return res
	}, nil
}

func Compile(qstr string) (MatchFunc, error) {
	s := newScanner(bytes.NewBuffer([]byte(qstr)))
	p := newParser(s)

	qts, err := p.readQueryTerms()
	if err != nil {
		return nil, errors.Wrap(err, "failed ot read query")
	}

	terms := []MatchFunc{}

	labelMatches := map[string][]MatchFunc{}
	for _, a := range qts {
		currts := labelMatches[a.label]
		mf, err := makeLabelMatch(a.operator, a.label)
		if err != nil {
			return nil, err
		}

		labelMatches[a.label] = append(currts, mf)
	}

	for _, lqs := range labelMatches {
		lq, err := makeDisjunctionMatch(lqs...)
		if err != nil {
			return nil, err
		}

		terms = append(terms, lq)
	}

	return makeConjunctionMatch(terms...)
}
