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
	"fmt"
	"io"
)

type queryTerm struct {
	label    string
	operator string
	value    string
}

type query map[string]map[string][]string

// readTerm reads the next available term, assuming the
// priority of the current term is prio
//
// STR: QA | A
// OP: "=" | "!=" | "=~" | "!~"
// QT: STR OP STR
// QTS:  QTS | QT
func (p *Parser) readQueryTerms() (query, error) {
	var qts []queryTerm
	for {
		qt, err := p.readQueryTerm()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		qts = append(qts, qt)
	}

	return query{}, nil
}

func (p *Parser) readQueryTerm() (queryTerm, error) {
	if p.peek().Type == EOF {
		return queryTerm{}, io.EOF
	}

	lval, err := p.lvalue()
	if err != nil {
		return queryTerm{}, err
	}

	op, err := p.operator()
	if err != nil {
		return queryTerm{}, err
	}

	rval, err := p.rexpr()
	if err != nil {
		return queryTerm{}, err
	}

	return queryTerm{
		label:    lval,
		operator: op,
		value:    rval,
	}, nil
}

func (p *Parser) operator() (string, error) {
	tok := p.next()
	switch tok.Type {
	case SpecialAtom:
	case EOF:
		return "", fmt.Errorf("expected label name, got EOF")
	default:
		return "", fmt.Errorf("expected operator, got %q", tok.Text)
	}

	switch tok.Text {
	case "!=", "=", "!~", "~":
		return tok.Text, nil
	default:
		return "", fmt.Errorf("unknown operator, got %q", tok.Text)
	}
}

func (p *Parser) lvalue() (string, error) {
	tok := p.next()
	switch tok.Type {
	case EOF:
		return "", fmt.Errorf("expected label name, got EOF")
	case QuotedString, Atom:
		return tok.Text, nil
	default:
		return "", fmt.Errorf("expected label name, got %q", tok.Text)
	}
}

func (p *Parser) rexpr() (string, error) {
	tok := p.next()
	switch tok.Type {
	case EOF:
		return "", fmt.Errorf("expected s name, got EOF")
	case QuotedString, Atom:
		return tok.Text, nil
	default:
		return "", fmt.Errorf("expected label expression, got %q", tok.Text)
	}
}
