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

type query []queryTerm

// readTerm reads the next available search term
//
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

	return query(qts), nil
}

// QT: LABEL OP STR
func (p *Parser) readQueryTerm() (queryTerm, error) {
	if p.peek().Type == EOF {
		return queryTerm{}, io.EOF
	}

	lval, err := p.label()
	if err != nil {
		return queryTerm{}, err
	}

	op, err := p.operator()
	if err != nil {
		return queryTerm{}, err
	}

	rval, err := p.string()
	if err != nil {
		return queryTerm{}, err
	}

	return queryTerm{
		label:    lval,
		operator: op,
		value:    rval,
	}, nil
}

type op struct{}

type opSet map[string]*op

// defaultOps is a set of ops totally stolen from SWI, I have
// literally no idea what 90% of these do.
var defaultOps = opSet{
	"=":  {},
	"~":  {},
	"!=": {},
	"!~": {},
}

func (os opSet) lookup(s string) (*op, bool) {
	ops, ok := os[s]
	if !ok {
		return nil, false
	}
	return ops, true
}

// OP: "=" | "!=" | "~" | "!~"
func (p *Parser) operator() (string, error) {
	tok := p.next()
	switch tok.Type {
	case Operator:
	case EOF:
		return "", fmt.Errorf("expected label name, got EOF")
	default:
		return "", fmt.Errorf("expected operator, got %q", tok.Text)
	}

	if _, ok := defaultOps.lookup(tok.Text); !ok {
		return "", fmt.Errorf("unknown operator, got %q", tok.Text)
	}

	return tok.Text, nil
}

// STR: QA | A
func (p *Parser) label() (string, error) {
	tok := p.next()
	switch tok.Type {
	case EOF:
		return "", fmt.Errorf("expected a label name")
	case String:
		return tok.Text[1 : len(tok.Text)-1], nil
	case Atom:
		return tok.Text, nil
	default:
		return "", fmt.Errorf("expected label name, got %q", tok.Text)
	}
}

// STR: QA | A
func (p *Parser) string() (string, error) {
	tok := p.next()
	switch tok.Type {
	case EOF:
		return "", fmt.Errorf("expected a label value")
	case String:
		return tok.Text[1 : len(tok.Text)-1], nil
	case Atom:
		return tok.Text, nil
	default:
		return "", fmt.Errorf("expected label value, got %q", tok.Text)
	}
}
