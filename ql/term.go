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
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

type queryTerm struct {
	label    string
	operator op
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

func (qs query) String() string {
	return fmt.Sprintf("%s", []queryTerm(qs))
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

	opb, err := p.operator()
	if err != nil {
		return queryTerm{}, err
	}

	rval, err := p.string()
	if err != nil {
		return queryTerm{}, err
	}

	op, err := opb(rval)
	return queryTerm{
		label:    lval,
		operator: op,
		value:    rval,
	}, nil
}

func (qs queryTerm) String() string {
	return fmt.Sprintf("{%s %s %s}", qs.label, qs.operator, qs.value)
}

type op interface {
	match(rv string) bool
}

type opBuilder func(lv string) (op, error)

type opSet map[string]opBuilder

// defaultOps is a set of ops totally stolen from SWI, I have
// literally no idea what 90% of these do.
var defaultOps = opSet{
	"=":  buildOpEqual,
	"~":  buildOpMatch,
	"!=": buildOpNotEqual,
	"!~": buildOpNotMatch,
}

type compareOp string

func (lv compareOp) String() string {
	return "="
}

func (lv compareOp) match(rv string) bool {
	return string(lv) == "*" || strings.Compare(string(lv), rv) == 0
}

func buildOpEqual(lv string) (op, error) {
	return compareOp(lv), nil
}

type compareNeOp string

func (lv compareNeOp) String() string {
	return "!="
}
func (lv compareNeOp) match(rv string) bool {
	return strings.Compare(string(lv), rv) != 0
}
func buildOpNotEqual(lv string) (op, error) {
	return compareNeOp(lv), nil
}

type matchOp regexp.Regexp

func (lv *matchOp) String() string {
	return "~"
}
func (lv *matchOp) match(rv string) bool {
	return (*regexp.Regexp)(lv).MatchString(rv)
}
func buildOpMatch(lv string) (op, error) {
	re, err := regexp.Compile(lv)
	if err != nil {
		return nil, errors.Wrap(err, "build regexp match failed")
	}

	return (*matchOp)(re), nil
}

type matchNeOp regexp.Regexp

func (lv *matchNeOp) String() string {
	return "!~"
}
func (lv *matchNeOp) match(rv string) bool {
	return !(*regexp.Regexp)(lv).MatchString(rv)
}
func buildOpNotMatch(lv string) (op, error) {
	re, err := regexp.Compile(lv)
	if err != nil {
		return nil, errors.Wrap(err, "build regexp match failed")
	}

	return (*matchNeOp)(re), nil
}

func (os opSet) lookup(s string) (opBuilder, bool) {
	opb, ok := os[s]
	if !ok {
		return nil, false
	}
	return opb, true
}

// OP: "=" | "!=" | "~" | "!~"
func (p *Parser) operator() (opBuilder, error) {
	tok := p.next()
	switch tok.Type {
	case Operator:
	case EOF:
		return nil, fmt.Errorf("expected label name, got EOF")
	default:
		return nil, fmt.Errorf("expected operator, got %q", tok.Text)
	}

	var opb opBuilder
	var ok bool
	if opb, ok = defaultOps.lookup(tok.Text); !ok {
		return nil, fmt.Errorf("unknown operator, got %q", tok.Text)
	}

	return opb, nil
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
