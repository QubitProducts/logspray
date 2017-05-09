// Copyright 2016 Tristan Colgate-McFarlane
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

import "fmt"

type OpType int

const (
	XFX OpType = iota
	FX
	XFY
	FY
	YFX
	XF
	YF
)

type Op map[OpType]int

type OpSet map[string]Op

// defaultOps is a set of ops totally stolen from SWI, I have
// literally no idea what 90% of these do.
var defaultOps = OpSet{
	"=":  {XFX: 1200, FX: 1200},
	"~":  {XFX: 1200, FX: 1200},
	"!=": {XFX: 1200, FX: 1200},
	"!~": {XFX: 1200, FX: 1200},
	"&":  {XFX: 1200, FX: 1200},
}

// Parser stores the state for the ivy parser.
type Parser struct {
	scanner  *Scanner
	fileName string

	lineNum    int
	errorCount int // Number of errors.

	peekTok Token
	curTok  Token // most recent token from scanner

	operators OpSet
}

// Error provides details of a syntax error
type Error struct {
	err error
	tok Token
}

func (err Error) Error() string {
	return fmt.Sprintf("line %v,  %v", err.tok.Line, err.err)
}

// newParser returns a new parser that will read from the scanner.
// The context must have have been created by this package's NewContext function.
func newParser(scanner *Scanner) *Parser {
	return &Parser{
		scanner: scanner,

		operators: defaultOps, // TODO: Need a deep copy here
	}
}

func (p *Parser) next() Token {
	return p.nextErrorOut(true)
}

// nextErrorOut accepts a flag whether to trigger a panic on error.
// The flag is set to false when we are draining input tokens in FlushToNewline.
func (p *Parser) nextErrorOut(errorOut bool) Token {
	tok := p.peekTok
	if tok.Type != EOF {
		p.peekTok = Token{Type: EOF}
	} else {
		tok = p.scanner.Next()
	}
	if tok.Type == TokError && errorOut {
		panic(fmt.Errorf("%q", tok)) // Need a local output writer
	}
	p.curTok = tok
	if tok.Type != Newline {
		// Show the line number before we hit the newline.
		p.lineNum = tok.Line
	}
	return tok
}

func (p *Parser) peek() Token {
	tok := p.peekTok
	if tok.Type != EOF {
		return tok
	}
	p.peekTok = p.scanner.Next()
	return p.peekTok
}

func (p *Parser) errorf(args ...interface{}) Error {
	return Error{}
}

func (os OpSet) lookup(s string) (Op, bool) {
	ops, ok := os[s]
	if !ok {
		return nil, false
	}
	return ops, true
}

// Infix returns left, op, and right priorities
func (os OpSet) Infix(s string) (int, int, int, bool) {
	o, ok := os.lookup(s)
	if !ok {
		return 0, 0, 0, false
	}

	if opp, ok := o[YFX]; ok {
		return opp, opp, opp - 1, true
	}
	if opp, ok := o[XFY]; ok {
		return opp - 1, opp, opp, true
	}
	if opp, ok := o[XFX]; ok {
		return opp - 1, opp, opp - 1, true
	}
	return 0, 0, 0, false
}

func (os OpSet) Prefix(s string) (int, int, bool) {
	o, ok := os.lookup(s)
	if !ok {
		return 0, 0, false
	}

	if opp, ok := o[FX]; ok {
		return opp, opp - 1, true
	}
	if opp, ok := o[FY]; ok {
		return opp, opp, true
	}
	return 0, 0, false
}
