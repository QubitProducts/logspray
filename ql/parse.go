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

import (
	"fmt"
)

// Parser stores the state for the ivy parser.
type Parser struct {
	scanner  *Scanner
	fileName string

	lineNum    int
	errorCount int // Number of errors.

	peekTok Token
	curTok  Token // most recent token from scanner
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
	}
}

func (p *Parser) next() (Token, error) {
	tok := p.peekTok
	if tok.Type != EOF {
		p.peekTok = Token{Type: EOF}
	} else {
		tok = p.scanner.Next()
	}
	if tok.Type == TokError {
		return tok, fmt.Errorf("error parsing query, %w", tok)
	}
	p.curTok = tok
	if tok.Type != Newline {
		// Show the line number before we hit the newline.
		p.lineNum = tok.Line
	}
	return tok, nil
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
