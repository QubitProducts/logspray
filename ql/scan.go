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
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Token represents a token or text string returned from the scanner.
type Token struct {
	Type Type   // The type of this item.
	Line int    // The line number on which this token appears
	Text string // The text of this item.
}

//go:generate stringer -type Type
// Type identifies the type of lex items.
type Type int

const (
	EOF      Type = iota // zero value so closed channel delivers EOF
	TokError             // error occurred; value is text of error
	Newline
	String   // A quoted string
	Atom     // a bare word
	Operator // Symbol made up of special chars
)

const special = "|&!=~"
const punctuation = "-_.*"

func (i Token) String() string {
	switch {
	case i.Type == EOF:
		return "EOF"
	case i.Type == TokError:
		return "error: " + i.Text
	case len(i.Text) > 20:
		return fmt.Sprintf("%s: %.20q...", i.Type, i.Text)
	}
	return fmt.Sprintf("%s: %q", i.Type, i.Text)
}

const eof = -1

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*Scanner) stateFn

// Scanner holds the state of the scanner.
type Scanner struct {
	tokens     chan Token // channel of scanned items
	r          io.ByteReader
	done       bool
	buf        []byte
	input      string  // the line of text being scanned.
	leftDelim  string  // start of action
	rightDelim string  // end of action
	state      stateFn // the next lexing function to enter
	line       int     // line number in input
	pos        int     // current position in the input
	start      int     // start position of this item
	width      int     // width of last rune read from input

	debug bool
}

// loadLine reads the next line of input and stores it in (appends it to) the input.
// (l.input may have data left over when we are called.)
// It strips carriage returns to make subsequent processing simpler.
func (l *Scanner) loadLine() {
	l.buf = l.buf[:0]
	for {
		c, err := l.r.ReadByte()
		if err != nil {
			l.done = true
			break
		}
		if c != '\r' {
			l.buf = append(l.buf, c)
		}
		if c == '\n' {
			break
		}
	}
	l.input = l.input[l.start:l.pos] + string(l.buf)
	l.pos -= l.start
	l.start = 0
}

// next returns the next rune in the input.
func (l *Scanner) next() rune {
	if !l.done && int(l.pos) == len(l.input) {
		l.loadLine()
	}
	if len(l.input) == int(l.pos) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *Scanner) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (l *Scanner) backup() {
	l.pos -= l.width
}

//  passes an item back to the client.
func (l *Scanner) emit(t Type) {
	if t == Newline {
		l.line++
	}
	s := l.input[l.start:l.pos]
	if l.debug {
		fmt.Fprintf(os.Stderr, "%d: emit %s\n", l.line, Token{t, l.line, s})
	}
	l.tokens <- Token{t, l.line, s}
	l.start = l.pos
	l.width = 0
}

// ignore skips over the pending input before this point.
func (l *Scanner) ignore() {
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
func (l *Scanner) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *Scanner) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
}

// errorf returns an error token and continues to scan.
func (l *Scanner) errorf(format string, args ...interface{}) stateFn {
	l.tokens <- Token{TokError, l.start, fmt.Sprintf(format, args...)}
	return lexAny
}

// new creates a new scanner for the input string.
func newScanner(r io.ByteReader) *Scanner {
	l := &Scanner{
		r:      r,
		line:   1,
		tokens: make(chan Token, 2), // We need a little room to save tokens.
		state:  lexAny,
	}
	return l
}

// Next returns the next token.
func (l *Scanner) Next() Token {
	// The lexer is concurrent but we don't want it to run in parallel
	// with the rest of the interpreter, so we only run the state machine
	// when we need a token.
	for l.state != nil {
		select {
		case tok := <-l.tokens:
			return tok
		default:
			// Run the machine
			l.state = l.state(l)
		}
	}
	if l.tokens != nil {
		close(l.tokens)
		l.tokens = nil
	}
	return Token{EOF, l.pos, "EOF"}
}

// state functions

// lexAny scans non-space items.
func lexAny(l *Scanner) stateFn {
	switch r := l.next(); {
	case r == eof:
		return nil
	case r == '\n': // TODO: \r
		l.emit(Newline)
		return lexAny
	case isSpace(r):
		return lexSpace
	case r == '"', r == '\'', r == '`':
		return lexQuote
	case isSpecial(r):
		return lexSpecialAtom
	case isAlphaNumeric(r):
		return lexAtom
	default:
		return l.errorf("unrecognized character: %#U", r)
	}
}

// lexSpace scans a run of space characters.
// One space has already been seen.
func lexSpace(l *Scanner) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
	return lexAny
}

// lexQuote scans a quoted string.
func lexQuote(l *Scanner) stateFn {
	c := rune(l.input[l.start])
Loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				break
			}
			fallthrough
		case eof:
			return l.errorf("unterminated quoted string")
		case c:
			break Loop
		}
	}
	l.emit(String)
	return lexAny
}

// lexAtom
func lexAtom(l *Scanner) stateFn {
Loop:
	for {
		c := l.next()
		switch {
		case isAlphaNumeric(c):
		default:
			l.backup()
			break Loop
		}
	}

	l.emit(Atom)
	return lexAny
}

// lexSpecialAtom
func lexSpecialAtom(l *Scanner) stateFn {
Loop:
	for {
		c := l.next()
		switch {
		case isSpecial(c):
		default:
			l.backup()
			break Loop
		}
	}
	l.emit(Operator)
	return lexAny
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or punctuation.
func isAlphaNumeric(r rune) bool {
	return strings.ContainsRune(punctuation, r) ||
		unicode.IsLetter(r) ||
		unicode.IsDigit(r)
}

// isSpecial
func isSpecial(r rune) bool {
	return strings.ContainsRune(special, r)
}

// isDigit reports whether r is an ASCII digit.
func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

// isEndOfLine reports whether r is an end-of-line character.
func isEndOfLine(r rune) bool {
	return r == '\r' || r == '\n'
}
