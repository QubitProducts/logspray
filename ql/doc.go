// Package ql implements support for parsing and running logsprays
// simple query expressions.
//
// Query expressions set of label and value matches. The ql does not currently
// support matching of the message text itself, this will be corrected in future
// Either label or value can be given as a quoted string using ",', or `
// quotes. Four match types are supported:
//
//   = : an exact match, or the single wild card "*" for any value
//   != : any value not equal to the value
//   ~ : A regular expression match, against the label value
//   !~ : A negated regular expression match against the label value
//
// Matches for the same label are or'd together. Matches for different labels
// are and'd together.
package ql
