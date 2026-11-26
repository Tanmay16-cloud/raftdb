// Package parser turns SQL text into an abstract syntax tree (AST).
//
// It works in two stages. The lexer splits the text into tokens: words,
// numbers, strings and symbols. The parser then reads the tokens by
// recursive descent, using precedence climbing (a Pratt parser) for
// expressions.
//
// The parser checks syntax only. Whether tables and columns exist, and
// whether types match, is the planner's job.
package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind int

const (
	tokEOF         tokenKind = iota
	tokWord                  // keyword or unquoted identifier: SELECT, users
	tokQuotedIdent           // "Quoted Name": always an identifier, never a keyword
	tokInt                   // 42
	tokFloat                 // 4.2, 1e9
	tokString                // 'text' (text holds the value, quotes removed)
	tokSymbol                // punctuation and operators: ( , <= ||
)

// Pos is a position in the SQL text. Lines and columns count from 1, and
// columns count characters, not bytes.
type Pos struct{ Line, Col int }

type token struct {
	kind tokenKind
	text string
	pos  Pos
}

// upper is the word in ASCII upper case, for matching keywords. SQL keywords
// are ASCII, so only ASCII letters are folded; this keeps a word like "ſelect"
// (with a long s) from matching SELECT.
func (t token) upper() string { return asciiUpper(t.text) }

// Error is a syntax error and the position where it was found.
type Error struct {
	Pos Pos
	Msg string
}

func (e *Error) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.Pos.Line, e.Pos.Col, e.Msg)
}

type lexer struct {
	src       string
	off       int // byte offset of the next unread character
	line, col int
}

// lex splits src into tokens, ending with a tokEOF token.
func lex(src string) ([]token, error) {
	l := &lexer{src: src, line: 1, col: 1}
	var toks []token
	for {
		tok, err := l.next()
		if err != nil {
			return nil, err
		}
		toks = append(toks, tok)
		if tok.kind == tokEOF {
			return toks, nil
		}
	}
}

// advance moves past the next n bytes, updating the line and column.
func (l *lexer) advance(n int) {
	for _, r := range l.src[l.off : l.off+n] {
		if r == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
	}
	l.off += n
}

func (l *lexer) pos() Pos { return Pos{l.line, l.col} }

var twoCharSymbols = []string{"<=", ">=", "<>", "!=", "||"}

const oneCharSymbols = "(),;.*+-/%=<>"

func (l *lexer) next() (token, error) {
	if err := l.skipSpaceAndComments(); err != nil {
		return token{}, err
	}
	pos := l.pos()
	if l.off >= len(l.src) {
		return token{kind: tokEOF, pos: pos}, nil
	}
	rest := l.src[l.off:]
	r, _ := utf8.DecodeRuneInString(rest)

	switch {
	case r == '\'':
		s, n, ok := scanQuoted(rest)
		if !ok {
			return token{}, &Error{pos, "unterminated string literal"}
		}
		l.advance(n)
		return token{tokString, s, pos}, nil

	case r == '"':
		s, n, ok := scanQuoted(rest)
		if !ok {
			return token{}, &Error{pos, "unterminated quoted identifier"}
		}
		if s == "" {
			return token{}, &Error{pos, "zero-length quoted identifier"}
		}
		l.advance(n)
		return token{tokQuotedIdent, s, pos}, nil

	case isDigit(rest[0]):
		n, isFloat := scanNumber(rest)
		l.advance(n)
		if isFloat {
			return token{tokFloat, rest[:n], pos}, nil
		}
		return token{tokInt, rest[:n], pos}, nil

	case isIdentStart(r):
		n := scanWord(rest)
		l.advance(n)
		return token{tokWord, rest[:n], pos}, nil
	}

	for _, s := range twoCharSymbols {
		if strings.HasPrefix(rest, s) {
			l.advance(2)
			return token{tokSymbol, s, pos}, nil
		}
	}
	if strings.IndexByte(oneCharSymbols, rest[0]) >= 0 {
		l.advance(1)
		return token{tokSymbol, rest[:1], pos}, nil
	}
	return token{}, &Error{pos, fmt.Sprintf("unexpected character %q", r)}
}

func (l *lexer) skipSpaceAndComments() error {
	for l.off < len(l.src) {
		rest := l.src[l.off:]
		r, n := utf8.DecodeRuneInString(rest)
		switch {
		case unicode.IsSpace(r):
			l.advance(n)
		case strings.HasPrefix(rest, "--"):
			end := strings.IndexByte(rest, '\n')
			if end < 0 {
				end = len(rest)
			}
			l.advance(end)
		case strings.HasPrefix(rest, "/*"):
			end := strings.Index(rest[2:], "*/")
			if end < 0 {
				return &Error{l.pos(), "unterminated /* comment"}
			}
			l.advance(end + 4)
		default:
			return nil
		}
	}
	return nil
}

// scanQuoted reads a string quoted with s[0], in which two quote characters
// in a row stand for one. It returns the unquoted value and the number of
// bytes consumed. It works on bytes, not runes, so any bytes inside the
// quotes survive unchanged.
func scanQuoted(s string) (value string, n int, ok bool) {
	q := s[0]
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] != q {
			b.WriteByte(s[i])
			continue
		}
		if i+1 < len(s) && s[i+1] == q {
			b.WriteByte(q)
			i++
			continue
		}
		return b.String(), i + 1, true
	}
	return "", 0, false
}

// scanNumber reads digits [. digits] [e [+-] digits].
func scanNumber(s string) (n int, isFloat bool) {
	digits := func(i int) int {
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		return i
	}
	i := digits(0)
	if i+1 < len(s) && s[i] == '.' && isDigit(s[i+1]) {
		isFloat = true
		i = digits(i + 1)
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && isDigit(s[j]) {
			isFloat = true
			i = digits(j)
		}
	}
	return i, isFloat
}

func scanWord(s string) int {
	n := 0
	for n < len(s) {
		r, size := utf8.DecodeRuneInString(s[n:])
		if !isIdentPart(r) {
			break
		}
		n += size
	}
	return n
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }

func isIdentPart(r rune) bool { return isIdentStart(r) || unicode.IsDigit(r) }

func asciiUpper(s string) string {
	return strings.Map(func(r rune) rune {
		if 'a' <= r && r <= 'z' {
			return r - 'a' + 'A'
		}
		return r
	}, s)
}

// asciiLower folds unquoted identifiers. Like PostgreSQL, it folds only
// ASCII letters.
func asciiLower(s string) string {
	return strings.Map(func(r rune) rune {
		if 'A' <= r && r <= 'Z' {
			return r - 'A' + 'a'
		}
		return r
	}, s)
}
