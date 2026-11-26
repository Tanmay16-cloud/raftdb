package parser

import (
	"fmt"
	"strconv"
)

// reserved words can never be used as bare identifiers; quote them
// ("select") to use them as names.
var reserved = setOf(
	"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
	"DELETE", "CREATE", "DROP", "TABLE", "ON", "NOT", "NULL", "AND", "OR",
	"TRUE", "FALSE", "IS", "IN", "LIKE", "BETWEEN", "ORDER", "GROUP", "BY",
	"HAVING", "LIMIT", "OFFSET", "AS", "JOIN", "INNER", "DISTINCT",
)

// contextual words are keywords only in particular places, so they also
// work as names (a column called "key" is common). quoteIdent still quotes
// them, which keeps canonical output unambiguous.
var contextual = setOf(
	"PRIMARY", "KEY", "INDEX", "UNIQUE", "IF", "EXISTS", "ASC", "DESC",
	"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
)

var keywords = union(reserved, contextual)

func setOf(words ...string) map[string]bool {
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return m
}

func union(a, b map[string]bool) map[string]bool {
	m := make(map[string]bool, len(a)+len(b))
	for k := range a {
		m[k] = true
	}
	for k := range b {
		m[k] = true
	}
	return m
}

// Parse parses exactly one statement. A trailing semicolon is allowed.
func Parse(src string) (Statement, error) {
	stmts, err := ParseAll(src)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, &Error{Pos{1, 1}, fmt.Sprintf("expected one statement, found %d", len(stmts))}
	}
	return stmts[0], nil
}

// ParseAll parses a script of statements separated by semicolons.
func ParseAll(src string) ([]Statement, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	var stmts []Statement
	for {
		for p.acceptSymbol(";") {
		}
		if p.peek().kind == tokEOF {
			return stmts, nil
		}
		s, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
		if p.peek().kind != tokEOF && !p.isSymbol(";") {
			return nil, p.unexpected(`";" or end of input`)
		}
	}
}

type parser struct {
	toks []token
	i    int
}

// ---------------------------------------------------------------------------
// Token helpers

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) peekAt(k int) token {
	if p.i+k >= len(p.toks) {
		return p.toks[len(p.toks)-1] // EOF
	}
	return p.toks[p.i+k]
}

func (p *parser) advance() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}
	return t
}

func isWord(t token, w string) bool { return t.kind == tokWord && t.upper() == w }

func (p *parser) isWord(w string) bool { return isWord(p.peek(), w) }

func (p *parser) acceptWord(w string) bool {
	if p.isWord(w) {
		p.advance()
		return true
	}
	return false
}

func (p *parser) expectWord(w string) error {
	if !p.acceptWord(w) {
		return p.unexpected(w)
	}
	return nil
}

func (p *parser) isSymbol(s string) bool {
	t := p.peek()
	return t.kind == tokSymbol && t.text == s
}

func (p *parser) acceptSymbol(s string) bool {
	if p.isSymbol(s) {
		p.advance()
		return true
	}
	return false
}

func (p *parser) expectSymbol(s string) error {
	if !p.acceptSymbol(s) {
		return p.unexpected(strconv.Quote(s))
	}
	return nil
}

// listEnd consumes the "," or ")" after an item in a parenthesised list and
// reports whether the list is finished.
func (p *parser) listEnd() (done bool, err error) {
	if p.acceptSymbol(",") {
		return false, nil
	}
	if p.acceptSymbol(")") {
		return true, nil
	}
	return false, p.unexpected(`"," or ")"`)
}

func (p *parser) unexpected(want string) error {
	t := p.peek()
	return &Error{t.pos, fmt.Sprintf("expected %s, got %s", want, describe(t))}
}

func describe(t token) string {
	switch t.kind {
	case tokEOF:
		return "end of input"
	case tokWord:
		if keywords[t.upper()] {
			return "keyword " + t.upper()
		}
		return "identifier " + t.text
	case tokQuotedIdent:
		return "identifier " + strconv.Quote(t.text)
	case tokInt, tokFloat:
		return "number " + t.text
	case tokString:
		return "string " + quoteString(t.text)
	}
	return strconv.Quote(t.text)
}

// isIdent reports whether the next token can be used as a name.
func (p *parser) isIdent() bool {
	t := p.peek()
	return t.kind == tokQuotedIdent || (t.kind == tokWord && !reserved[t.upper()])
}

// parseIdent reads a name. Unquoted names are folded to lower case; quoted
// names are kept exactly.
func (p *parser) parseIdent(what string) (string, error) {
	if !p.isIdent() {
		return "", p.unexpected(what)
	}
	t := p.advance()
	if t.kind == tokQuotedIdent {
		return t.text, nil
	}
	return asciiLower(t.text), nil
}

// ---------------------------------------------------------------------------
// Statements

func (p *parser) parseStatement() (Statement, error) {
	switch {
	case p.isWord("SELECT"):
		return p.parseSelect()
	case p.isWord("INSERT"):
		return p.parseInsert()
	case p.isWord("UPDATE"):
		return p.parseUpdate()
	case p.isWord("DELETE"):
		return p.parseDelete()
	case p.isWord("CREATE"):
		return p.parseCreate()
	case p.isWord("DROP"):
		return p.parseDrop()
	case p.acceptWord("BEGIN"):
		p.acceptWord("TRANSACTION")
		return &Begin{}, nil
	case p.acceptWord("COMMIT"):
		return &Commit{}, nil
	case p.acceptWord("ROLLBACK"):
		return &Rollback{}, nil
	}
	return nil, p.unexpected("a statement")
}

// parseIfNotExists reads an optional IF NOT EXISTS. IF is only a keyword
// when NOT follows it, so a table called "if" still works.
func (p *parser) parseIfNotExists() (bool, error) {
	if !p.isWord("IF") || !isWord(p.peekAt(1), "NOT") {
		return false, nil
	}
	p.advance()
	p.advance()
	return true, p.expectWord("EXISTS")
}

func (p *parser) parseIfExists() bool {
	if p.isWord("IF") && isWord(p.peekAt(1), "EXISTS") {
		p.advance()
		p.advance()
		return true
	}
	return false
}

func (p *parser) parseCreate() (Statement, error) {
	p.advance() // CREATE
	if p.acceptWord("TABLE") {
		return p.parseCreateTable()
	}
	unique := p.acceptWord("UNIQUE")
	if p.acceptWord("INDEX") {
		return p.parseCreateIndex(unique)
	}
	if unique {
		return nil, p.unexpected("INDEX")
	}
	return nil, p.unexpected("TABLE or INDEX")
}

func (p *parser) parseCreateTable() (Statement, error) {
	s := &CreateTable{}
	var err error
	if s.IfNotExists, err = p.parseIfNotExists(); err != nil {
		return nil, err
	}
	if s.Name, err = p.parseIdent("table name"); err != nil {
		return nil, err
	}
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		s.Columns = append(s.Columns, col)
		if done, err := p.listEnd(); err != nil {
			return nil, err
		} else if done {
			return s, nil
		}
	}
}

func (p *parser) parseColumnDef() (ColumnDef, error) {
	var col ColumnDef
	var err error
	if col.Name, err = p.parseIdent("column name"); err != nil {
		return col, err
	}
	t := p.peek()
	if t.kind != tokWord {
		return col, p.unexpected("column type")
	}
	typ, ok := typeNames[asciiLower(t.text)]
	if !ok {
		return col, &Error{t.pos, fmt.Sprintf("unknown column type %s (expected INT, FLOAT, TEXT or BOOL)", t.text)}
	}
	p.advance()
	col.Type = typ
	for {
		switch {
		case p.acceptWord("PRIMARY"):
			if err := p.expectWord("KEY"); err != nil {
				return col, err
			}
			col.PrimaryKey = true
		case p.acceptWord("NOT"):
			if err := p.expectWord("NULL"); err != nil {
				return col, err
			}
			col.NotNull = true
		default:
			return col, nil
		}
	}
}

func (p *parser) parseCreateIndex(unique bool) (Statement, error) {
	s := &CreateIndex{Unique: unique}
	var err error
	if s.IfNotExists, err = p.parseIfNotExists(); err != nil {
		return nil, err
	}
	if s.Name, err = p.parseIdent("index name"); err != nil {
		return nil, err
	}
	if err := p.expectWord("ON"); err != nil {
		return nil, err
	}
	if s.Table, err = p.parseIdent("table name"); err != nil {
		return nil, err
	}
	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}
	if s.Column, err = p.parseIdent("column name"); err != nil {
		return nil, err
	}
	if err := p.expectSymbol(")"); err != nil {
		return nil, err
	}
	return s, nil
}

func (p *parser) parseDrop() (Statement, error) {
	p.advance() // DROP
	switch {
	case p.acceptWord("TABLE"):
		s := &DropTable{IfExists: p.parseIfExists()}
		var err error
		s.Name, err = p.parseIdent("table name")
		return s, err
	case p.acceptWord("INDEX"):
		s := &DropIndex{IfExists: p.parseIfExists()}
		var err error
		s.Name, err = p.parseIdent("index name")
		return s, err
	}
	return nil, p.unexpected("TABLE or INDEX")
}

func (p *parser) parseInsert() (Statement, error) {
	p.advance() // INSERT
	if err := p.expectWord("INTO"); err != nil {
		return nil, err
	}
	s := &Insert{}
	var err error
	if s.Table, err = p.parseIdent("table name"); err != nil {
		return nil, err
	}
	if p.acceptSymbol("(") {
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return nil, err
			}
			s.Columns = append(s.Columns, col)
			if done, err := p.listEnd(); err != nil {
				return nil, err
			} else if done {
				break
			}
		}
	}
	if err := p.expectWord("VALUES"); err != nil {
		return nil, err
	}
	for {
		if err := p.expectSymbol("("); err != nil {
			return nil, err
		}
		row, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		s.Rows = append(s.Rows, row)
		if !p.acceptSymbol(",") {
			return s, nil
		}
	}
}

// parseExprList reads "expr, expr, ...)" after an opening parenthesis.
func (p *parser) parseExprList() ([]Expr, error) {
	var list []Expr
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list = append(list, e)
		if done, err := p.listEnd(); err != nil {
			return nil, err
		} else if done {
			return list, nil
		}
	}
}

func (p *parser) parseSelect() (Statement, error) {
	p.advance() // SELECT
	s := &Select{Distinct: p.acceptWord("DISTINCT")}
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		s.Items = append(s.Items, item)
		if !p.acceptSymbol(",") {
			break
		}
	}

	if p.acceptWord("FROM") {
		ref, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		s.From = &ref
	joins:
		for {
			var j Join
			switch {
			case p.acceptSymbol(","): // FROM a, b is a cross join
				if j.Table, err = p.parseTableRef(); err != nil {
					return nil, err
				}
			case p.isWord("JOIN") || p.isWord("INNER"):
				if p.acceptWord("INNER") {
					if err := p.expectWord("JOIN"); err != nil {
						return nil, err
					}
				} else {
					p.advance() // JOIN
				}
				if j.Table, err = p.parseTableRef(); err != nil {
					return nil, err
				}
				if err := p.expectWord("ON"); err != nil {
					return nil, err
				}
				if j.On, err = p.parseExpr(); err != nil {
					return nil, err
				}
			default:
				break joins
			}
			s.Joins = append(s.Joins, j)
		}
	}

	var err error
	if p.acceptWord("WHERE") {
		if s.Where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	if p.acceptWord("GROUP") {
		if err := p.expectWord("BY"); err != nil {
			return nil, err
		}
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			s.GroupBy = append(s.GroupBy, e)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if p.acceptWord("HAVING") {
		if s.Having, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	if p.acceptWord("ORDER") {
		if err := p.expectWord("BY"); err != nil {
			return nil, err
		}
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			item := OrderItem{Expr: e}
			if p.acceptWord("DESC") {
				item.Desc = true
			} else {
				p.acceptWord("ASC")
			}
			s.OrderBy = append(s.OrderBy, item)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if p.acceptWord("LIMIT") {
		if s.Limit, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	if p.acceptWord("OFFSET") {
		if s.Offset, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (p *parser) parseSelectItem() (SelectItem, error) {
	if p.acceptSymbol("*") {
		return SelectItem{Star: true}, nil
	}
	e, err := p.parseExpr()
	if err != nil {
		return SelectItem{}, err
	}
	item := SelectItem{Expr: e}
	if p.acceptWord("AS") || p.isIdent() {
		if item.Alias, err = p.parseIdent("alias"); err != nil {
			return SelectItem{}, err
		}
	}
	return item, nil
}

func (p *parser) parseTableRef() (TableRef, error) {
	var ref TableRef
	var err error
	if ref.Name, err = p.parseIdent("table name"); err != nil {
		return ref, err
	}
	if p.acceptWord("AS") || p.isIdent() {
		if ref.Alias, err = p.parseIdent("alias"); err != nil {
			return ref, err
		}
	}
	return ref, nil
}

func (p *parser) parseUpdate() (Statement, error) {
	p.advance() // UPDATE
	s := &Update{}
	var err error
	if s.Table, err = p.parseIdent("table name"); err != nil {
		return nil, err
	}
	if err := p.expectWord("SET"); err != nil {
		return nil, err
	}
	for {
		var a Assignment
		if a.Column, err = p.parseIdent("column name"); err != nil {
			return nil, err
		}
		if err := p.expectSymbol("="); err != nil {
			return nil, err
		}
		if a.Value, err = p.parseExpr(); err != nil {
			return nil, err
		}
		s.Set = append(s.Set, a)
		if !p.acceptSymbol(",") {
			break
		}
	}
	if p.acceptWord("WHERE") {
		if s.Where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (p *parser) parseDelete() (Statement, error) {
	p.advance() // DELETE
	if err := p.expectWord("FROM"); err != nil {
		return nil, err
	}
	s := &Delete{}
	var err error
	if s.Table, err = p.parseIdent("table name"); err != nil {
		return nil, err
	}
	if p.acceptWord("WHERE") {
		if s.Where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// ---------------------------------------------------------------------------
// Expressions: precedence climbing
//
// Each binary operator has a precedence; higher binds tighter. To parse an
// expression whose operators all have precedence >= min, read one operand,
// then, while the next operator's precedence is >= min, read its right
// operand at (that precedence + 1) and combine. The +1 makes operators of
// equal precedence group to the left: 1 - 2 - 3 is (1 - 2) - 3.

const (
	precOr      = iota + 1 // OR
	precAnd                // AND
	precNot                // prefix NOT
	precCompare            // = != < <= > >= IS IN LIKE BETWEEN
	precConcat             // ||
	precAdd                // + -
	precMul                // * / %
	precUnary              // prefix -
)

func (p *parser) parseExpr() (Expr, error) { return p.parseBinary(precOr) }

func (p *parser) parseBinary(min int) (Expr, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}
	for {
		prec := p.infixPrec()
		if prec == 0 || prec < min {
			return left, nil
		}
		if left, err = p.parseInfix(left, prec); err != nil {
			return nil, err
		}
	}
}

// infixPrec returns the precedence of the operator at the current token, or
// 0 if it is not an infix operator.
func (p *parser) infixPrec() int {
	t := p.peek()
	switch t.kind {
	case tokSymbol:
		switch t.text {
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			return precCompare
		case "||":
			return precConcat
		case "+", "-":
			return precAdd
		case "*", "/", "%":
			return precMul
		}
	case tokWord:
		switch t.upper() {
		case "OR":
			return precOr
		case "AND":
			return precAnd
		case "IS", "IN", "LIKE", "BETWEEN":
			return precCompare
		case "NOT": // NOT IN, NOT LIKE, NOT BETWEEN
			if next := p.peekAt(1); isWord(next, "IN") || isWord(next, "LIKE") || isWord(next, "BETWEEN") {
				return precCompare
			}
		}
	}
	return 0
}

func (p *parser) parseInfix(left Expr, prec int) (Expr, error) {
	t := p.advance()
	if t.kind == tokSymbol {
		op := t.text
		if op == "<>" {
			op = "!="
		}
		right, err := p.parseBinary(prec + 1)
		if err != nil {
			return nil, err
		}
		return &Binary{Op: op, Left: left, Right: right}, nil
	}

	switch t.upper() {
	case "OR", "AND":
		right, err := p.parseBinary(prec + 1)
		if err != nil {
			return nil, err
		}
		return &Binary{Op: t.upper(), Left: left, Right: right}, nil
	case "IS":
		not := p.acceptWord("NOT")
		if err := p.expectWord("NULL"); err != nil {
			return nil, err
		}
		return &IsNull{X: left, Not: not}, nil
	case "NOT":
		return p.parsePredicate(left, p.advance().upper(), true)
	default:
		return p.parsePredicate(left, t.upper(), false)
	}
}

// parsePredicate parses the rest of X [NOT] IN/LIKE/BETWEEN ...
func (p *parser) parsePredicate(left Expr, op string, not bool) (Expr, error) {
	switch op {
	case "IN":
		if err := p.expectSymbol("("); err != nil {
			return nil, err
		}
		list, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		return &In{X: left, List: list, Not: not}, nil
	case "LIKE":
		right, err := p.parseBinary(precCompare + 1)
		if err != nil {
			return nil, err
		}
		if not {
			return &Binary{Op: "NOT LIKE", Left: left, Right: right}, nil
		}
		return &Binary{Op: "LIKE", Left: left, Right: right}, nil
	default: // BETWEEN
		low, err := p.parseBinary(precCompare + 1)
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("AND"); err != nil {
			return nil, err
		}
		high, err := p.parseBinary(precCompare + 1)
		if err != nil {
			return nil, err
		}
		return &Between{X: left, Low: low, High: high, Not: not}, nil
	}
}

// parsePrefix parses an operand: a literal, a name, a function call, a
// parenthesised expression, or a prefix operator applied to an operand.
func (p *parser) parsePrefix() (Expr, error) {
	t := p.peek()
	switch t.kind {
	case tokInt:
		p.advance()
		v, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, &Error{t.pos, "integer out of range: " + t.text}
		}
		return &IntLit{Value: v}, nil
	case tokFloat:
		p.advance()
		v, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, &Error{t.pos, "number out of range: " + t.text}
		}
		return &FloatLit{Value: v}, nil
	case tokString:
		p.advance()
		return &StringLit{Value: t.text}, nil
	case tokSymbol:
		switch t.text {
		case "(":
			p.advance()
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			return e, p.expectSymbol(")")
		case "-":
			p.advance()
			x, err := p.parseBinary(precUnary)
			if err != nil {
				return nil, err
			}
			return &Unary{Op: "-", X: x}, nil
		}
	case tokWord:
		switch t.upper() {
		case "NOT":
			p.advance()
			x, err := p.parseBinary(precNot)
			if err != nil {
				return nil, err
			}
			return &Unary{Op: "NOT", X: x}, nil
		case "TRUE", "FALSE":
			p.advance()
			return &BoolLit{Value: t.upper() == "TRUE"}, nil
		case "NULL":
			p.advance()
			return &NullLit{}, nil
		}
	}
	if p.isIdent() {
		return p.parseName()
	}
	return nil, p.unexpected("expression")
}

// parseName parses column, table.column, or a function call.
func (p *parser) parseName() (Expr, error) {
	name, err := p.parseIdent("name")
	if err != nil {
		return nil, err
	}
	if p.acceptSymbol("(") {
		call := &Call{Name: name}
		if p.acceptSymbol("*") {
			call.Star = true
			return call, p.expectSymbol(")")
		}
		if p.acceptSymbol(")") {
			return call, nil
		}
		if call.Args, err = p.parseExprList(); err != nil {
			return nil, err
		}
		return call, nil
	}
	if p.acceptSymbol(".") {
		col, err := p.parseIdent("column name")
		if err != nil {
			return nil, err
		}
		return &ColumnRef{Table: name, Column: col}, nil
	}
	return &ColumnRef{Column: name}, nil
}
