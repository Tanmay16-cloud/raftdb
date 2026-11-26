package parser

import (
	"strconv"
	"strings"
)

// Statement is a parsed SQL statement.
//
// String returns the statement as canonical SQL: keywords in upper case,
// every compound expression in parentheses, and synonyms normalised (for
// example, <> becomes != and VARCHAR becomes TEXT). Parsing that text again
// gives back an identical tree.
type Statement interface {
	statement()
	String() string
}

// Expr is a parsed expression.
type Expr interface {
	expr()
	String() string
}

// ---------------------------------------------------------------------------
// Statements

// Type is a column type.
type Type int

const (
	TypeInt   Type = iota + 1 // 64-bit signed integer
	TypeFloat                 // 64-bit floating point
	TypeText                  // UTF-8 string
	TypeBool
)

var typeNames = map[string]Type{
	"int": TypeInt, "integer": TypeInt, "bigint": TypeInt,
	"float": TypeFloat, "real": TypeFloat, "double": TypeFloat,
	"text": TypeText, "varchar": TypeText, "string": TypeText,
	"bool": TypeBool, "boolean": TypeBool,
}

func (t Type) String() string {
	switch t {
	case TypeInt:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeText:
		return "TEXT"
	case TypeBool:
		return "BOOL"
	}
	return "UNKNOWN"
}

type ColumnDef struct {
	Name       string
	Type       Type
	PrimaryKey bool
	NotNull    bool
}

// CreateTable is CREATE TABLE [IF NOT EXISTS] name (column type [PRIMARY KEY] [NOT NULL], ...).
type CreateTable struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDef
}

// DropTable is DROP TABLE [IF EXISTS] name.
type DropTable struct {
	Name     string
	IfExists bool
}

// CreateIndex is CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (column).
type CreateIndex struct {
	Name        string
	Table       string
	Column      string
	Unique      bool
	IfNotExists bool
}

// DropIndex is DROP INDEX [IF EXISTS] name.
type DropIndex struct {
	Name     string
	IfExists bool
}

// Insert is INSERT INTO table [(columns)] VALUES (row), (row), ...
type Insert struct {
	Table   string
	Columns []string // nil means every column, in table order
	Rows    [][]Expr
}

// Select is a SELECT query. Clauses that are absent are nil.
type Select struct {
	Distinct bool
	Items    []SelectItem
	From     *TableRef
	Joins    []Join
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []OrderItem
	Limit    Expr
	Offset   Expr
}

// SelectItem is one entry in the SELECT list: * or an expression with an
// optional alias.
type SelectItem struct {
	Star  bool
	Expr  Expr
	Alias string
}

type TableRef struct {
	Name  string
	Alias string
}

// Join is JOIN table ON condition, or ", table" (a cross join) when On is nil.
type Join struct {
	Table TableRef
	On    Expr
}

type OrderItem struct {
	Expr Expr
	Desc bool
}

// Update is UPDATE table SET column = value, ... [WHERE condition].
type Update struct {
	Table string
	Set   []Assignment
	Where Expr
}

type Assignment struct {
	Column string
	Value  Expr
}

// Delete is DELETE FROM table [WHERE condition].
type Delete struct {
	Table string
	Where Expr
}

// Explain is EXPLAIN SELECT ...: show the query plan instead of running it.
type Explain struct {
	Query *Select
}

// Vacuum removes row versions that no transaction can see any more.
type Vacuum struct{}

// Begin, Commit and Rollback control transactions.
type (
	Begin    struct{}
	Commit   struct{}
	Rollback struct{}
)

func (*CreateTable) statement() {}
func (*DropTable) statement()   {}
func (*CreateIndex) statement() {}
func (*DropIndex) statement()   {}
func (*Insert) statement()      {}
func (*Select) statement()      {}
func (*Update) statement()      {}
func (*Delete) statement()      {}
func (*Explain) statement()     {}
func (*Vacuum) statement()      {}
func (*Begin) statement()       {}
func (*Commit) statement()      {}
func (*Rollback) statement()    {}

func (s *CreateTable) String() string {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if s.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(quoteIdent(s.Name) + " (")
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(c.Name) + " " + c.Type.String())
		if c.PrimaryKey {
			b.WriteString(" PRIMARY KEY")
		}
		if c.NotNull {
			b.WriteString(" NOT NULL")
		}
	}
	b.WriteString(")")
	return b.String()
}

func (s *DropTable) String() string {
	if s.IfExists {
		return "DROP TABLE IF EXISTS " + quoteIdent(s.Name)
	}
	return "DROP TABLE " + quoteIdent(s.Name)
}

func (s *CreateIndex) String() string {
	var b strings.Builder
	b.WriteString("CREATE ")
	if s.Unique {
		b.WriteString("UNIQUE ")
	}
	b.WriteString("INDEX ")
	if s.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(quoteIdent(s.Name) + " ON " + quoteIdent(s.Table) + " (" + quoteIdent(s.Column) + ")")
	return b.String()
}

func (s *DropIndex) String() string {
	if s.IfExists {
		return "DROP INDEX IF EXISTS " + quoteIdent(s.Name)
	}
	return "DROP INDEX " + quoteIdent(s.Name)
}

func (s *Insert) String() string {
	var b strings.Builder
	b.WriteString("INSERT INTO " + quoteIdent(s.Table))
	if s.Columns != nil {
		quoted := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			quoted[i] = quoteIdent(c)
		}
		b.WriteString(" (" + strings.Join(quoted, ", ") + ")")
	}
	b.WriteString(" VALUES ")
	for i, row := range s.Rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(" + joinExprs(row) + ")")
	}
	return b.String()
}

func (s *Select) String() string {
	var b strings.Builder
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}
	for i, it := range s.Items {
		if i > 0 {
			b.WriteString(", ")
		}
		if it.Star {
			b.WriteString("*")
			continue
		}
		b.WriteString(it.Expr.String())
		if it.Alias != "" {
			b.WriteString(" AS " + quoteIdent(it.Alias))
		}
	}
	if s.From != nil {
		b.WriteString(" FROM " + s.From.String())
	}
	for _, j := range s.Joins {
		if j.On == nil {
			b.WriteString(", " + j.Table.String())
		} else {
			b.WriteString(" JOIN " + j.Table.String() + " ON " + j.On.String())
		}
	}
	if s.Where != nil {
		b.WriteString(" WHERE " + s.Where.String())
	}
	if s.GroupBy != nil {
		b.WriteString(" GROUP BY " + joinExprs(s.GroupBy))
	}
	if s.Having != nil {
		b.WriteString(" HAVING " + s.Having.String())
	}
	for i, o := range s.OrderBy {
		if i == 0 {
			b.WriteString(" ORDER BY ")
		} else {
			b.WriteString(", ")
		}
		b.WriteString(o.Expr.String())
		if o.Desc {
			b.WriteString(" DESC")
		}
	}
	if s.Limit != nil {
		b.WriteString(" LIMIT " + s.Limit.String())
	}
	if s.Offset != nil {
		b.WriteString(" OFFSET " + s.Offset.String())
	}
	return b.String()
}

func (t TableRef) String() string {
	if t.Alias != "" {
		return quoteIdent(t.Name) + " AS " + quoteIdent(t.Alias)
	}
	return quoteIdent(t.Name)
}

func (s *Update) String() string {
	var b strings.Builder
	b.WriteString("UPDATE " + quoteIdent(s.Table) + " SET ")
	for i, a := range s.Set {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(a.Column) + " = " + a.Value.String())
	}
	if s.Where != nil {
		b.WriteString(" WHERE " + s.Where.String())
	}
	return b.String()
}

func (s *Delete) String() string {
	if s.Where != nil {
		return "DELETE FROM " + quoteIdent(s.Table) + " WHERE " + s.Where.String()
	}
	return "DELETE FROM " + quoteIdent(s.Table)
}

func (s *Explain) String() string { return "EXPLAIN " + s.Query.String() }
func (*Vacuum) String() string    { return "VACUUM" }
func (*Begin) String() string     { return "BEGIN" }
func (*Commit) String() string    { return "COMMIT" }
func (*Rollback) String() string  { return "ROLLBACK" }

// ---------------------------------------------------------------------------
// Expressions

type (
	IntLit    struct{ Value int64 }
	FloatLit  struct{ Value float64 }
	StringLit struct{ Value string }
	BoolLit   struct{ Value bool }
	NullLit   struct{}

	// ColumnRef is column or table.column.
	ColumnRef struct {
		Table  string // empty if unqualified
		Column string
	}

	// Unary is -X or NOT X.
	Unary struct {
		Op string // "-" or "NOT"
		X  Expr
	}

	// Binary is Left Op Right, where Op is one of
	// OR AND = != < <= > >= LIKE NOT LIKE || + - * / %.
	Binary struct {
		Op          string
		Left, Right Expr
	}

	// IsNull is X IS [NOT] NULL.
	IsNull struct {
		X   Expr
		Not bool
	}

	// In is X [NOT] IN (list).
	In struct {
		X    Expr
		List []Expr
		Not  bool
	}

	// Between is X [NOT] BETWEEN Low AND High.
	Between struct {
		X, Low, High Expr
		Not          bool
	}

	// Call is a function call such as lower(name) or count(*).
	Call struct {
		Name string
		Args []Expr
		Star bool // count(*)
	}
)

func (*IntLit) expr()    {}
func (*FloatLit) expr()  {}
func (*StringLit) expr() {}
func (*BoolLit) expr()   {}
func (*NullLit) expr()   {}
func (*ColumnRef) expr() {}
func (*Unary) expr()     {}
func (*Binary) expr()    {}
func (*IsNull) expr()    {}
func (*In) expr()        {}
func (*Between) expr()   {}
func (*Call) expr()      {}

func (e *IntLit) String() string { return strconv.FormatInt(e.Value, 10) }

func (e *FloatLit) String() string {
	s := strconv.FormatFloat(e.Value, 'g', -1, 64)
	if !strings.ContainsAny(s, ".e") {
		s += ".0" // keep it a float when parsed again: 2000.0, not 2000
	}
	return s
}

func (e *StringLit) String() string { return quoteString(e.Value) }

func (e *BoolLit) String() string {
	if e.Value {
		return "TRUE"
	}
	return "FALSE"
}

func (*NullLit) String() string { return "NULL" }

func (e *ColumnRef) String() string {
	if e.Table != "" {
		return quoteIdent(e.Table) + "." + quoteIdent(e.Column)
	}
	return quoteIdent(e.Column)
}

func (e *Unary) String() string {
	if e.Op == "NOT" {
		return "(NOT " + e.X.String() + ")"
	}
	return "(" + e.Op + e.X.String() + ")"
}

func (e *Binary) String() string {
	return "(" + e.Left.String() + " " + e.Op + " " + e.Right.String() + ")"
}

func (e *IsNull) String() string {
	if e.Not {
		return "(" + e.X.String() + " IS NOT NULL)"
	}
	return "(" + e.X.String() + " IS NULL)"
}

func (e *In) String() string {
	op := " IN ("
	if e.Not {
		op = " NOT IN ("
	}
	return "(" + e.X.String() + op + joinExprs(e.List) + "))"
}

func (e *Between) String() string {
	op := " BETWEEN "
	if e.Not {
		op = " NOT BETWEEN "
	}
	return "(" + e.X.String() + op + e.Low.String() + " AND " + e.High.String() + ")"
}

func (e *Call) String() string {
	if e.Star {
		return quoteIdent(e.Name) + "(*)"
	}
	return quoteIdent(e.Name) + "(" + joinExprs(e.Args) + ")"
}

// ---------------------------------------------------------------------------
// Helpers

func joinExprs(list []Expr) string {
	parts := make([]string, len(list))
	for i, e := range list {
		parts[i] = e.String()
	}
	return strings.Join(parts, ", ")
}

func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// quoteIdent writes an identifier so that it parses back to the same name:
// bare if that is unambiguous, otherwise in double quotes.
func quoteIdent(s string) string {
	if isPlainIdent(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func isPlainIdent(s string) bool {
	if s == "" || s != asciiLower(s) || keywords[asciiUpper(s)] {
		return false
	}
	for i, r := range s {
		if (i == 0 && !isIdentStart(r)) || !isIdentPart(r) {
			return false
		}
	}
	return true
}
