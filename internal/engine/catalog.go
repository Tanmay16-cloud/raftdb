package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"

	"raftdb/internal/mvcc"
	"raftdb/internal/parser"
)

// Table is a table definition, stored as JSON under c/<name>.
type Table struct {
	ID      uint32   `json:"id"`
	Name    string   `json:"name"`
	Columns []Column `json:"cols"`
	PK      int      `json:"pk"` // index of the primary key column, or -1 for a hidden row ID
	Indexes []*Index `json:"idx,omitempty"`
}

type Column struct {
	Name    string      `json:"n"`
	Type    parser.Type `json:"t"`
	NotNull bool        `json:"nn,omitempty"`
}

// Index is a secondary index on one column.
type Index struct {
	ID     uint32 `json:"id"`
	Name   string `json:"n"`
	Column int    `json:"c"`
	Unique bool   `json:"u,omitempty"`
}

func (t *Table) column(name string) int {
	for i, c := range t.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// String returns the statements that would recreate the table's schema.
func (t *Table) String() string {
	ct := &parser.CreateTable{Name: t.Name}
	for i, c := range t.Columns {
		ct.Columns = append(ct.Columns, parser.ColumnDef{
			Name: c.Name, Type: c.Type, PrimaryKey: i == t.PK, NotNull: c.NotNull && i != t.PK,
		})
	}
	lines := []string{ct.String() + ";"}
	for _, idx := range t.Indexes {
		ci := &parser.CreateIndex{Name: idx.Name, Table: t.Name, Column: t.Columns[idx.Column].Name, Unique: idx.Unique}
		lines = append(lines, ci.String()+";")
	}
	return strings.Join(lines, "\n")
}

// loadCatalog reads every table definition visible to the transaction.
func (tx *txn) loadCatalog() error {
	tx.tables = make(map[string]*Table)
	it := tx.scan(catalogKey(""), prefixEnd(catalogKey("")))
	for {
		_, v, ok, err := it.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		t := new(Table)
		if err := json.Unmarshal(v, t); err != nil {
			return fmt.Errorf("engine: corrupt catalog entry: %w", err)
		}
		tx.tables[t.Name] = t
	}
}

func (tx *txn) saveTable(t *Table) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if len(b) > mvcc.MaxValueSize {
		return fmt.Errorf("the definition of table %q is too large (%d bytes; the limit is %d)", t.Name, len(b), mvcc.MaxValueSize)
	}
	if err := tx.kv.Put(catalogKey(t.Name), b); err != nil {
		return err
	}
	tx.ownCatalog()
	tx.tables[t.Name] = t
	return nil
}

// nextID allocates an ID for a new table or index. IDs come from a
// sequence, so they are never reused, even by rolled-back transactions.
func (tx *txn) nextID() (uint32, error) {
	n, err := tx.db.store.Sequence(nextIDKey)
	if n > math.MaxUint32 {
		return 0, errors.New("engine: out of table and index IDs")
	}
	return uint32(n), err
}

func (tx *txn) table(name string) (*Table, error) {
	t, ok := tx.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return t, nil
}

// findIndex returns the index called name and the table it belongs to.
func (tx *txn) findIndex(name string) (*Table, int) {
	for _, t := range tx.tables {
		for i, idx := range t.Indexes {
			if idx.Name == name {
				return t, i
			}
		}
	}
	return nil, -1
}

// ownCatalog marks the transaction as changing the schema and gives it a
// private copy of the catalog map, which may be shared with the database's
// cache and other transactions until then.
func (tx *txn) ownCatalog() {
	if !tx.ddl {
		tx.tables = maps.Clone(tx.tables)
		tx.ddl = true
	}
}
