package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"raftdb/internal/parser"
	"raftdb/internal/storage"
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

// loadCatalog reads every table definition into memory. It runs at open and
// after a rollback, which may have undone schema changes.
func (db *DB) loadCatalog() error {
	db.tables = make(map[string]*Table)
	it := db.scan(catalogKey(""), prefixEnd(catalogKey("")))
	for {
		_, v, ok, err := it.next()
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
		db.tables[t.Name] = t
	}
}

func (db *DB) saveTable(t *Table) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if len(b) > storage.MaxValueSize {
		return fmt.Errorf("the definition of table %q is too large (%d bytes; the limit is %d)", t.Name, len(b), storage.MaxValueSize)
	}
	if err := db.tree.Put(catalogKey(t.Name), b); err != nil {
		return err
	}
	db.tables[t.Name] = t
	return nil
}

// nextID allocates an ID for a new table or index.
func (db *DB) nextID() (uint32, error) {
	return db.counter(nextIDKey)
}

// counter increments the counter stored under key and returns its old value.
// Counters start at 1.
func (db *DB) counter(key []byte) (uint32, error) {
	id := uint32(1)
	v, ok, err := db.tree.Get(key)
	if err != nil {
		return 0, err
	}
	if ok {
		id = binary.BigEndian.Uint32(v)
	}
	return id, db.tree.Put(key, binary.BigEndian.AppendUint32(nil, id+1))
}

func (db *DB) table(name string) (*Table, error) {
	t, ok := db.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return t, nil
}

// findIndex returns the index called name and the table it belongs to.
func (db *DB) findIndex(name string) (*Table, int) {
	for _, t := range db.tables {
		for i, idx := range t.Indexes {
			if idx.Name == name {
				return t, i
			}
		}
	}
	return nil, -1
}

// Tables returns the definitions of every table.
func (db *DB) Tables() []*Table {
	out := make([]*Table, 0, len(db.tables))
	for _, t := range db.tables {
		out = append(out, t)
	}
	slicesSortByName(out)
	return out
}

// Table returns the definition of one table, or nil.
func (db *DB) Table(name string) *Table { return db.tables[name] }
