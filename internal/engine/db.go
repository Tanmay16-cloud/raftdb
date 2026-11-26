package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"raftdb/internal/parser"
	"raftdb/internal/storage"
)

// DB is a SQL database stored in a single B+ tree. It is used by one
// session at a time; concurrent transactions arrive with MVCC.
type DB struct {
	tree   *storage.BTree
	tables map[string]*Table
	inTxn  bool // inside BEGIN ... COMMIT
}

// Result is the outcome of one statement.
type Result struct {
	Columns []string // for queries; nil otherwise
	Rows    []Row
	Message string // command tag, such as "INSERT 3" or "CREATE TABLE"
}

// Open opens the database at path, creating it if needed.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, storage.Options{})
}

// OpenWithOptions is like Open but lets the caller tune the storage engine.
func OpenWithOptions(path string, opts storage.Options) (*DB, error) {
	tree, err := storage.OpenWithOptions(path, opts)
	if err != nil {
		return nil, err
	}
	db := &DB{tree: tree}
	if err := db.loadCatalog(); err != nil {
		tree.Close()
		return nil, err
	}
	return db, nil
}

// Close closes the database. An open transaction is rolled back.
func (db *DB) Close() error {
	if db.inTxn {
		db.tree.Rollback()
	}
	return db.tree.Close()
}

// InTransaction reports whether a BEGIN is waiting for COMMIT or ROLLBACK.
func (db *DB) InTransaction() bool { return db.inTxn }

// Stats returns the storage engine's activity counters.
func (db *DB) Stats() storage.Stats { return db.tree.Stats() }

// Exec parses and runs a script of statements. It stops at the first error
// and returns the results of the statements before it.
func (db *DB) Exec(sql string) ([]*Result, error) {
	stmts, err := parser.ParseAll(sql)
	if err != nil {
		return nil, err
	}
	var results []*Result
	for _, s := range stmts {
		r, err := db.ExecStatement(s)
		if err != nil {
			return results, err
		}
		results = append(results, r)
	}
	return results, nil
}

// Query runs a single statement and returns its result.
func (db *DB) Query(sql string) (*Result, error) {
	s, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return db.ExecStatement(s)
}

// ExecStatement runs one parsed statement.
//
// Every statement is atomic. Outside a transaction, each statement commits
// on success; if it fails partway (say, the third row of an INSERT
// violates the primary key), the storage engine rolls back to the last
// commit, so the first two rows vanish too. Inside BEGIN ... COMMIT, the
// changes accumulate until COMMIT. An error inside a transaction rolls back
// the whole transaction, because the storage engine can only roll back to
// the last commit (statement-level savepoints would need more machinery).
func (db *DB) ExecStatement(s parser.Statement) (*Result, error) {
	switch s.(type) {
	case *parser.Begin:
		if db.inTxn {
			return nil, errors.New("a transaction is already in progress")
		}
		db.inTxn = true
		return &Result{Message: "BEGIN"}, nil
	case *parser.Commit:
		if !db.inTxn {
			return nil, errors.New("no transaction in progress")
		}
		db.inTxn = false
		if err := db.tree.Sync(); err != nil {
			return nil, db.abort(err)
		}
		return &Result{Message: "COMMIT"}, nil
	case *parser.Rollback:
		if !db.inTxn {
			return nil, errors.New("no transaction in progress")
		}
		db.inTxn = false
		if err := db.abort(nil); err != nil {
			return nil, err
		}
		return &Result{Message: "ROLLBACK"}, nil
	}

	res, err := db.run(s)
	if err != nil {
		if db.inTxn {
			db.inTxn = false
			return nil, fmt.Errorf("%w (the transaction was rolled back)", db.abort(err))
		}
		return nil, db.abort(err)
	}
	if !db.inTxn {
		if err := db.tree.Sync(); err != nil {
			return nil, db.abort(err)
		}
	}
	return res, nil
}

// abort discards uncommitted changes, reloads the catalog they may have
// touched, and returns cause (or the rollback's own error if cause is nil).
func (db *DB) abort(cause error) error {
	err := db.tree.Rollback()
	if err == nil {
		err = db.loadCatalog()
	}
	if cause != nil {
		return cause
	}
	return err
}

func (db *DB) run(s parser.Statement) (*Result, error) {
	switch s := s.(type) {
	case *parser.CreateTable:
		return db.createTable(s)
	case *parser.DropTable:
		return db.dropTable(s)
	case *parser.CreateIndex:
		return db.createIndex(s)
	case *parser.DropIndex:
		return db.dropIndex(s)
	case *parser.Insert:
		return db.insert(s)
	case *parser.Update:
		return db.update(s)
	case *parser.Delete:
		return db.delete(s)
	case *parser.Select:
		return db.selectRows(s)
	case *parser.Explain:
		return db.explain(s.Query)
	}
	return nil, fmt.Errorf("unsupported statement %T", s)
}

// ---------------------------------------------------------------------------
// Storage access

// rangeIter walks the keys in [start, end). The storage engine's Scan holds
// the tree's lock while it calls back, so rangeIter reads a batch of entries
// at a time and resumes after the last key. Between batches the caller is
// free to run other tree operations, such as fetching a row for an index
// entry.
type rangeIter struct {
	tree       *storage.BTree
	start, end []byte
	batch      []kv
	i          int
	done       bool
}

type kv struct{ k, v []byte }

const scanBatch = 256

func (db *DB) scan(start, end []byte) *rangeIter {
	return &rangeIter{tree: db.tree, start: start, end: end}
}

func (it *rangeIter) next() (key, value []byte, ok bool, err error) {
	if it.i == len(it.batch) {
		if it.done {
			return nil, nil, false, nil
		}
		it.batch, it.i = it.batch[:0], 0
		err := it.tree.Scan(it.start, it.end, func(k, v []byte) bool {
			it.batch = append(it.batch, kv{k, v})
			return len(it.batch) < scanBatch
		})
		if err != nil {
			return nil, nil, false, err
		}
		if len(it.batch) < scanBatch {
			it.done = true
		}
		if len(it.batch) == 0 {
			return nil, nil, false, nil
		}
		// The smallest key after the last one in this batch.
		it.start = append(slices.Clip(it.batch[len(it.batch)-1].k), 0)
	}
	e := it.batch[it.i]
	it.i++
	return e.k, e.v, true, nil
}

// deleteRange removes every key in [start, end).
func (db *DB) deleteRange(start, end []byte) error {
	for {
		var keys [][]byte
		err := db.tree.Scan(start, end, func(k, _ []byte) bool {
			keys = append(keys, k)
			return len(keys) < scanBatch
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		for _, k := range keys {
			if _, err := db.tree.Delete(k); err != nil {
				return err
			}
		}
	}
}

func (db *DB) exists(start, end []byte) (bool, error) {
	found := false
	err := db.tree.Scan(start, end, func(_, _ []byte) bool {
		found = true
		return false
	})
	return found, err
}

func checkKeySize(key []byte, what string) error {
	if len(key) > storage.MaxKeySize {
		return fmt.Errorf("%s is too long to store in a key (%d bytes encoded; the limit is %d)", what, len(key), storage.MaxKeySize)
	}
	return nil
}

func slicesSortByName(ts []*Table) {
	slices.SortFunc(ts, func(a, b *Table) int { return strings.Compare(a.Name, b.Name) })
}

// ---------------------------------------------------------------------------
// Data definition

const maxNameLen = 64

func checkName(kind, name string) error {
	if len(name) > maxNameLen {
		return fmt.Errorf("%s name %q is longer than %d bytes", kind, name, maxNameLen)
	}
	return nil
}

func (db *DB) createTable(s *parser.CreateTable) (*Result, error) {
	if err := checkName("table", s.Name); err != nil {
		return nil, err
	}
	if _, ok := db.tables[s.Name]; ok {
		if s.IfNotExists {
			return &Result{Message: "CREATE TABLE"}, nil
		}
		return nil, fmt.Errorf("table %q already exists", s.Name)
	}
	t := &Table{Name: s.Name, PK: -1}
	for i, c := range s.Columns {
		if err := checkName("column", c.Name); err != nil {
			return nil, err
		}
		if t.column(c.Name) >= 0 {
			return nil, fmt.Errorf("column %q specified more than once", c.Name)
		}
		if c.PrimaryKey {
			if t.PK >= 0 {
				return nil, fmt.Errorf("multiple primary keys for table %q are not allowed", s.Name)
			}
			t.PK = i
		}
		t.Columns = append(t.Columns, Column{Name: c.Name, Type: c.Type, NotNull: c.NotNull || c.PrimaryKey})
	}
	id, err := db.nextID()
	if err != nil {
		return nil, err
	}
	t.ID = id
	if err := db.saveTable(t); err != nil {
		return nil, err
	}
	return &Result{Message: "CREATE TABLE"}, nil
}

func (db *DB) dropTable(s *parser.DropTable) (*Result, error) {
	t, ok := db.tables[s.Name]
	if !ok {
		if s.IfExists {
			return &Result{Message: "DROP TABLE"}, nil
		}
		return nil, fmt.Errorf("table %q does not exist", s.Name)
	}
	for _, idx := range t.Indexes {
		p := indexPrefix(idx.ID)
		if err := db.deleteRange(p, prefixEnd(p)); err != nil {
			return nil, err
		}
	}
	p := rowPrefix(t.ID)
	if err := db.deleteRange(p, prefixEnd(p)); err != nil {
		return nil, err
	}
	for _, k := range [][]byte{rowIDKey(t.ID), catalogKey(t.Name)} {
		if _, err := db.tree.Delete(k); err != nil {
			return nil, err
		}
	}
	delete(db.tables, t.Name)
	return &Result{Message: "DROP TABLE"}, nil
}

func (db *DB) createIndex(s *parser.CreateIndex) (*Result, error) {
	if err := checkName("index", s.Name); err != nil {
		return nil, err
	}
	if t, _ := db.findIndex(s.Name); t != nil {
		if s.IfNotExists {
			return &Result{Message: "CREATE INDEX"}, nil
		}
		return nil, fmt.Errorf("index %q already exists", s.Name)
	}
	t, err := db.table(s.Table)
	if err != nil {
		return nil, err
	}
	col := t.column(s.Column)
	if col < 0 {
		return nil, fmt.Errorf("column %q of table %q does not exist", s.Column, s.Table)
	}
	id, err := db.nextID()
	if err != nil {
		return nil, err
	}
	idx := &Index{ID: id, Name: s.Name, Column: col, Unique: s.Unique}

	// Backfill the index from the existing rows. A unique index fails here if
	// the column already holds duplicates.
	it := db.scan(rowPrefix(t.ID), prefixEnd(rowPrefix(t.ID)))
	for {
		k, v, ok, err := it.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		row, err := decodeRow(v, len(t.Columns))
		if err != nil {
			return nil, err
		}
		if err := db.addIndexEntry(t, idx, row, k[len(rowPrefix(t.ID)):]); err != nil {
			return nil, err
		}
	}
	t2 := *t
	t2.Indexes = append(slices.Clip(t.Indexes), idx)
	if err := db.saveTable(&t2); err != nil {
		return nil, err
	}
	return &Result{Message: "CREATE INDEX"}, nil
}

func (db *DB) dropIndex(s *parser.DropIndex) (*Result, error) {
	t, i := db.findIndex(s.Name)
	if t == nil {
		if s.IfExists {
			return &Result{Message: "DROP INDEX"}, nil
		}
		return nil, fmt.Errorf("index %q does not exist", s.Name)
	}
	p := indexPrefix(t.Indexes[i].ID)
	if err := db.deleteRange(p, prefixEnd(p)); err != nil {
		return nil, err
	}
	t2 := *t
	t2.Indexes = slices.Delete(slices.Clone(t.Indexes), i, i+1)
	if err := db.saveTable(&t2); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP INDEX"}, nil
}

// ---------------------------------------------------------------------------
// Writing rows

// checkRow converts each value to its column's type and enforces NOT NULL.
func checkRow(t *Table, row Row) error {
	for i, c := range t.Columns {
		v, ok := coerce(row[i], c.Type)
		if !ok {
			return fmt.Errorf("column %q is %s, but the value %s is %s", c.Name, c.Type, literal(row[i]), typeName(row[i]))
		}
		if v == nil && c.NotNull {
			return fmt.Errorf("null value in column %q violates not-null constraint", c.Name)
		}
		row[i] = v
	}
	return nil
}

// primaryKey returns the encoded primary key for a new row: the key
// column's value, or the next hidden row ID if the table has no key.
func (db *DB) primaryKey(t *Table, row Row) ([]byte, error) {
	if t.PK >= 0 {
		return appendKey(nil, row[t.PK]), nil
	}
	id, err := db.counter(rowIDKey(t.ID))
	if err != nil {
		return nil, err
	}
	return appendKey(nil, int64(id)), nil
}

// writeRow stores a row and its index entries. The caller has already
// checked the row with checkRow.
func (db *DB) writeRow(t *Table, row Row, pk []byte) error {
	key := rowKey(t.ID, pk)
	if err := checkKeySize(key, "primary key value"); err != nil {
		return err
	}
	if t.PK >= 0 {
		_, dup, err := db.tree.Get(key)
		if err != nil {
			return err
		}
		if dup {
			return fmt.Errorf("duplicate key value violates primary key: %s = %s already exists", t.Columns[t.PK].Name, literal(row[t.PK]))
		}
	}
	val := encodeRow(row)
	if len(val) > storage.MaxValueSize {
		return fmt.Errorf("row is too large (%d bytes; the limit is %d)", len(val), storage.MaxValueSize)
	}
	if err := db.tree.Put(key, val); err != nil {
		return err
	}
	for _, idx := range t.Indexes {
		if err := db.addIndexEntry(t, idx, row, pk); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) deleteRow(t *Table, row Row, pk []byte) error {
	if _, err := db.tree.Delete(rowKey(t.ID, pk)); err != nil {
		return err
	}
	for _, idx := range t.Indexes {
		if _, err := db.tree.Delete(indexKey(idx.ID, row[idx.Column], pk)); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) addIndexEntry(t *Table, idx *Index, row Row, pk []byte) error {
	v := row[idx.Column]
	key := indexKey(idx.ID, v, pk)
	if err := checkKeySize(key, fmt.Sprintf("value of indexed column %q", t.Columns[idx.Column].Name)); err != nil {
		return err
	}
	// Several NULLs may share a unique index: NULL is never equal to NULL.
	if idx.Unique && v != nil {
		p := appendKey(indexPrefix(idx.ID), v)
		dup, err := db.exists(p, prefixEnd(p))
		if err != nil {
			return err
		}
		if dup {
			return fmt.Errorf("duplicate key value violates unique index %q: %s = %s already exists", idx.Name, t.Columns[idx.Column].Name, literal(v))
		}
	}
	return db.tree.Put(key, pk)
}

func (db *DB) insert(s *parser.Insert) (*Result, error) {
	t, err := db.table(s.Table)
	if err != nil {
		return nil, err
	}
	// cols[i] is the table column that the i-th value goes into.
	cols := make([]int, 0, len(t.Columns))
	if s.Columns == nil {
		for i := range t.Columns {
			cols = append(cols, i)
		}
	} else {
		seen := make(map[int]bool)
		for _, name := range s.Columns {
			c := t.column(name)
			if c < 0 {
				return nil, fmt.Errorf("column %q of table %q does not exist", name, t.Name)
			}
			if seen[c] {
				return nil, fmt.Errorf("column %q specified more than once", name)
			}
			seen[c] = true
			cols = append(cols, c)
		}
	}
	consts := &compiler{where: "VALUES"}
	for _, exprs := range s.Rows {
		if len(exprs) != len(cols) {
			return nil, fmt.Errorf("INSERT has %d values but %d target columns", len(exprs), len(cols))
		}
		row := make(Row, len(t.Columns))
		for i, e := range exprs {
			f, err := consts.compile(e)
			if err != nil {
				return nil, err
			}
			if row[cols[i]], err = f(nil); err != nil {
				return nil, err
			}
		}
		if err := checkRow(t, row); err != nil {
			return nil, err
		}
		pk, err := db.primaryKey(t, row)
		if err != nil {
			return nil, err
		}
		if err := db.writeRow(t, row, pk); err != nil {
			return nil, err
		}
	}
	return &Result{Message: fmt.Sprintf("INSERT %d", len(s.Rows))}, nil
}

// matchRows returns every row of t that satisfies where, with its encoded
// primary key. UPDATE and DELETE collect all their rows before changing
// any. Changing rows during the scan could make it see a row twice: an
// update that moves a row forward in the index being scanned would meet it
// again later (the "Halloween problem").
func (db *DB) matchRows(t *Table, where parser.Expr) (rows []Row, pks [][]byte, err error) {
	plan, err := db.planTableScan(t, t.Name, where, true)
	if err != nil {
		return nil, nil, err
	}
	it, err := plan.open(db)
	if err != nil {
		return nil, nil, err
	}
	for {
		r, err := it.next()
		if err != nil {
			return nil, nil, err
		}
		if r == nil {
			return rows, pks, nil
		}
		n := len(t.Columns)
		rows = append(rows, r[:n:n])
		pks = append(pks, []byte(r[n].(rowKeyValue)))
	}
}

func (db *DB) delete(s *parser.Delete) (*Result, error) {
	t, err := db.table(s.Table)
	if err != nil {
		return nil, err
	}
	rows, pks, err := db.matchRows(t, s.Where)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if err := db.deleteRow(t, rows[i], pks[i]); err != nil {
			return nil, err
		}
	}
	return &Result{Message: fmt.Sprintf("DELETE %d", len(rows))}, nil
}

func (db *DB) update(s *parser.Update) (*Result, error) {
	t, err := db.table(s.Table)
	if err != nil {
		return nil, err
	}
	c := &compiler{scope: tableScope(t, t.Name), where: "UPDATE"}
	type assignment struct {
		col int
		val evalFn
	}
	var set []assignment
	seen := make(map[int]bool)
	for _, a := range s.Set {
		col := t.column(a.Column)
		if col < 0 {
			return nil, fmt.Errorf("column %q of table %q does not exist", a.Column, t.Name)
		}
		if seen[col] {
			return nil, fmt.Errorf("multiple assignments to column %q", a.Column)
		}
		seen[col] = true
		f, err := c.compile(a.Value)
		if err != nil {
			return nil, err
		}
		set = append(set, assignment{col, f})
	}

	rows, pks, err := db.matchRows(t, s.Where)
	if err != nil {
		return nil, err
	}
	// Compute every new row before changing anything.
	newRows := make([]Row, len(rows))
	newPKs := make([][]byte, len(rows))
	for i, old := range rows {
		// Every SET expression sees the row as it was before the update.
		row := slices.Clone(old)
		for _, a := range set {
			if row[a.col], err = a.val(old); err != nil {
				return nil, err
			}
		}
		if err := checkRow(t, row); err != nil {
			return nil, err
		}
		newRows[i], newPKs[i] = row, pks[i]
		if t.PK >= 0 {
			newPKs[i] = appendKey(nil, row[t.PK])
		}
	}
	// Then delete all the old rows before writing any new one. Primary key
	// and unique index checks therefore see the statement's final state, so
	// "SET id = id + 1" succeeds whatever order the rows were read in (which
	// depends on the plan). Replacing rows one at a time would fail when an
	// early row moved onto a later row's key before that row had moved away.
	for i, old := range rows {
		if err := db.deleteRow(t, old, pks[i]); err != nil {
			return nil, err
		}
	}
	for i, row := range newRows {
		if err := db.writeRow(t, row, newPKs[i]); err != nil {
			return nil, err
		}
	}
	return &Result{Message: fmt.Sprintf("UPDATE %d", len(rows))}, nil
}
