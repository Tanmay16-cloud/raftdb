package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"raftdb/internal/mvcc"
	"raftdb/internal/parser"
	"raftdb/internal/storage"
)

// ErrConflict is returned (wrapped) when a transaction collides with a
// concurrent one. The transaction has been rolled back and can be retried.
var ErrConflict = mvcc.ErrConflict

// DB is a SQL database. It is safe for concurrent use: each Session runs
// its own transactions, isolated from the others by MVCC.
type DB struct {
	tree  *storage.BTree
	store *mvcc.Store

	// mu orders transaction starts and commits for the schema change rule
	// (see txn.commit). commitSeq counts commits that wrote something, and
	// schemaSeq is the commitSeq of the last one that changed the schema.
	mu                   sync.Mutex
	commitSeq, schemaSeq uint64

	// The catalog as of schemaSeq catalogSeq, shared read-only by
	// transactions (see begin).
	catalog      map[string]*Table
	catalogSeq   uint64
	catalogValid bool

	// The session behind DB.Exec and friends.
	defMu sync.Mutex
	def   *Session
}

// Session is one connection's view of the database: a sequence of
// statements, possibly grouped into transactions. A Session must be used
// by one goroutine at a time.
type Session struct {
	db *DB
	tx *txn // the open BEGIN ... COMMIT transaction, if any
}

// txn is the state of one transaction: its MVCC transaction and the
// catalog as its snapshot sees it.
type txn struct {
	db       *DB
	kv       *mvcc.Txn
	tables   map[string]*Table
	startSeq uint64
	ddl      bool // changed the schema
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
	store, err := mvcc.Open(tree)
	if err != nil {
		tree.Close()
		return nil, err
	}
	db := &DB{tree: tree, store: store}
	db.def = db.Session()
	return db, nil
}

// Session starts a new session.
func (db *DB) Session() *Session { return &Session{db: db} }

// Close closes the database. The default session's open transaction is
// rolled back; other sessions should be closed first.
func (db *DB) Close() error {
	db.defMu.Lock()
	db.def.Close()
	db.defMu.Unlock()
	return db.tree.Close()
}

// Stats returns the storage engine's activity counters.
func (db *DB) Stats() storage.Stats { return db.tree.Stats() }

// Exec, Query, ExecStatement, InTransaction, Tables and Table use the
// database's default session.

func (db *DB) Exec(sql string) ([]*Result, error) {
	db.defMu.Lock()
	defer db.defMu.Unlock()
	return db.def.Exec(sql)
}

func (db *DB) Query(sql string) (*Result, error) {
	db.defMu.Lock()
	defer db.defMu.Unlock()
	return db.def.Query(sql)
}

func (db *DB) ExecStatement(s parser.Statement) (*Result, error) {
	db.defMu.Lock()
	defer db.defMu.Unlock()
	return db.def.ExecStatement(s)
}

func (db *DB) InTransaction() bool {
	db.defMu.Lock()
	defer db.defMu.Unlock()
	return db.def.InTransaction()
}

func (db *DB) Tables() []*Table {
	db.defMu.Lock()
	defer db.defMu.Unlock()
	return db.def.Tables()
}

func (db *DB) Table(name string) *Table {
	db.defMu.Lock()
	defer db.defMu.Unlock()
	return db.def.Table(name)
}

// Close rolls back the session's open transaction, if any.
func (s *Session) Close() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.rollback()
	s.tx = nil
	return err
}

// InTransaction reports whether a BEGIN is waiting for COMMIT or ROLLBACK.
func (s *Session) InTransaction() bool { return s.tx != nil }

// Exec parses and runs a script of statements. It stops at the first error
// and returns the results of the statements before it.
func (s *Session) Exec(sql string) ([]*Result, error) {
	stmts, err := parser.ParseAll(sql)
	if err != nil {
		return nil, err
	}
	var results []*Result
	for _, st := range stmts {
		r, err := s.ExecStatement(st)
		if err != nil {
			return results, err
		}
		results = append(results, r)
	}
	return results, nil
}

// Query runs a single statement and returns its result.
func (s *Session) Query(sql string) (*Result, error) {
	st, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return s.ExecStatement(st)
}

// ExecStatement runs one parsed statement.
//
// Outside BEGIN ... COMMIT, each statement runs in a transaction of its own
// (read-only for queries), so it is atomic: if the third row of an INSERT
// violates the primary key, the first two vanish too. Inside a transaction,
// an error rolls back the whole transaction, since there are no savepoints.
func (s *Session) ExecStatement(st parser.Statement) (*Result, error) {
	switch st.(type) {
	case *parser.Begin:
		if s.tx != nil {
			return nil, errors.New("a transaction is already in progress")
		}
		tx, err := s.db.begin(false)
		if err != nil {
			return nil, err
		}
		s.tx = tx
		return &Result{Message: "BEGIN"}, nil
	case *parser.Commit:
		if s.tx == nil {
			return nil, errors.New("no transaction in progress")
		}
		tx := s.tx
		s.tx = nil
		if err := tx.commit(); err != nil {
			return nil, err
		}
		return &Result{Message: "COMMIT"}, nil
	case *parser.Rollback:
		if s.tx == nil {
			return nil, errors.New("no transaction in progress")
		}
		if err := s.Close(); err != nil {
			return nil, err
		}
		return &Result{Message: "ROLLBACK"}, nil
	case *parser.Vacuum:
		if s.tx != nil {
			return nil, errors.New("VACUUM cannot run inside a transaction")
		}
		n, err := s.db.store.Vacuum()
		if err != nil {
			return nil, err
		}
		return &Result{Message: fmt.Sprintf("VACUUM %d", n)}, nil
	}

	if s.tx != nil {
		res, err := s.tx.run(st)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("%w (the transaction was rolled back)", err)
		}
		return res, nil
	}
	_, query := st.(*parser.Select)
	_, explain := st.(*parser.Explain)
	tx, err := s.db.begin(query || explain)
	if err != nil {
		return nil, err
	}
	res, err := tx.run(st)
	if err != nil {
		tx.rollback()
		return nil, err
	}
	if err := tx.commit(); err != nil {
		return nil, err
	}
	return res, nil
}

// Tables returns the definitions of every table, as the session's
// transaction (or, outside one, the latest committed state) sees them.
func (s *Session) Tables() []*Table {
	tables := s.catalog()
	out := make([]*Table, 0, len(tables))
	for _, t := range tables {
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b *Table) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// Table returns the definition of one table, or nil.
func (s *Session) Table(name string) *Table { return s.catalog()[name] }

func (s *Session) catalog() map[string]*Table {
	if s.tx != nil {
		return s.tx.tables
	}
	tx, err := s.db.begin(true)
	if err != nil {
		return nil
	}
	defer tx.rollback()
	return tx.tables
}

// ---------------------------------------------------------------------------
// Transactions

// begin starts a transaction.
//
// Decoding the catalog for every statement would be slow, so the database
// caches it. A transaction sees every schema change committed before it
// began and none after, which is exactly the cached catalog if no schema
// change has committed since the cache was loaded. Transactions share the
// cached map until one changes the schema (see ownCatalog).
func (db *DB) begin(readOnly bool) (*txn, error) {
	db.mu.Lock()
	tx := &txn{db: db, startSeq: db.commitSeq}
	var err error
	if readOnly {
		tx.kv = db.store.BeginReadOnly()
	} else {
		tx.kv, err = db.store.Begin()
	}
	schemaSeq := db.schemaSeq
	cached := db.catalogValid && db.catalogSeq == schemaSeq
	tx.tables = db.catalog
	db.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if cached {
		return tx, nil
	}
	if err := tx.loadCatalog(); err != nil {
		tx.kv.Rollback()
		return nil, err
	}
	db.mu.Lock()
	if db.schemaSeq == schemaSeq {
		db.catalog, db.catalogSeq, db.catalogValid = tx.tables, schemaSeq, true
	}
	db.mu.Unlock()
	return tx, nil
}

// commit commits the transaction, or rolls it back if it would break the
// schema change rule.
//
// Snapshot isolation alone doesn't protect schema changes. CREATE INDEX
// fills the index from its snapshot, so a row that a concurrent
// transaction inserts would be missing from it; and rows inserted
// concurrently with DROP TABLE would be left behind. Neither collides on a
// key, so MVCC sees no conflict. The rule is coarse but safe: a transaction
// that changed the schema commits only if no other transaction has
// committed writes since it began, and a transaction that wrote data
// commits only if no schema change has committed since it began. In effect
// schema changes take a lock on the whole database, checked at commit.
func (tx *txn) commit() error {
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.kv.Wrote() {
		switch {
		case tx.ddl && db.commitSeq != tx.startSeq:
			tx.kv.Rollback()
			return fmt.Errorf("%w: other transactions wrote data while this one changed the schema", ErrConflict)
		case !tx.ddl && db.schemaSeq > tx.startSeq:
			tx.kv.Rollback()
			return fmt.Errorf("%w: another transaction changed the schema", ErrConflict)
		}
	}
	if err := tx.kv.Commit(); err != nil {
		return err
	}
	if tx.kv.Wrote() {
		db.commitSeq++
		if tx.ddl {
			db.schemaSeq = db.commitSeq
		}
	}
	return nil
}

func (tx *txn) rollback() error { return tx.kv.Rollback() }

func (tx *txn) run(s parser.Statement) (*Result, error) {
	switch s := s.(type) {
	case *parser.CreateTable:
		return tx.createTable(s)
	case *parser.DropTable:
		return tx.dropTable(s)
	case *parser.CreateIndex:
		return tx.createIndex(s)
	case *parser.DropIndex:
		return tx.dropIndex(s)
	case *parser.Insert:
		return tx.insert(s)
	case *parser.Update:
		return tx.update(s)
	case *parser.Delete:
		return tx.delete(s)
	case *parser.Select:
		return tx.selectRows(s)
	case *parser.Explain:
		return tx.explain(s.Query)
	}
	return nil, fmt.Errorf("unsupported statement %T", s)
}

// ---------------------------------------------------------------------------
// Storage access

func (tx *txn) scan(start, end []byte) *mvcc.Iterator { return tx.kv.Scan(start, end) }

// deleteRange removes every key in [start, end). Deleted keys vanish from
// the transaction's own view, so each pass rescans from the start.
func (tx *txn) deleteRange(start, end []byte) error {
	for {
		var keys [][]byte
		it := tx.scan(start, end)
		for len(keys) < 256 {
			k, _, ok, err := it.Next()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			keys = append(keys, k)
		}
		if len(keys) == 0 {
			return nil
		}
		for _, k := range keys {
			if _, err := tx.kv.Delete(k); err != nil {
				return err
			}
		}
	}
}

func checkKeySize(key []byte, what string) error {
	if mvcc.CheckKey(key) != nil {
		return fmt.Errorf("%s is too long to store in a key (%d bytes encoded)", what, len(key))
	}
	return nil
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

func (tx *txn) createTable(s *parser.CreateTable) (*Result, error) {
	if err := checkName("table", s.Name); err != nil {
		return nil, err
	}
	if _, ok := tx.tables[s.Name]; ok {
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
	id, err := tx.nextID()
	if err != nil {
		return nil, err
	}
	t.ID = id
	if err := tx.saveTable(t); err != nil {
		return nil, err
	}
	return &Result{Message: "CREATE TABLE"}, nil
}

func (tx *txn) dropTable(s *parser.DropTable) (*Result, error) {
	t, ok := tx.tables[s.Name]
	if !ok {
		if s.IfExists {
			return &Result{Message: "DROP TABLE"}, nil
		}
		return nil, fmt.Errorf("table %q does not exist", s.Name)
	}
	for _, idx := range t.Indexes {
		p := indexPrefix(idx.ID)
		if err := tx.deleteRange(p, prefixEnd(p)); err != nil {
			return nil, err
		}
	}
	p := rowPrefix(t.ID)
	if err := tx.deleteRange(p, prefixEnd(p)); err != nil {
		return nil, err
	}
	if _, err := tx.kv.Delete(catalogKey(t.Name)); err != nil {
		return nil, err
	}
	tx.ownCatalog()
	delete(tx.tables, t.Name)
	return &Result{Message: "DROP TABLE"}, nil
}

func (tx *txn) createIndex(s *parser.CreateIndex) (*Result, error) {
	if err := checkName("index", s.Name); err != nil {
		return nil, err
	}
	if t, _ := tx.findIndex(s.Name); t != nil {
		if s.IfNotExists {
			return &Result{Message: "CREATE INDEX"}, nil
		}
		return nil, fmt.Errorf("index %q already exists", s.Name)
	}
	t, err := tx.table(s.Table)
	if err != nil {
		return nil, err
	}
	col := t.column(s.Column)
	if col < 0 {
		return nil, fmt.Errorf("column %q of table %q does not exist", s.Column, s.Table)
	}
	id, err := tx.nextID()
	if err != nil {
		return nil, err
	}
	idx := &Index{ID: id, Name: s.Name, Column: col, Unique: s.Unique}

	// Backfill the index from the existing rows. A unique index fails here if
	// the column already holds duplicates.
	it := tx.scan(rowPrefix(t.ID), prefixEnd(rowPrefix(t.ID)))
	for {
		k, v, ok, err := it.Next()
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
		if err := tx.addIndexEntry(t, idx, row, k[len(rowPrefix(t.ID)):]); err != nil {
			return nil, err
		}
	}
	t2 := *t
	t2.Indexes = append(slices.Clip(t.Indexes), idx)
	if err := tx.saveTable(&t2); err != nil {
		return nil, err
	}
	return &Result{Message: "CREATE INDEX"}, nil
}

func (tx *txn) dropIndex(s *parser.DropIndex) (*Result, error) {
	t, i := tx.findIndex(s.Name)
	if t == nil {
		if s.IfExists {
			return &Result{Message: "DROP INDEX"}, nil
		}
		return nil, fmt.Errorf("index %q does not exist", s.Name)
	}
	p := indexPrefix(t.Indexes[i].ID)
	if err := tx.deleteRange(p, prefixEnd(p)); err != nil {
		return nil, err
	}
	t2 := *t
	t2.Indexes = slices.Delete(slices.Clone(t.Indexes), i, i+1)
	if err := tx.saveTable(&t2); err != nil {
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
func (tx *txn) primaryKey(t *Table, row Row) ([]byte, error) {
	if t.PK >= 0 {
		return appendKey(nil, row[t.PK]), nil
	}
	id, err := tx.db.store.Sequence(rowIDKey(t.ID))
	if err != nil {
		return nil, err
	}
	return appendKey(nil, int64(id)), nil
}

// writeRow stores a row and its index entries. The caller has already
// checked the row with checkRow.
func (tx *txn) writeRow(t *Table, row Row, pk []byte) error {
	key := rowKey(t.ID, pk)
	if err := checkKeySize(key, "primary key value"); err != nil {
		return err
	}
	if t.PK >= 0 {
		_, dup, err := tx.kv.Get(key)
		if err != nil {
			return err
		}
		if dup {
			return fmt.Errorf("duplicate key value violates primary key: %s = %s already exists", t.Columns[t.PK].Name, literal(row[t.PK]))
		}
	}
	val := encodeRow(row)
	if len(val) > mvcc.MaxValueSize {
		return fmt.Errorf("row is too large (%d bytes; the limit is %d)", len(val), mvcc.MaxValueSize)
	}
	if err := tx.kv.Put(key, val); err != nil {
		return err
	}
	for _, idx := range t.Indexes {
		if err := tx.addIndexEntry(t, idx, row, pk); err != nil {
			return err
		}
	}
	return nil
}

func (tx *txn) deleteRow(t *Table, row Row, pk []byte) error {
	if _, err := tx.kv.Delete(rowKey(t.ID, pk)); err != nil {
		return err
	}
	for _, idx := range t.Indexes {
		if _, err := tx.kv.Delete(indexKey(idx, row[idx.Column], pk)); err != nil {
			return err
		}
	}
	return nil
}

func (tx *txn) addIndexEntry(t *Table, idx *Index, row Row, pk []byte) error {
	v := row[idx.Column]
	key := indexKey(idx, v, pk)
	if err := checkKeySize(key, fmt.Sprintf("value of indexed column %q", t.Columns[idx.Column].Name)); err != nil {
		return err
	}
	// Several NULLs may share a unique index: NULL is never equal to NULL.
	if idx.Unique && v != nil {
		_, dup, err := tx.kv.Get(key)
		if err != nil {
			return err
		}
		if dup {
			return fmt.Errorf("duplicate key value violates unique index %q: %s = %s already exists", idx.Name, t.Columns[idx.Column].Name, literal(v))
		}
	}
	return tx.kv.Put(key, pk)
}

func (tx *txn) insert(s *parser.Insert) (*Result, error) {
	t, err := tx.table(s.Table)
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
		pk, err := tx.primaryKey(t, row)
		if err != nil {
			return nil, err
		}
		if err := tx.writeRow(t, row, pk); err != nil {
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
func (tx *txn) matchRows(t *Table, where parser.Expr) (rows []Row, pks [][]byte, err error) {
	plan, err := tx.planTableScan(t, t.Name, where, true)
	if err != nil {
		return nil, nil, err
	}
	it, err := plan.open(tx)
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

func (tx *txn) delete(s *parser.Delete) (*Result, error) {
	t, err := tx.table(s.Table)
	if err != nil {
		return nil, err
	}
	rows, pks, err := tx.matchRows(t, s.Where)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if err := tx.deleteRow(t, rows[i], pks[i]); err != nil {
			return nil, err
		}
	}
	return &Result{Message: fmt.Sprintf("DELETE %d", len(rows))}, nil
}

func (tx *txn) update(s *parser.Update) (*Result, error) {
	t, err := tx.table(s.Table)
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

	rows, pks, err := tx.matchRows(t, s.Where)
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
		if err := tx.deleteRow(t, old, pks[i]); err != nil {
			return nil, err
		}
	}
	for i, row := range newRows {
		if err := tx.writeRow(t, row, newPKs[i]); err != nil {
			return nil, err
		}
	}
	return &Result{Message: fmt.Sprintf("UPDATE %d", len(rows))}, nil
}
