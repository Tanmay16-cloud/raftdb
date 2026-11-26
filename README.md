# raftdb

A distributed SQL database built from scratch in Go: a SQL front end, a
B+ tree storage engine with a write-ahead log, MVCC transactions, and Raft
replication across nodes.

> Status: **Milestone 4 of 7 complete.** raftdb runs SQL on a single node:
> tables, secondary indexes, joins, aggregates and transactions, planned by a
> rule-based optimizer and executed on a crash-safe storage engine.

## Architecture (target)

```
 client ──gRPC──▶ SQL layer        lexer → parser → planner → executor
                     │             rows and indexes encoded as key/value pairs
                     ▼
                  Transactions     MVCC, snapshot isolation
                     │
                     ▼
                  Raft             leader election, log replication, snapshots
                     │
                     ▼
                  Storage engine   B+ tree · LRU buffer pool · write-ahead log
                     │
                     ▼
                  data file + WAL file (4 KB pages)
```

## Roadmap

- [x] **1. Storage engine.** Disk-backed B+ tree over 4 KB pages: point
      lookups, inserts, overwrites, deletes and ordered range scans.
- [x] **2. Durability.** Write-ahead log with checksummed frames, atomic
      commits, crash recovery, checkpoints, and a bounded LRU buffer pool.
      Verified by randomized power-failure simulation.
- [x] **3. SQL front end.** Hand-written lexer and recursive-descent parser
      with precedence climbing, producing an AST that prints back as
      canonical SQL. Fuzz-tested for crashes and round-trip stability.
- [x] **4. Executor.** Rows and indexes encoded onto the key/value store,
      a planner that chooses between scans, key lookups, index range scans,
      hash joins and nested-loop joins, `EXPLAIN`, and atomic statements and
      transactions. Verified by differential testing of index plans against
      full scans.
- [ ] **5. Transactions.** MVCC with snapshot isolation.
- [ ] **6. Replication.** Raft over gRPC, tested with injected partitions and
      crashes.
- [ ] **7. Server and benchmarks.** gRPC server, client, throughput and
      failover-latency numbers.

## Usage

```sh
go test ./...                                    # run the test suite
go test ./internal/storage -run=XXX -bench=.     # run the benchmarks
go run ./cmd/raftdb -db demo.db                  # SQL shell
go test ./internal/parser -fuzz=FuzzParse -fuzztime=60s   # fuzz the parser
```

The shell reads statements ending in `;`, which may span lines, and has a
few backslash commands: `\d` lists tables, `\d <table>` shows a schema,
`\parse <sql>` shows the parser's reading of a statement, `\timing`,
`\stats`, `\q`.

```
raftdb=> CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, city TEXT);
raftdb=> CREATE INDEX users_age ON users (age);
raftdb=> INSERT INTO users VALUES (1, 'Asha', 22, 'Pilani'), (2, 'Rohan', 17, 'Goa'),
raftdb->                          (3, 'Meera', 30, 'Pilani'), (4, 'Kabir', NULL, 'Delhi');
INSERT 4
raftdb=> CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amount FLOAT);
raftdb=> INSERT INTO orders VALUES (10, 1, 99.5), (11, 1, 10), (12, 3, 5.25);
raftdb=> SELECT name, age FROM users WHERE age BETWEEN 18 AND 30 ORDER BY age;
name  | age
------+----
Asha  |  22
Meera |  30
(2 rows)
raftdb=> EXPLAIN SELECT name, age FROM users WHERE age BETWEEN 18 AND 30 ORDER BY age;
                              QUERY PLAN
----------------------------------------------------------------------
Sort: age
-> Project: name, age
   -> Filter: (age BETWEEN 18 AND 30)
      -> Index Scan using users_age on users ((age BETWEEN 18 AND 30))
raftdb=> SELECT u.name, count(*) AS orders, sum(o.amount) AS total
raftdb-> FROM users u JOIN orders o ON u.id = o.uid
raftdb-> GROUP BY u.name ORDER BY total DESC;
name  | orders | total
------+--------+------
Asha  |      2 | 109.5
Meera |      1 |  5.25
(2 rows)
raftdb=> EXPLAIN SELECT u.name, sum(o.amount) AS total FROM users u JOIN orders o ON u.id = o.uid GROUP BY u.name;
            QUERY PLAN
----------------------------------
Project: name, total
-> Hash Aggregate: group by u.name
   -> Hash Join: (u.id = o.uid)
      -> Seq Scan on users u
      -> Seq Scan on orders o
raftdb=> INSERT INTO users VALUES (2, 'Dev', 40, 'Goa');
ERROR: duplicate key value violates primary key: id = 2 already exists
```

## Executor (Milestone 4)

`internal/engine` runs SQL on top of the storage engine. It has four parts:
a catalog, an encoding of rows and indexes as keys and values, a planner,
and an executor.

### Everything is a key

The whole database lives in the B+ tree's single ordered key space:

| Key | Value |
|---|---|
| `c/<table name>` | table definition (JSON) |
| `s/next-id` | the next table or index ID |
| `r` + table ID + primary key | the row |
| `x` + index ID + indexed value + primary key | the primary key |
| `q` + table ID | the next hidden row ID (tables without a primary key) |

IDs are 4-byte big-endian, so a table's rows, and an index's entries, sit
next to each other and a prefix scan reads exactly them.

Primary keys and indexed values use an **order-preserving encoding**:
comparing two encodings byte by byte gives the same answer as comparing the
values. That is what lets a B+ tree range scan answer `WHERE age BETWEEN 18
AND 30`.
- Integers are 8 bytes big-endian with the sign bit flipped, so negative
  numbers sort first.
- Floats are their IEEE 754 bits, with all bits flipped for negative numbers
  and only the sign bit for positive ones. `-0.0` is normalised to `0.0`.
- Text has each `0x00` escaped as `0x00 0xFF` and ends with `0x00 0x01`.
  The terminator sorts below every escaped byte, so `"ab"` < `"abc"`. No
  encoding is a prefix of another, which matters because index keys have the
  primary key appended: without it, `("a", pk "b…")` could sort after
  `("ab", …)`.
- `NULL`, `FALSE` and `TRUE` are single tag bytes.

Appending the primary key to each index key keeps duplicate values distinct,
and the entry's value is the primary key, so an index scan fetches each row
with one more lookup. Rows themselves use a compact format (varints and
length-prefixed strings) that need not preserve order.

### Planning

A `SELECT` becomes a tree of operators, built bottom-up:

```
Limit → Sort → Distinct → Project → Filter (HAVING) → Hash Aggregate
      → joins → scans with their own filters
```

**Predicate pushdown.** The `WHERE` clause, and every `ON` clause (all joins
are inner joins), is split into its `AND`-ed conjuncts. Each one goes as low
in the tree as it can. A condition on one table filters that table's scan,
and may choose how the table is read. A condition linking tables runs at the
first join where all of them are present.

**Access paths.** For each table the planner picks, by rule (there are no
statistics yet), the first of:

1. primary key = constant (**Primary Key Lookup**: one row)
2. unique indexed column = constant
3. indexed column = constant (**Index Scan**)
4. a range on the primary key (**Primary Key Range Scan**)
5. a range on an indexed column
6. a full scan (**Seq Scan**)

It recognises `col op constant`, `constant op col` and `BETWEEN`, and
combines several conditions on one column into the tightest range. A
condition that can't be turned into a range is left to the filter, such as
`age + 1 = 23`, an `OR`, or `age = 22.5` on an `INT` column. The filter
also re-checks every condition the range already guarantees, so the
access-path code is free to be approximate but never wrong.

**Joins.** Tables are joined left-deep, in the order written. If a join
condition has the form `left_expr = right_expr`, it uses a **hash join**: it
builds a hash table on the new table's key and probes it with each row so
far. Otherwise it uses a **nested-loop join**. `EXPLAIN` prints the chosen
tree, in the style of PostgreSQL.

### Execution

Operators follow the **iterator (Volcano) model**: each one pulls rows from
its inputs one at a time. Scans, filters, projections and limits stream.
Sort, aggregation, `DISTINCT` and a hash join's build side must see all
their input first.

Semantics follow PostgreSQL where it matters:
- **NULL and three-valued logic.** `NULL = NULL` is `NULL`, `WHERE` keeps
  only `TRUE`, and `false AND NULL` is `false`.
- **Ordering.** `NULL`s sort last.
- **Unique indexes.** Any number of `NULL`s may share a unique index.
- **Types.** `INT` widens to `FLOAT` and nothing else converts implicitly.
- **Arithmetic.** Integer overflow and division by zero are errors.
- **Grouping.** Column references are checked against `GROUP BY`.

**Atomic statements and transactions.** Outside `BEGIN`, each statement
commits on its own. If it fails, the storage engine rolls back everything
since the last commit, so a multi-row `INSERT` that hits a duplicate on its
third row leaves nothing behind, index entries included. Inside
`BEGIN … COMMIT`, an error rolls back the whole transaction, because the
storage engine can only roll back to the last commit.

**UPDATE and DELETE** collect the matching rows before changing any, which
avoids the "Halloween problem": an update that moves a row forward in the
index being scanned would otherwise meet that row again. An `UPDATE` then
deletes every old row before writing any new one. So primary key and unique
checks see the statement's final state, and `UPDATE t SET id = id + 1`
succeeds whatever order the rows were read in. PostgreSQL, checking row by
row, can fail on that statement.

### Testing the executor

- **Encoding properties.** For 200k random pairs of values (extreme
  integers, signed zeros, infinities, strings with NUL bytes), encoded order
  must equal value order, and no encoding may be a prefix of another. Rows
  must round-trip exactly, and every truncation of a row must be rejected.
- **SQL tests.** About 45 queries checked for exact results. `EXPLAIN`
  checks that the planner picks the right access path and join. About 40
  error messages are pinned down, and the tests check that none of the
  failing statements changed anything.
- **Atomicity, transactions and persistence.** Partial inserts and updates
  leave no trace. Rollback undoes schema changes, and `DROP TABLE` really
  removes the rows from storage. After a reopen, the catalog and indexes are
  still there, IDs are not reused, and an uncommitted transaction is gone.
- **Differential testing** (`random_test.go`). Two databases receive the same
  random inserts, updates, deletes and transactions; only one has secondary
  indexes. Every random query must return the same rows from both, so every
  index plan is checked against a full scan. After every few hundred steps,
  each index must hold exactly one correct entry per row. An 8-page buffer
  pool keeps the storage engine evicting throughout.

The differential test found two real bugs:
- **Rollback kept uncommitted data.** A dirty page evicted to the log and then
  read back sat in the buffer pool marked clean, so `Rollback`, which dropped
  only dirty pages, kept it.
- **`SET id = id + 1` could fail.** It succeeded or failed depending on
  whether the plan read rows in key order or index order.

Both now have targeted regression tests.

Statement coverage: about 86%.

### Known limitations
- The planner is rule-based. It has no statistics, does no cost estimates
  and doesn't reorder joins.
- Only inner joins; no outer joins or subqueries.
- Sorting, aggregation, hash joins and `UPDATE`/`DELETE` hold their input
  in memory.
- An index covers one column.
- Errors inside a transaction abort it, since there are no savepoints.

## SQL parser (Milestone 3)

`internal/parser` turns SQL text into an abstract syntax tree in two stages.

**Lexer** (`lexer.go`). Splits the text into tokens: words, quoted
identifiers, integers, floats, strings and symbols. It skips `--` and
`/* */` comments and records each token's line and column for error messages.
String literals are scanned byte by byte, so any bytes inside quotes come
through unchanged.

**Parser** (`parser.go`). Hand-written recursive descent: one function per
grammar rule (`parseSelect`, `parseCreateTable`, ...). Expressions use
**precedence climbing** (a Pratt parser). Each operator has a precedence, and
the parser keeps folding operators into the left operand while the next one
binds at least as tightly as the current minimum:

| Precedence (low to high) | Operators |
|---|---|
| 1 | `OR` |
| 2 | `AND` |
| 3 | prefix `NOT` |
| 4 | `= != <> < <= > >=`, `IS [NOT] NULL`, `[NOT] IN`, `[NOT] LIKE`, `[NOT] BETWEEN` |
| 5 | `\|\|` |
| 6 | `+ -` |
| 7 | `* / %` |
| 8 | prefix `-` |

So `NOT a = 1 AND b` parses as `((NOT (a = 1)) AND b)`, and `1 - 2 - 3` as
`((1 - 2) - 3)`.

**Supported SQL:** `CREATE TABLE [IF NOT EXISTS]` (types `INT`, `FLOAT`,
`TEXT`, `BOOL` and common synonyms; `PRIMARY KEY`, `NOT NULL`),
`DROP TABLE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, multi-row `INSERT`,
`SELECT [DISTINCT]` with aliases, `JOIN ... ON`, comma joins, `WHERE`,
`GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, and function calls
including `count(*)`; also `UPDATE`, `DELETE`, `BEGIN`, `COMMIT` and `ROLLBACK`.

**Keywords.** Reserved words (`SELECT`, `FROM`, `WHERE`, ...) can only be
used as names when quoted. Contextual keywords (`KEY`, `INDEX`, `IF`, `ASC`,
...) work as ordinary names too, because the parser only treats them as
keywords in the places they can appear. Like PostgreSQL, unquoted names fold
to lower case and quoted names keep their exact spelling.

**Syntax only.** The parser doesn't check whether tables or columns exist,
whether types match, or whether a table has two primary keys. Those checks
need the schema, so they belong to the planner in Milestone 4.

**Canonical form.** Every node has a `String()` method that prints canonical
SQL: keywords upper case, every compound expression parenthesised, and
synonyms normalised (`<>` becomes `!=`, `VARCHAR` becomes `TEXT`).

**Testing.** Table-driven tests check the canonical form of about 40
statements (which pins down precedence), the exact tree for a sample query,
and the exact message and position of about 20 syntax errors. The key
property is the **round trip**: parsing the canonical form of any statement
must give an identical tree. `FuzzParse` checks this, along with "never
panic" and "errors are always `*parser.Error`", on random inputs: 4.7 million
of them in a 2-minute run, with no failures.

## Storage engine design

### B+ tree (Milestone 1)

**Nodes.** Leaves store sorted `(key, value)` pairs and a pointer to the next
leaf, so a range scan descends once and then walks sideways. Internal nodes
store separator keys and child pointers; `children[i]` holds keys below
`keys[i]`, and `children[i+1]` holds keys at or above it.

**Splitting.** Keys (up to 256 bytes) and values (up to 1 KB) vary in length,
so an overfull node is split at the point that best balances the two halves
**by bytes**, not by key count. A count-based split can leave one half still
too large for a page; with the size limits above, a byte-balanced split
always produces two halves that fit. Leaf splits copy the first right-hand key
up to the parent; internal splits move the middle key up. When the root
splits, the tree grows one level taller.

### Durability (Milestone 2)

Two files: `<name>.db` holds the pages (page 0 is a meta page with the root
page and page count), and `<name>.db-wal` is the **write-ahead log**.

**The write path:**

```
Put ──▶ buffer pool (page marked dirty)
Sync ─▶ append every dirty page to the WAL; the last frame is the commit record
        └─ fsync the WAL                       ◀── the commit is durable here
        └─ if the WAL is long: checkpoint
              copy the newest version of each page into the data file
              fsync the data file
              reset the WAL (truncate, new salt, fsync)
```

**Commits are atomic.** Everything changed since the previous `Sync` is one
transaction. After a crash, the database holds every change up to some commit
and nothing after it.

**WAL frames** hold a full 4 KB page image plus a 24-byte header: page ID,
commit flag, root and page count (for commit frames), a **salt**, and a
**CRC-32C checksum**. Recovery reads frames in order and stops at the first
frame whose checksum or salt doesn't match. That way:
- a frame torn by a crash, or flipped bits, ends the log there;
- frames after the last commit record are ignored (that transaction never
  finished);
- frames left over from before a log reset carry an old salt and are ignored,
  even if a crash undid the truncation.

**Redo-only recovery.** The data file only ever receives committed pages,
and only during a checkpoint. When the buffer pool must evict a dirty,
uncommitted page, the page goes into the WAL, not the data file, and later
reads look it up there through an in-memory index. So recovery never has to
undo anything: it copies committed frames into the data file and discards the
rest. Interrupting recovery is harmless, because running it again copies the
same pages.

**Rollback** discards everything since the last commit. It drops the pool's
dirty pages, plus any page read back from an uncommitted log frame, then
rewinds the end of the log to the last commit record and restores the root
and page count. The next transaction overwrites the discarded frames in
place, which creates a subtle hazard. Say a crash keeps a new commit frame
but loses a new frame before it. A discarded frame would then still sit in
between, with the right salt and a valid checksum of its own. To rule this
out, **checksums are chained**: each frame's CRC also covers the previous
frame's CRC. The discarded frame was computed from a different predecessor,
so recovery rejects it. `TestDiscardedFrameIsNotReplayedAfterRollback`
reproduces exactly this crash.

**Full page images** also protect against torn writes in the data file: if a
crash leaves a page half-written during a checkpoint, the log still has the
complete page and recovery rewrites it.

**Ordering rules** that make this correct:
1. A commit is acknowledged only after the WAL fsync returns.
2. The data file is fsynced *before* the WAL is reset.
3. The meta page is never trusted over the WAL: if the log has a commit,
   its root and page count win.

**Fail-stop on I/O errors.** After any write or fsync error, the pager
refuses further work until the database is reopened and recovered. Retrying
a failed fsync is unsafe, because the kernel may already have discarded the
unwritten data. PostgreSQL learned this in 2018 ("fsyncgate").

**Buffer pool.** A fixed number of pages (default 1024 = 4 MB) with LRU
eviction, built from a hash map plus a doubly linked list, so every operation
is O(1). Pages don't need pinning, because the tree decodes each page into its
own memory and replaces page buffers instead of modifying them.

### Known limitations
- Deletes never merge nodes or free pages (rebalancing and a free-page list
  are future work).
- A single mutex serialises all operations.
- Every commit costs a separate fsync; group commit would batch them.
- Creating a database doesn't fsync the parent directory, which a fully
  durable implementation on Linux would need.

## Storage testing

- **Model-based tests:** random puts, deletes, commits, rollbacks and reopens checked
  against a Go map. One configuration keeps only 4 pages in the buffer pool and
  checkpoints every 20 frames, so evictions and checkpoints happen constantly.
- **Crash simulation** (`memfs_test.go`): an in-memory file system tracks
  which writes each fsync has made durable. A simulated power failure keeps
  the durable data plus a random subset of later writes, some torn at
  512-byte sector boundaries. `TestCrashRecovery` runs 300 random workloads,
  cuts the power at a random write, sometimes crashes again *during
  recovery*, and checks the result is exactly the last committed state.
- **Targeted recovery tests:** unsynced changes are lost, a log with no
  checkpoints rebuilds the whole database, and a torn or bit-flipped final
  commit is discarded without losing earlier commits.
- **Checking the tests themselves:** deliberately breaking the code (replaying
  uncommitted frames, skipping the data-file fsync before resetting the log,
  or skipping the log fsync on commit) makes the crash tests fail every time.

Statement coverage: about 88%.

## Benchmarks

i5-1235U laptop, Windows 11, 100-byte values, default 4 MB buffer pool:

| Operation | Time per op | Notes |
|---|---|---|
| Put (random keys) | ~21 µs | the data (~12 MB) is larger than the pool, so evictions go to the WAL |
| Put + Sync each time | ~1.0–1.3 ms | dominated by fsync: about 1,000 commits/sec |
| Get (100k keys) | ~21 µs | includes buffer-pool misses |

The gap between the first two rows is why databases use group commit: batching
many commits into one fsync.
