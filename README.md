# raftdb

A distributed SQL database built from scratch in Go: a SQL front end, a
B+ tree storage engine with a write-ahead log, MVCC transactions, and Raft
replication across nodes.

> Status: **Milestone 5 of 7 complete.** raftdb runs SQL on a single node:
> tables, secondary indexes, joins and aggregates, planned by a rule-based
> optimizer, with concurrent sessions isolated by MVCC (snapshot isolation),
> on a crash-safe storage engine.

## Architecture (target)

```
 client ──gRPC──▶ SQL layer        lexer → parser → planner → executor
                     │             rows and indexes encoded as key/value pairs
                     ▼
                  Transactions     MVCC versions, snapshot isolation
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
- [x] **5. Transactions.** MVCC with snapshot isolation: concurrent
      sessions, readers that never block, write-conflict detection,
      crash recovery of unfinished transactions, and `VACUUM`. Verified
      against a reference model of snapshot isolation, with injected
      crashes.
- [ ] **6. Replication.** Raft over gRPC, tested with injected partitions and
      crashes.
- [ ] **7. Server and benchmarks.** gRPC server, client, throughput and
      failover-latency numbers.

## Usage

```sh
go test ./...                                    # run the test suite
go test ./internal/storage -run=XXX -bench=.     # storage benchmarks
go test ./internal/engine -run=XXX -bench=.      # SQL benchmarks
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

The whole database lives in one ordered key space, which the MVCC layer
(Milestone 5) versions:

| Key | Value |
|---|---|
| `c/<table name>` | table definition (JSON) |
| `r` + table ID + primary key | the row |
| `x` + index ID + indexed value + primary key | the primary key |
| `x` + index ID + indexed value (unique index, non-NULL value) | the primary key |

IDs are 4-byte big-endian, so a table's rows, and an index's entries, sit
next to each other and a prefix scan reads exactly them. Table and index
IDs, and the hidden row IDs of tables without a primary key, come from
non-transactional sequences, so they are never reused.

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

Appending the primary key to each index key keeps duplicate values
distinct. A unique index leaves it off, so that two concurrent transactions
inserting the same value write the same key and MVCC catches the conflict.
NULLs, any number of which may share a unique index, keep it. An entry's
value is the primary key, so an index scan fetches each row with one more
lookup. Rows themselves use a compact format (varints and
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
runs in a transaction of its own (read-only for queries). So a multi-row
`INSERT` that hits a duplicate on its third row leaves nothing behind,
index entries included. Inside `BEGIN … COMMIT`, an error rolls back the
whole transaction, since there are no savepoints.

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
  pool keeps the storage engine evicting throughout, and a `VACUUM` every
  50 steps must not change any result.

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

## Transactions: MVCC and snapshot isolation (Milestone 5)

`internal/mvcc` sits between the SQL layer and the B+ tree, and lets many
sessions run transactions at once. It uses **multi-version concurrency
control**:
- every write creates a new version of a key instead of overwriting it;
- each transaction reads from a **snapshot** of the database as of when it
  began.

So readers never block writers, writers never block readers, and a
transaction sees one consistent state however long it runs. This is the
approach of PostgreSQL and of CockroachDB-style key/value stores.

### Versions and visibility

Each read-write transaction takes a version number from a counter when it
begins, and stamps every version it writes with it. It also records which
transactions were still active at that moment. A version written by
transaction `v` is visible to transaction `t` if:

```
v == t.version                                    t's own write, or
v <  t.version and v was not active when t began  committed before t
```

A rolled-back transaction's versions are deleted, so no one ever sees them.
Committing changes no data at all: the transaction just leaves the active
set, so snapshots taken after that include it. Read-only transactions (all
autocommit `SELECT`s) take a snapshot without consuming a version or writing
anything.

Versions live in the tree under `v` + escaped key + 8-byte version. The key
is escaped the same prefix-free, order-preserving way as text values, so all
versions of a key are adjacent, oldest first. The versions of every key in
`[start, end)` then form one contiguous range, which a scan reads in order,
keeping the newest visible version of each key.

### Conflicts: first writer wins

Before writing a key, a transaction looks at the key's newest version. If
that version isn't visible to it, another transaction either committed the
key after this one began or is still writing it. The write then fails with
a serialization error, and the client retries. In practice:

| Session A | Session B | |
|---|---|---|
| `BEGIN` | `BEGIN` | |
| `UPDATE accounts SET balance = balance - 30 WHERE id = 1` | | |
| | `UPDATE accounts SET balance = balance - 20 WHERE id = 1` | **error**: could not serialize access due to a concurrent update |
| `COMMIT` | | balance is 70, not a lost update |

The same mechanism enforces uniqueness across sessions. Two concurrent
inserts of the same primary key, or of the same value in a unique index,
write the same key, so the second one fails.

Snapshot isolation is **not serializable**: **write skew** is still possible.
Two doctors are on call, and at least one must stay. Each transaction checks
that the other doctor is on call and signs its own doctor off. The two
transactions write different rows, so neither conflicts, and both commit.
`TestWriteSkewIsPossible` demonstrates this. Serializable snapshot isolation,
which also tracks read-write dependencies, is future work.

### Schema changes

MVCC alone doesn't make schema changes safe. `CREATE INDEX` fills the index
from its snapshot, so it would miss a row inserted by a concurrent
transaction. Rows inserted concurrently with `DROP TABLE` would be left
behind. Neither case collides on a key, so MVCC sees no conflict.

raftdb uses a coarse rule, checked at commit:
- a transaction that changed the schema commits only if no other
  transaction committed writes since it began;
- a transaction that wrote data commits only if no schema change committed
  since it began.

In effect, DDL takes a lock on the whole database. Each transaction reads
the catalog through its own snapshot, so uncommitted schema changes are
invisible to other sessions. The decoded catalog is cached per committed
schema version, so statements don't decode it every time.

### Durability and recovery

A transaction touches storage only on its first write. Before its first
version, it records the version counter, so a crash can't hand that number
out again, and an **active marker**. Commit deletes the marker and fsyncs,
and only then leaves the in-memory active set, so no one can see the writes
before they are durable. Transactions that wrote nothing never fsync.

Each transaction keeps the set of keys it wrote in memory, for `ROLLBACK`.
But every commit flushes the whole tree, including other transactions'
uncommitted versions. After a crash, opening the database therefore deletes
every version whose transaction is still marked active. That costs one scan
over all versions after a crash, in exchange for writing nothing but the
versions themselves during normal operation. An earlier design wrote a
per-key undo record instead, and the extra writes made bulk inserts about
1.8x slower.

### Garbage collection: VACUUM

Old versions accumulate, so `VACUUM` removes the ones no transaction can
ever see again. The **horizon** is the oldest version that any open
transaction might still need: the smallest of each open transaction's own
version and the versions in its snapshot. For each key, only the newest
version below the horizon matters to anyone. Older ones are deleted, and so
is that newest one if it records a deletion. Vacuum doesn't run
automatically yet. Until it does, tables that see many updates grow, and
every read scans past the dead versions.

### Testing MVCC

- **Model-based test** (`mvcc_test.go`). Random interleavings of
  overlapping transactions, read-write and read-only, run against a reference model of snapshot
  isolation that knows nothing about versions. In the model:
  - each transaction reads the committed state as of its start, overlaid
    with its own writes;
  - a write conflicts exactly when another transaction wrote the same key
    and is still open, or committed after this one began.

  The store must agree on every read, scan, delete and conflict. The run
  includes vacuums and two kinds of injected crash: a power cut that loses
  everything since the last fsync, and a crash after the storage engine has
  flushed every open transaction's versions. After each crash the store must
  hold exactly the committed state. Each seed runs 3,000 operations with a
  4-page buffer pool, and 100 seeds passed. The model test caught its own bug
  first: the model built a commit's new state from the transaction's
  snapshot instead of from the latest committed state.
- **Anomaly tests** (`internal/engine/mvcc_test.go`). With two sessions, the
  tests check:
  - no dirty reads, non-repeatable reads, or phantoms, including through an
    index;
  - lost updates and concurrent duplicate keys fail;
  - write skew does happen;
  - the schema-change rule holds in both orders;
  - a transaction left open when the database closes is rolled back on
    reopen;
  - `VACUUM` never breaks an open snapshot.
- **Concurrent bank transfers.** Four goroutines, each with its own session,
  move money between accounts and retry on conflicts. Meanwhile, two readers
  check that every snapshot sees the same total. A typical run commits 160
  transfers and retries about 200 conflicts.
- The engine's differential test also runs on top of MVCC, with `VACUUM`
  every 50 steps. 30 seeds passed.

The Go race detector needs a 64-bit C compiler, which the development
machine lacks. I reviewed the shared state by hand: the store's counters and
active set are guarded by its mutex, snapshots never change after `BEGIN`,
and cached catalog maps are copied before a schema change modifies them.

Statement coverage: about 86%.

### Known limitations
- Snapshot isolation, not serializable: write skew is possible.
- A conflict fails immediately instead of waiting for the other transaction,
  so clients must retry.
- Schema changes conflict with every concurrent write, on any table.
- `VACUUM` is manual and scans the whole database while holding the store's
  lock.
- Commits are serialized, one fsync each; group commit would batch them.
- After a crash, recovery scans every version.

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

**SQL layer** (`go test ./internal/engine -run=XXX -bench=.`). A table with
an integer primary key, a secondary index and a text column, compared with
Milestone 4, which had no MVCC:

| Operation | Milestone 4 | With MVCC | Notes |
|---|---|---|---|
| `INSERT`, one transaction each | ~1.3–1.9 ms | ~1.3–1.6 ms | fsync-bound either way |
| `INSERT`, 1,000 per transaction | ~86 µs | ~111 µs | each write first reads the key's versions to check for conflicts |
| point `SELECT` by primary key | ~26 µs | ~22 µs | read-only snapshots write nothing; the catalog is cached |

Most of what remains is the storage engine decoding a whole node on every
visit. A cache of decoded nodes is the obvious next optimisation.
