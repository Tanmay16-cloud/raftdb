# raftdb

A distributed SQL database built from scratch in Go: a SQL front end, a
B+ tree storage engine with a write-ahead log, MVCC transactions, and Raft
replication across nodes.

> Status: **Milestone 3 of 7 complete.** The storage engine is crash-safe and
> runs in bounded memory, and the SQL parser is fuzz-tested.

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
- [ ] **4. Executor.** Row encoding onto the key/value store, secondary
      indexes, a planner that chooses between scans and index lookups.
- [ ] **5. Transactions.** MVCC with snapshot isolation.
- [ ] **6. Replication.** Raft over gRPC, tested with injected partitions and
      crashes.
- [ ] **7. Server and benchmarks.** gRPC server, client, throughput and
      failover-latency numbers.

## Usage

```sh
go test ./...                                    # run the test suite
go test ./internal/storage -run=XXX -bench=.     # run the benchmarks
go run ./cmd/raftdb -db demo.db                  # interactive shell
go test ./internal/parser -fuzz=FuzzParse -fuzztime=60s   # fuzz the parser
```

```
> put user:1 Asha
> put user:2 Rohan
> scan user: user;
user:1 = Asha
user:2 = Rohan
(2 keys)
> parse SELECT name FROM users WHERE age > 18 AND city = 'Pilani' OR NOT active
*parser.Select
  SELECT name FROM users WHERE (((age > 18) AND (city = 'Pilani')) OR (NOT active))
> parse SELECT * FROM users WHERE
error: line 1, column 26: expected expression, got end of input
```

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

## Testing

- **Model-based tests:** random puts, deletes, commits and reopens checked
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
