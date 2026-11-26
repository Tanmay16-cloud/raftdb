# raftdb

[![CI](https://github.com/Tanmay16-cloud/raftdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Tanmay16-cloud/raftdb/actions/workflows/ci.yml)

A distributed SQL database built from scratch in Go: a SQL front end, a
B+ tree storage engine with a write-ahead log, MVCC transactions, and Raft
replication across nodes.

> Status: **all 7 milestones complete.** raftdb runs SQL (tables, secondary
> indexes, joins and aggregates, planned by a rule-based optimizer) with
> concurrent sessions isolated by MVCC (snapshot isolation), on a crash-safe
> storage engine, replicated across nodes by Raft, served over gRPC to a
> client that finds the leader by itself. On a laptop, a three-node cluster
> serves about 31,000 point reads/s or 3,000 inserts/s, and resumes writes
> about 160 ms after its leader is killed.

## Architecture

```
 client ──gRPC──▶ SQL layer        lexer → parser → planner → executor
                     │             rows and indexes encoded as key/value pairs
                     ▼
                  Transactions     optimistic MVCC: read a snapshot, buffer writes,
                     │             commit as one deterministic command
                     ▼
                  Raft             leader election (pre-vote, check quorum),
                     │             log replication, linearizable ReadIndex reads
                     │   ◀──gRPC──▶ other nodes
                     ▼
                  Storage engine   B+ tree · LRU buffer pool · write-ahead log
                     │             (one tree for the Raft log, one for the data)
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
      sessions, readers that never block, optimistic transactions that
      commit as one deterministic command (ready for replication),
      first-committer-wins conflict detection, and `VACUUM`. Verified
      against a reference model of snapshot isolation, with injected
      crashes.
- [x] **6. Replication.** Raft over gRPC: elections with pre-vote and
      check-quorum, log replication, linearizable reads, and a persistent
      log. Verified by a deterministic cluster simulator with partitions,
      message loss, reordering, clock drift and crashes, checked by
      planting known Raft bugs; and end to end with SQL under chaos.
- [x] **7. Server and benchmarks.** A gRPC SQL server and a client that
      follows the leader and retries only when that is safe; group commit
      and parallel leader/follower fsyncs; `raftdb serve` / `raftdb
      connect`; and `raftbench`, measuring throughput, latency percentiles
      and failover time on a real cluster.

## Usage

Requires Go 1.26 or later (for gRPC).

```sh
go test ./...                                    # run the test suite
go test -short ./...                             # skip the slow chaos test
go test ./internal/storage -run=XXX -bench=.     # storage benchmarks
go test ./internal/engine -run=XXX -bench=.      # SQL benchmarks
go test ./internal/cluster -run=XXX -bench=.     # replicated benchmarks
go run ./cmd/raftdb -db demo.db                  # SQL shell (single node)
go test ./internal/parser -fuzz=FuzzParse -fuzztime=60s   # fuzz the parser
go run ./cmd/raftbench -workload insert -clients 16       # cluster benchmark
go run ./cmd/raftbench -workload insert -failover         # kill the leader halfway
```

A three-node cluster, one terminal per node, and a shell connected to it:

```sh
go build -o raftdb ./cmd/raftdb
./raftdb serve -id 1 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
./raftdb serve -id 2 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
./raftdb serve -id 3 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
./raftdb connect -cluster 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
```

The shell works the same against a cluster; `\stats` shows the Raft state
of the node it is connected to. Kill the leader's process and keep typing:
the shell finds the new leader.

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
transaction sees one consistent state however long it runs.

### Versions and snapshots

Versions live in the tree under `v` + escaped key + 8-byte version. The key
is escaped the same prefix-free, order-preserving way as text values, so all
versions of a key are adjacent, oldest first. The versions of every key in
`[start, end)` then form one contiguous range, which a scan reads in order,
keeping the newest visible version of each key.

Each commit gets the next number from a version counter. A snapshot is just
a version number: it sees exactly the versions below it, so it includes
every commit made before it was taken and none made after.

### Optimistic transactions: first committer wins

A transaction (`mvcc.Tx`) reads from its snapshot and **buffers its writes
in memory**. Its reads, scans included, see the snapshot overlaid with its
own writes. Nothing reaches storage before `COMMIT`. At `COMMIT`, the
transaction becomes a single, self-contained **command**: its snapshot
version and the list of its writes. Applying the command checks every key it
writes. If any has a version newer than the snapshot, another transaction
committed that key after this one read, so the commit fails with a
serialization error and the client retries:

| Session A | Session B | |
|---|---|---|
| `BEGIN` | `BEGIN` | |
| `UPDATE accounts SET balance = balance - 30 WHERE id = 1` | `UPDATE accounts SET balance = balance - 20 WHERE id = 1` | both run against their snapshots |
| `COMMIT` | | |
| | `COMMIT` | **error**: could not serialize access due to a concurrent update |

The balance ends at 70, so no update is lost. The same check enforces
uniqueness across sessions. Two concurrent inserts of the same primary key,
or of the same value in a unique index, write the same key, so the second
commit fails.

**Why optimistic.** The first version of this milestone was pessimistic, in
the style of PostgreSQL:
- a transaction wrote versions into the tree as it went;
- a write failed the moment it touched a key another open transaction had
  written.

That works on one machine. But a replicated database must make the same
changes on every node in the same order, and that design needs every node to
track every in-flight transaction. As commands, transactions are easy to
replicate (Milestone 6): Raft orders the commands, and applying one is
**deterministic**. The conflict check depends only on committed state and
the command, so every replica makes the same decision. Applying a batch of
buffered writes in one pass also turned out faster: bulk inserts went from
111 µs to 82 µs per row, and point queries from 22 µs to 12 µs. The cost is
that a doomed transaction finds out only at `COMMIT`.

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
behind. Neither case collides on a key, so no write conflict is detected.

raftdb uses a coarse rule, applied with every commit:
- a commit that changes the schema fails if anything was committed since
  its snapshot;
- a commit that writes data fails if the schema changed since its snapshot.

In effect, DDL takes a lock on the whole database. Each transaction reads
the catalog through its own snapshot, so uncommitted schema changes are
invisible to other sessions. The decoded catalog is cached per schema
version, so statements don't decode it every time.

### Durability and recovery

Locally, each commit command is applied and then fsynced. The store's own
bookkeeping is updated in the same storage commit as the data: the version
counter, the last data and schema commits, and the vacuum floor. Since
uncommitted writes never reach storage, there is nothing to undo after a
crash. Read-only transactions write nothing at all. Sequences hand out table
and row IDs. They are non-transactional, like PostgreSQL's, so concurrent
inserts never conflict over them.

### Garbage collection: VACUUM

Old versions accumulate, so `VACUUM` removes the ones no transaction can
ever see again. Its **horizon** is the oldest snapshot still open. For each
key, only the newest version below the horizon matters to anyone. Older ones
are deleted, and so is that newest one if it records a deletion. The horizon
travels inside the vacuum command, so every replica removes the same
versions. The store also remembers the highest horizon so far, and refuses
commits from snapshots older than it ("snapshot too old"). Vacuum doesn't
run automatically yet.

### Testing MVCC

- **Model-based tests** run random interleavings of overlapping
  transactions against a reference model of snapshot isolation that knows
  nothing about versions:
  - `tx_test.go` tests optimistic transactions. Each transaction reads the
    committed state as of its start, overlaid with its own writes, and its
    commit fails exactly when a commit made after it started changed a key
    it writes.
  - `mvcc_test.go` tests the underlying versioned store, which applies each
    command.

  The store must agree with the model on every read, scan, delete and
  conflict. Each seed runs 3,000 operations with a 4-page buffer pool,
  including vacuums and two kinds of injected crash. After a crash, the
  store must hold exactly the committed state. 100 seeds passed for each
  layer; a typical seed makes about 300 commits and 30 conflicts. The first
  model test caught a bug in itself: the model built a commit's new state
  from the transaction's snapshot instead of from the latest committed
  state.
- **Commands.** Applying a command twice at the same log index is a no-op.
  Encoding round-trips, and every truncation of an encoded command is
  rejected.
- **Anomaly tests** (`internal/engine/mvcc_test.go`). With two sessions,
  the tests check:
  - no dirty reads, non-repeatable reads, or phantoms, including through an
    index;
  - lost updates and concurrent duplicate keys fail at `COMMIT`;
  - write skew does happen;
  - the schema-change rule holds in both orders;
  - `VACUUM` never breaks an open snapshot.
- **Concurrent bank transfers.** Four goroutines, each with its own session,
  move money between accounts and retry on conflicts. Meanwhile, two readers
  check that every snapshot sees the same total.
- The engine's differential test also runs on top of MVCC, with `VACUUM`
  every 50 steps.

The Go race detector needs a 64-bit C compiler, which the development
machine lacks. I reviewed the shared state by hand: commands and snapshots
are serialized by the store's apply lock, a snapshot never changes, and
cached catalog maps are copied before a schema change modifies them.

### Known limitations
- Snapshot isolation, not serializable: write skew is possible.
- Conflicts are found only at `COMMIT`, so a doomed transaction runs to the
  end. Clients must retry.
- A transaction's writes are buffered in memory.
- Schema changes conflict with every concurrent write, on any table.
- `VACUUM` is manual and scans the whole database while holding the store's
  lock.
- Locally, commits are serialized, one fsync each; group commit would batch
  them.

## Replication: Raft (Milestone 6)

A cluster keeps a full copy of the database on every node, and stays
available as long as a majority of nodes are up and can talk to each other.
The nodes agree on a single ordered log of commands using **Raft**: every
node applies the same commands in the same order to its own MVCC store, so
every replica ends up byte-for-byte identical.

```
 SQL session ── BEGIN: ReadIndex ──▶ leader ── snapshot of its local store
             ── COMMIT: command ───▶ leader ── append to log ──▶ followers
                                        ◀── a majority has persisted it ──┘
                                     every node applies it to its store
```

### What gets replicated

Raft replicates commands, so transactions must turn into commands. That is
why Milestone 5's transactions became optimistic. A transaction reads a
snapshot of the leader's store and buffers its writes; `COMMIT` proposes one
command holding the snapshot version and the writes. Applying a command is
**deterministic**. The first-committer-wins check, the schema rule and the
version numbers all depend only on the store and the command, so every
replica decides the same way. Other commands reserve sequence ranges for
table and row IDs (100 at a time, so inserts rarely need an extra round) and
run `VACUUM` with the leader's horizon.

Determinism had to hold across crashes too, and a test found a case where it
didn't. A commit whose deletes all missed consumed a version number without
recording it. A replica that crashed and replayed its log would then number
later commits differently from the others. Now the version counter is saved
with every applied command. `TestReplicasConverge` replays a random command
log into a second store that crashes at random points, and requires the two
to end up identical. It fails without the fix.

### The Raft core

`internal/raft` is a deterministic state machine with no I/O, goroutines or
clocks, in the style of etcd's raft library. The caller feeds it ticks,
messages and proposals, and collects a `Ready`: state and entries to persist,
messages to send, and committed entries to apply. It implements:

- **Leader election** with randomized timeouts and the up-to-date-log vote
  restriction.
- **Log replication** with consistency checks and fast backtracking by term.
  Pipelining is limited to a single chain per follower (see below).
- **Commitment** only by counting replicas of an entry from the leader's
  current term. A new leader commits a no-op to finish what earlier terms
  left behind.
- **Pre-vote** and **check-quorum** (thesis sections 9.6 and 6.2). A node
  that can't hear the leader first asks whether it could win, without
  raising its term. A leader that can't hear a majority steps down.
- **Linearizable reads with ReadIndex** (thesis section 6.4). Before a
  transaction takes its snapshot, the leader confirms with a quorum that it
  still leads, and waits until it has applied everything committed before
  the request. A deposed leader that hasn't noticed yet can't serve stale
  data. Even without ReadIndex, its writes would be safe, because the
  commit check runs against the real log. But its read-only queries would
  return stale results.

`internal/cluster` drives the core: a goroutine per node, the Raft log in a
B+ tree of its own (entries larger than a page are split into chunks), and
the node's MVCC store. A node persists entries and its term and vote, and
fsyncs, before sending any message that depends on them (a leader's appends
are the one exception; see Milestone 7). Applied commands
are flushed every 256 entries; after a crash, the node replays its log from
the last flush.

Nodes talk over **gRPC**: a single method carries batches of Raft messages.
There is no `.proto` file and no generated code. The service descriptor is
written by hand and messages use a small binary codec, so the build needs no
code generator. A sender goroutine per peer batches whatever has queued up.

### Testing Raft

**A deterministic simulator** (`raft_test.go`) runs 1-, 3- and 5-node
clusters through 30,000 random steps of faults, then heals the network and
requires the cluster to recover:
- **Network faults:** messages are lost, duplicated and delivered in any
  order. Nodes get partitioned, and some links fail in one direction only.
- **Clock drift:** clocks run at different rates per node.
- **Crashes:** a crashed node loses everything it hadn't persisted,
  including work it had computed but not yet handled.

After every step, the simulator checks Raft's safety properties:
- **election safety:** at most one leader per term;
- **log matching:** logs that agree at an index agree before it;
- **leader completeness:** a new leader holds every committed entry;
- **state machine safety:** no two nodes apply different entries at the
  same index;
- **linearizable reads:** a read's index covers everything committed when
  the read began;
- **monotonic hard state:** term and vote never go backwards.

The run must also do real work. It fails if fewer than 50 entries commit, or
fewer than 10 leaders are elected, while faults are on. A typical 3-node
run elects about 25 leaders, survives about 45 crashes and commits 400–800
entries under faults. 100 seeds per cluster size passed.

**The simulator found real problems before any test failed outright.** Its
first version reported success because its faults were miscalibrated: nodes
were down so often that the cluster rarely had a quorum, so it was testing
almost nothing. Once the guard against quiet runs was in place, two genuine
liveness bugs showed up:
- **Disruptive elections.** A node with a one-way network failure kept
  starting elections and deposing healthy leaders. Pre-vote and
  check-quorum fixed it.
- **Replication flood.** Every heartbeat to a lagging follower started a new
  append-acknowledge chain, and old chains never ended. The flood delayed
  acknowledgements until the leader stepped down. Meanwhile, each new term
  moved the commit target further away, so in one run 900 entries were
  appended and only 4 committed. Now only the furthest append continues a
  chain.

**Checking the checker.** Six classic Raft bugs were planted, one at a time.
Each must make the suite fail:

| Planted bug | Caught by |
|---|---|
| votes ignore how up to date the candidate's log is | leader completeness |
| a node votes twice in one term | election safety / monotonic hard state |
| a leader commits an earlier term's entry by counting replicas | `TestFigure8`, a scripted replay of Figure 8 in the Raft paper |
| a follower commits past the entries it just verified | state machine safety |
| ReadIndex skips the quorum round | `TestStaleLeaderCannotConfirmReads`, a leader frozen as if by a long GC pause |
| reads served before the new leader commits in its own term | linearizable reads |

Two of these were missed at first, and each gap was fixed in the tests. The
random simulator rarely produces Figure 8's precise sequence of crashes, so
that case got a scripted test. Stale-leader reads need a leader whose clock
stops, which a random walk rarely produces, so that case got one too.
Running without check-quorum goes undetected, as it should, because
check-quorum only affects liveness.

**End to end** (`internal/cluster`). SQL runs on clusters over an in-memory
network with fault injection:
- **Basic replication:** after a mix of DDL, DML and transactions, every
  replica is byte-for-byte identical. A follower refuses reads and names
  the leader.
- **Failover:** the leader crashes and the cluster elects a new one. No
  committed row is lost. The old leader restarts from its log and catches
  up.
- **Partitioned leader:** an isolated leader can neither commit nor serve
  reads. The majority carries on, and the old leader's uncommitted write is
  discarded.
- **Bank transfers under chaos:** four clients transfer money between
  accounts while nodes crash and restart, links are cut and 20% of messages
  are lost. Every replica must end with the same total and identical
  contents. Typical runs commit about 300 transfers through 4 crashes.
- **gRPC:** a three-node cluster runs over real gRPC connections on
  localhost.

### Known limitations
- No log compaction or snapshots yet. The Raft log grows forever, and a new
  or wiped node must replay it from the start.
- Fixed membership: no adding or removing nodes.
- Only the leader serves transactions. Followers redirect clients to it
  (Milestone 7), so reads don't scale with cluster size.
- A timed-out commit is ambiguous: it may or may not have been applied.
- The gRPC transport is unauthenticated plaintext.

## Server, client and benchmarks (Milestone 7)

### Serving SQL

Each node serves two gRPC services on one port: `raftdb.Raft` for traffic
between nodes and `raftdb.SQL` for clients. As in Milestone 6, there is no
`.proto` file. Messages encode themselves (`internal/wire`), and both
services share one codec, since gRPC allows one per server.

A SQL session is one bidirectional gRPC stream. The client sends
statements, the server answers each with results, and an open transaction
lives as long as the stream. The server runs the statements on the node's
database. A response says what happened in terms a client can act on:

| Code | Meaning | Safe to retry? |
|---|---|---|
| not leader | the node isn't the leader; the response names it | yes, nothing ran |
| leader changed | a commit was lost to a leadership change | yes, it did not happen |
| conflict | serialization failure; rolled back | the application decides |
| unavailable | no quorum answered in time | **no**: a commit may have happened |

### The client

`internal/client` knows every node's address and remembers which one leads.
It retries only when the server guarantees that nothing ran: no statement in
the request completed, and no transaction was open, since an open
transaction lived on the old leader and is gone. A request that may have
committed is never sent twice. If the connection breaks mid-request, the
client reports `ErrConnectionLost`, meaning the outcome is unknown, rather
than guessing. In practice:
- a client given a follower's address is redirected once and remembers the
  leader;
- when the leader dies, an in-flight write fails with an unknown outcome,
  and the next one finds the new leader by itself;
- a transaction open on the dead leader fails at its next statement.

The tests (`internal/server`) cover:
- leader routing from a follower's address;
- every value type over the wire;
- sessions and transactions that span requests;
- conflicts between sessions;
- rollback of a transaction whose session is dropped;
- failover. The test kills the leader mid-workload and counts rows to check
  that only ambiguous writes are uncertain, then checks the old leader
  catches up after a restart.

### Making replicated commits fast

The first benchmark showed a three-node cluster doing 146 inserts/s with
one client and 783/s with 16. Group commit was already in place: the node
loop drains every queued proposal before a Raft round, so concurrent
transactions share one append, one fsync and one round of messages. Yet
latency doubled from one client to four. Two changes fixed most of it.

**Leader and follower fsyncs in parallel** (Raft thesis section 10.2.1). The
leader used to fsync an entry and only then send it, so each commit paid for
two fsyncs in sequence. Now the leader sends appends first and writes to its
own disk while the followers write to theirs. That is only safe if the
leader stops counting its own copy towards a majority until it is actually
on disk. So the core now waits for an explicit `Persisted(index, term)` from
the caller. A crash between sending and persisting then can't produce a
commit that a majority doesn't hold.

The simulator was extended to model this:
- leader writes are asynchronous and pipelined, with several Readys in
  flight while the node keeps receiving messages and sending new ones;
- nodes may crash with writes in flight.

The planted-bug check confirmed the extension was needed. A leader that
counts its own unpersisted entries passed the old simulator, because one
Ready at a time can't expose the bug. The pipelined simulator catches it:
"leader 2 of term 7 lacks committed entry 93". One defensive check stays
untested: `Persisted` ignores a write whose entries were replaced in the
meantime, which needs a node to lose and regain leadership with writes in
flight.

**Half the fsyncs were unnecessary.** Timing the node loop showed about two
log saves per entry, and the leader spent 78% of its time in fsync. The
second save happened because the commit index, which is part of Raft's hard
state, had changed. It doesn't need to be durable: after a restart,
committed entries are learned again and replayed. Now the commit index rides
along with the next write of entries, term or vote.

| Inserts/s (p50 latency) | 1 client | 16 clients |
|---|---|---|
| Milestone 6 node, with group commit | 146 (6.5 ms) | 783 (19.5 ms) |
| + parallel fsyncs | 206 (4.7 ms) | 1,002 (15.0 ms) |
| + no fsync for commit-index changes | **360 (2.6 ms)** | **1,316 (11.5 ms)** |

A flaky test also turned up a shutdown race. A statement running while its
node shut down read from closed storage, and the raw storage error reached
the client. It is now reported as "unavailable". The chaos test passed 15
runs in a row with the machine saturated by a benchmark.

### Benchmarks

`cmd/raftbench` starts a cluster of real nodes on localhost (gRPC, separate
data files, one shared SSD), runs concurrent client sessions through the
client library, and reports throughput and latency percentiles. Three
nodes, 10 ms Raft ticks, on an i5-1235U laptop running Windows 11:

| Workload | Clients | Throughput | p50 | p99 |
|---|---|---|---|---|
| point `SELECT` by primary key | 16 | 31,000 ops/s | 0.5 ms | 2.0 ms |
| 90% point reads, 10% inserts | 16 | 3,700 ops/s | 3.9 ms | 12 ms |
| `INSERT`, autocommit | 1 | 360 ops/s | 2.6 ms | 6.0 ms |
| `INSERT`, autocommit | 16 | 1,300 ops/s | 11.5 ms | 24 ms |
| `INSERT`, autocommit | 64 | 3,000 ops/s | 20 ms | 44 ms |
| transfer: `BEGIN`, two `UPDATE`s, `COMMIT` | 16 | 1,000 txn/s | 14 ms | 33 ms |

Throughput grows with concurrency because group commit puts more
transactions behind each fsync. In the transfer workload, about 2% of
transactions hit a serialization conflict and were rolled back. The total
balance was exact at the end.

**Failover.** With `-failover`, raftbench kills the leader halfway through
an insert workload with 16 clients. It reports two numbers: how long the
cluster takes to elect a new leader, and when the first write begun after
the kill succeeds. Writes in flight at the kill fail with an unknown
outcome, one per client.

| Raft tick (election timeout 10–20 ticks) | New leader after | First new write succeeds after |
|---|---|---|
| 10 ms | 118–169 ms | about 160 ms (one run of five: 324 ms) |
| 5 ms | 78–86 ms | 88–158 ms |

Shorter ticks speed up failover at the cost of more heartbeat traffic, and
a higher risk of needless elections on a slow network.

### Known limitations
- Each transaction needs a ReadIndex round for its snapshot before it can
  commit. Lease reads would save it, but they rely on bounded clock drift,
  which the simulator showed is not safe to assume.
- The data store flushes on the node loop every 256 entries, which shows up
  in tail latency.
- No TLS or authentication.
- The client doesn't retry conflicts; applications must.

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

| Operation | Milestone 4 | Pessimistic MVCC | Optimistic MVCC | Notes |
|---|---|---|---|---|
| `INSERT`, one transaction each | ~1.3–1.9 ms | ~1.3–1.6 ms | ~1.4 ms | fsync-bound |
| `INSERT`, 1,000 per transaction | ~86 µs | ~111 µs | ~82 µs | writes are buffered and applied in one pass at commit |
| point `SELECT` by primary key | ~26 µs | ~22 µs | ~12 µs | read-only snapshots write nothing; the catalog is cached |

**Replicated** (`go test ./internal/cluster -run=XXX -bench=.`). Three nodes
in one process, on the same disk:

| Operation | Time per op | Notes |
|---|---|---|
| `INSERT`, one transaction each | ~5.5 ms → ~2.5 ms | a ReadIndex round, then a log fsync on the leader and a follower; Milestone 7 overlapped the two and dropped a redundant one |

Throughput and failover over a real gRPC cluster are in the Milestone 7
section.

Most of what remains is the storage engine decoding a whole node on every
visit. A cache of decoded nodes is the obvious next optimisation.

## License

MIT. See [LICENSE](LICENSE).
