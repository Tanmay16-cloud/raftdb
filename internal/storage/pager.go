package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// PageSize is the size of every page in the database file.
const PageSize = 4096

const (
	metaMagic   uint32 = 0x31424452 // "RDB1" in little-endian
	metaVersion uint32 = 1
)

// ErrCorrupt is returned when the database file does not have the expected layout.
var ErrCorrupt = errors.New("storage: database file is corrupt")

// Options tunes the storage engine. The zero value gives sensible defaults.
type Options struct {
	// PoolPages is how many pages the buffer pool keeps in memory.
	// Default 1024 (4 MB).
	PoolPages int
	// CheckpointFrames is how many frames (one page each) the write-ahead
	// log may hold before a commit triggers a checkpoint. Default 1000.
	CheckpointFrames int

	fs fileSystem // tests swap in a file system that simulates crashes
}

func (o Options) withDefaults() Options {
	if o.PoolPages <= 0 {
		o.PoolPages = 1024
	}
	if o.CheckpointFrames <= 0 {
		o.CheckpointFrames = 1000
	}
	if o.fs == nil {
		o.fs = osFS{}
	}
	return o
}

// Stats counts storage activity since the database was opened.
type Stats struct {
	CacheHits   int64
	CacheMisses int64
	Evictions   int64
	Commits     int64
	Checkpoints int64
}

// Pager manages the database's pages. It caches them in a bounded buffer
// pool and makes changes durable through a write-ahead log (WAL).
//
// Two files: <path> holds the pages, and <path>-wal holds the log. Page 0
// of the data file is the meta page:
//
//	magic u32 | version u32 | root page u32 | page count u32
//
// How a change reaches disk:
//
//  1. Write puts the new page in the buffer pool, marked dirty.
//  2. Commit appends every dirty page to the log, marks the last frame as
//     the commit record, and fsyncs the log. Once that fsync returns, the
//     change is durable.
//  3. When the log grows past CheckpointFrames, a checkpoint copies the
//     newest version of every logged page into the data file, fsyncs it,
//     and empties the log.
//
// The data file only ever receives committed pages, and only during a
// checkpoint. If the pool has to evict a dirty page before it is committed,
// the page goes to the log as an ordinary frame, and later reads find it
// there. Recovery ignores such a frame unless a commit record follows it, so
// it only ever needs to redo committed work, never undo uncommitted work.
//
// On open, recovery copies every committed frame into the data file (that
// is, it runs a checkpoint) and discards anything after the last commit.
type Pager struct {
	db        file
	wal       *wal
	pool      *bufferPool
	opts      Options
	numPages  uint32
	root      uint32
	metaDirty bool
	err       error // set by fail; once set, every operation returns it
	stats     Stats
}

// OpenPager opens the database at path, creating it if it does not exist,
// and recovers any committed changes left in the write-ahead log.
func OpenPager(path string, opts Options) (*Pager, error) {
	opts = opts.withDefaults()
	db, err := opts.fs.OpenFile(path)
	if err != nil {
		return nil, err
	}
	walFile, err := opts.fs.OpenFile(path + "-wal")
	if err != nil {
		db.Close()
		return nil, err
	}
	p := &Pager{db: db, pool: newBufferPool(opts.PoolPages), opts: opts}
	if err := p.recover(walFile); err != nil {
		db.Close()
		walFile.Close()
		return nil, err
	}
	return p, nil
}

func (p *Pager) recover(walFile file) error {
	w, last, err := openWAL(walFile)
	if err != nil {
		return err
	}
	p.wal = w
	if last != nil {
		// The log is the authority on the latest committed state; the data
		// file may be stale or half-written by an interrupted checkpoint.
		p.root, p.numPages = last.root, last.numPages
		return p.checkpoint()
	}
	if err := w.reset(); err != nil {
		return err
	}
	size, err := p.db.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		p.numPages = 1 // page 0 is reserved for the meta page
		p.metaDirty = true
		return nil
	}
	return p.readMeta(size)
}

func (p *Pager) readMeta(fileSize int64) error {
	if fileSize < PageSize {
		return fmt.Errorf("%w: file smaller than one page", ErrCorrupt)
	}
	buf := make([]byte, PageSize)
	if _, err := p.db.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("read meta page: %w", err)
	}
	if binary.LittleEndian.Uint32(buf[0:]) != metaMagic {
		return fmt.Errorf("%w: bad magic number", ErrCorrupt)
	}
	if v := binary.LittleEndian.Uint32(buf[4:]); v != metaVersion {
		return fmt.Errorf("storage: unsupported file version %d", v)
	}
	p.root = binary.LittleEndian.Uint32(buf[8:])
	p.numPages = binary.LittleEndian.Uint32(buf[12:])
	if p.numPages == 0 || int64(p.numPages)*PageSize > fileSize {
		return fmt.Errorf("%w: meta page claims %d pages but file is %d bytes", ErrCorrupt, p.numPages, fileSize)
	}
	if p.root >= p.numPages {
		return fmt.Errorf("%w: root page %d out of range", ErrCorrupt, p.root)
	}
	return nil
}

func encodeMeta(root, numPages uint32) []byte {
	buf := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(buf[0:], metaMagic)
	binary.LittleEndian.PutUint32(buf[4:], metaVersion)
	binary.LittleEndian.PutUint32(buf[8:], root)
	binary.LittleEndian.PutUint32(buf[12:], numPages)
	return buf
}

// fail records an I/O error after which memory and disk may disagree. From
// then on every operation returns the error, and the database must be
// reopened, which runs recovery from the log.
//
// Retrying instead is unsafe: after a failed fsync the kernel may already
// have dropped the unwritten pages, so a second fsync can "succeed" without
// saving them. PostgreSQL hit exactly this bug in 2018 ("fsyncgate").
func (p *Pager) fail(err error) error {
	if p.err == nil {
		p.err = fmt.Errorf("storage: I/O failure, reopen the database to recover: %w", err)
	}
	return p.err
}

// Read returns the contents of page id. Callers must not modify the returned
// slice; to change a page, build a new buffer and pass it to Write.
func (p *Pager) Read(id uint32) ([]byte, error) {
	if p.err != nil {
		return nil, p.err
	}
	if id == 0 || id >= p.numPages {
		return nil, fmt.Errorf("%w: page %d out of range", ErrCorrupt, id)
	}
	if pg, ok := p.pool.get(id); ok {
		p.stats.CacheHits++
		return pg.data, nil
	}
	p.stats.CacheMisses++

	buf := make([]byte, PageSize)
	if off, ok := p.wal.index[id]; ok {
		// The newest version is in the log: either committed but not yet
		// checkpointed, or evicted from the pool before being committed.
		if err := p.wal.readPage(off, buf); err != nil {
			return nil, err
		}
	} else if _, err := p.db.ReadAt(buf, int64(id)*PageSize); err != nil {
		return nil, fmt.Errorf("read page %d: %w", id, err)
	}
	if err := p.cache(id, buf, false); err != nil {
		return nil, err
	}
	return buf, nil
}

// Write replaces the contents of page id. The change is durable after the
// next Commit.
func (p *Pager) Write(id uint32, buf []byte) error {
	if p.err != nil {
		return p.err
	}
	if id == 0 || id >= p.numPages || len(buf) != PageSize {
		panic(fmt.Sprintf("storage: invalid write to page %d (len %d)", id, len(buf)))
	}
	return p.cache(id, buf, true)
}

// cache adds a page to the buffer pool, sending any dirty page it evicts to
// the log.
func (p *Pager) cache(id uint32, buf []byte, dirty bool) error {
	victim := p.pool.put(id, buf, dirty)
	if victim == nil {
		return nil
	}
	p.stats.Evictions++
	if !victim.dirty {
		return nil // the disk already has this version
	}
	if err := p.wal.append(victim.id, victim.data, nil); err != nil {
		return p.fail(err)
	}
	return nil
}

// Allocate reserves a new page at the end of the file and returns its ID.
// The caller must Write the page before the next Commit.
func (p *Pager) Allocate() uint32 {
	id := p.numPages
	p.numPages++
	p.metaDirty = true
	return id
}

// Root returns the page ID of the B+ tree root, or 0 if there is no tree yet.
func (p *Pager) Root() uint32 { return p.root }

// SetRoot records a new root page; it is persisted by the next Commit.
func (p *Pager) SetRoot(id uint32) {
	p.root = id
	p.metaDirty = true
}

// Commit makes every change since the previous Commit durable, atomically:
// after a crash, either all of them survive or none do.
func (p *Pager) Commit() error {
	if p.err != nil {
		return p.err
	}
	dirty := p.pool.dirtyPages()
	if len(dirty) == 0 && !p.metaDirty && !p.wal.uncommitted {
		return nil
	}
	info := &commitInfo{root: p.root, numPages: p.numPages}
	for i, pg := range dirty {
		var c *commitInfo
		if i == len(dirty)-1 {
			c = info
		}
		if err := p.wal.append(pg.id, pg.data, c); err != nil {
			return p.fail(err)
		}
	}
	if len(dirty) == 0 {
		// Every changed page was already evicted to the log; just seal it.
		if err := p.wal.append(0, nil, info); err != nil {
			return p.fail(err)
		}
	}
	if err := p.wal.sync(); err != nil {
		return p.fail(err)
	}
	p.pool.markClean()
	p.metaDirty = false
	p.stats.Commits++

	if p.wal.frames() >= p.opts.CheckpointFrames {
		if err := p.checkpoint(); err != nil {
			return p.fail(err)
		}
	}
	return nil
}

// checkpoint copies the newest version of every logged page into the data
// file, fsyncs it, and empties the log. It must only run when everything in
// the log is committed. If a crash interrupts it, the log is still intact
// and recovery simply runs it again; copying the same pages twice is harmless.
func (p *Pager) checkpoint() error {
	ids := make([]uint32, 0, len(p.wal.index))
	for id := range p.wal.index {
		ids = append(ids, id)
	}
	slices.Sort(ids) // sequential writes are kinder to the disk
	buf := make([]byte, PageSize)
	for _, id := range ids {
		if err := p.wal.readPage(p.wal.index[id], buf); err != nil {
			return err
		}
		if _, err := p.db.WriteAt(buf, int64(id)*PageSize); err != nil {
			return fmt.Errorf("checkpoint page %d: %w", id, err)
		}
	}
	if _, err := p.db.WriteAt(encodeMeta(p.root, p.numPages), 0); err != nil {
		return fmt.Errorf("checkpoint meta page: %w", err)
	}
	// The data file must be durable before the log that backs it is erased.
	if err := p.db.Sync(); err != nil {
		return err
	}
	if err := p.wal.reset(); err != nil {
		return err
	}
	p.metaDirty = false
	p.stats.Checkpoints++
	return nil
}

// Stats returns activity counters since the pager was opened.
func (p *Pager) Stats() Stats { return p.stats }

// Close commits outstanding changes, checkpoints, and closes both files.
func (p *Pager) Close() error {
	err := p.err
	if err == nil {
		err = p.Commit()
	}
	if err == nil && p.wal.frames() > 0 {
		err = p.checkpoint()
	}
	for _, f := range []file{p.db, p.wal.f} {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
