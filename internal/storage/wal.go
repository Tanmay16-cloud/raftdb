package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
)

const (
	walMagic        uint32 = 0x4C415752 // "RWAL" in little-endian
	walVersion      uint32 = 2
	walHeaderSize          = 16
	frameHeaderSize        = 24
	frameSize              = frameHeaderSize + PageSize
	flagCommit      uint32 = 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// wal is the write-ahead log: an append-only file of full page images.
//
// Header (16 bytes):
//
//	magic u32 | version u32 | salt u32 | crc32 of the first 12 bytes u32
//
// Each frame records the new contents of one page:
//
//	page ID u32 | flags u32 | root u32 | page count u32 | salt u32 | crc32 u32 | page (4096 bytes)
//
// A frame with the commit flag ends a transaction; its root and page count
// record the tree's metadata as of that commit. A commit with no page left
// to write uses page ID 0 and an empty page.
//
// The checksum covers the frame's other header fields, its page, and the
// previous frame's checksum (the first frame chains from the header's). So a
// frame torn by a crash fails validation and the log is treated as ending
// just before it. The salt changes every time the log is reset, so a frame
// left over from before a reset (say, because a crash undid the truncation)
// is ignored too.
//
// Chaining matters because of rollback. A rollback rewinds the end of the
// log to the last commit, and the next transaction overwrites the discarded
// frames. If a crash then keeps the new commit frame but loses a new frame
// before it, the discarded frame underneath is still there with the right
// salt and a valid checksum of its own. Without the chain, recovery would
// treat it as part of the new transaction. With it, the discarded frame's
// checksum was computed from a different predecessor, so it fails.
type wal struct {
	f           file
	salt        uint32
	end         int64            // offset of the next frame
	chain       uint32           // checksum of the last frame (or the header)
	index       map[uint32]int64 // page ID -> offset of its newest committed frame
	pending     map[uint32]int64 // the same, for frames since the last commit
	uncommitted bool             // frames appended since the last commit frame

	commitEnd   int64  // end of the last commit frame
	commitChain uint32 // checksum of the last commit frame
}

type commitInfo struct {
	root, numPages uint32
}

// openWAL reads the log and returns the state recorded by its last complete
// transaction, or nil if there is none. On return, w.index covers only
// committed frames: anything after the last commit frame belongs to a
// transaction that a crash cut short, and is ignored.
func openWAL(f file) (*wal, *commitInfo, error) {
	w := &wal{f: f, index: make(map[uint32]int64), pending: make(map[uint32]int64)}
	size, err := f.Size()
	if err != nil {
		return nil, nil, err
	}
	hdr := make([]byte, walHeaderSize)
	if size >= walHeaderSize {
		if _, err := f.ReadAt(hdr, 0); err != nil {
			return nil, nil, fmt.Errorf("read WAL header: %w", err)
		}
	}
	if size < walHeaderSize ||
		binary.LittleEndian.Uint32(hdr[0:]) != walMagic ||
		binary.LittleEndian.Uint32(hdr[12:]) != crc32.Checksum(hdr[:12], castagnoli) {
		// No valid header: the log is new, or a crash interrupted a reset.
		// Resets only happen after a checkpoint has made the data file
		// complete, so there is nothing to recover either way.
		w.salt = rand.Uint32()
		return w, nil, nil
	}
	if v := binary.LittleEndian.Uint32(hdr[4:]); v != walVersion {
		if size == walHeaderSize {
			// An empty log from an older version: nothing to recover.
			w.salt = rand.Uint32()
			return w, nil, nil
		}
		return nil, nil, fmt.Errorf("storage: unsupported WAL version %d", v)
	}
	w.salt = binary.LittleEndian.Uint32(hdr[8:])
	chain := binary.LittleEndian.Uint32(hdr[12:])

	var last *commitInfo
	pending := make(map[uint32]int64) // frames of the transaction being read
	buf := make([]byte, frameSize)
	for off := int64(walHeaderSize); off+frameSize <= size; off += frameSize {
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, nil, fmt.Errorf("read WAL frame: %w", err)
		}
		if !w.valid(buf, chain) {
			break // torn, corrupt or stale: the log ends here
		}
		chain = binary.LittleEndian.Uint32(buf[20:])
		if id := binary.LittleEndian.Uint32(buf[0:]); id != 0 {
			pending[id] = off
		}
		if binary.LittleEndian.Uint32(buf[4:])&flagCommit != 0 {
			for id, o := range pending {
				w.index[id] = o
			}
			clear(pending)
			w.end, w.commitEnd = off+frameSize, off+frameSize
			w.chain, w.commitChain = chain, chain
			last = &commitInfo{
				root:     binary.LittleEndian.Uint32(buf[8:]),
				numPages: binary.LittleEndian.Uint32(buf[12:]),
			}
		}
	}
	return w, last, nil
}

// frameChecksum is the CRC-32C of the previous frame's checksum, this
// frame's header fields, and its page.
func frameChecksum(prev uint32, frame []byte) uint32 {
	c := crc32.Checksum(binary.LittleEndian.AppendUint32(nil, prev), castagnoli)
	c = crc32.Update(c, castagnoli, frame[:20])
	return crc32.Update(c, castagnoli, frame[frameHeaderSize:])
}

func (w *wal) valid(frame []byte, prev uint32) bool {
	return binary.LittleEndian.Uint32(frame[16:]) == w.salt &&
		binary.LittleEndian.Uint32(frame[20:]) == frameChecksum(prev, frame)
}

// append writes a frame for page id. A non-nil commit makes it a commit
// frame. The frame is not durable until sync.
func (w *wal) append(id uint32, page []byte, commit *commitInfo) error {
	buf := make([]byte, frameSize)
	binary.LittleEndian.PutUint32(buf[0:], id)
	if commit != nil {
		binary.LittleEndian.PutUint32(buf[4:], flagCommit)
		binary.LittleEndian.PutUint32(buf[8:], commit.root)
		binary.LittleEndian.PutUint32(buf[12:], commit.numPages)
	}
	binary.LittleEndian.PutUint32(buf[16:], w.salt)
	copy(buf[frameHeaderSize:], page)
	crc := frameChecksum(w.chain, buf)
	binary.LittleEndian.PutUint32(buf[20:], crc)

	if _, err := w.f.WriteAt(buf, w.end); err != nil {
		return fmt.Errorf("append WAL frame: %w", err)
	}
	if id != 0 {
		w.pending[id] = w.end
	}
	w.end += frameSize
	w.chain = crc
	w.uncommitted = commit == nil
	if commit != nil {
		for pid, off := range w.pending {
			w.index[pid] = off
		}
		clear(w.pending)
		w.commitEnd, w.commitChain = w.end, crc
	}
	return nil
}

// lookup returns the offset of the newest frame for page id, if any.
func (w *wal) lookup(id uint32) (int64, bool) {
	if off, ok := w.pending[id]; ok {
		return off, true
	}
	off, ok := w.index[id]
	return off, ok
}

// rollback forgets every frame since the last commit. The next append
// overwrites them; see the type comment for why that is safe.
func (w *wal) rollback() {
	w.end, w.chain = w.commitEnd, w.commitChain
	clear(w.pending)
	w.uncommitted = false
}

// readPage reads the page stored in the frame at off.
func (w *wal) readPage(off int64, buf []byte) error {
	if _, err := w.f.ReadAt(buf, off+frameHeaderSize); err != nil {
		return fmt.Errorf("read WAL page: %w", err)
	}
	return nil
}

func (w *wal) sync() error { return w.f.Sync() }

// frames returns how many frames the log holds.
func (w *wal) frames() int { return int((w.end - walHeaderSize) / frameSize) }

// reset empties the log under a new salt. It must only run once the data
// file holds everything the log does.
func (w *wal) reset() error {
	w.salt++
	if w.salt == 0 { // zeroed bytes must never look like a valid frame
		w.salt = 1
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	hdr := make([]byte, walHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:], walMagic)
	binary.LittleEndian.PutUint32(hdr[4:], walVersion)
	binary.LittleEndian.PutUint32(hdr[8:], w.salt)
	hcrc := crc32.Checksum(hdr[:12], castagnoli)
	binary.LittleEndian.PutUint32(hdr[12:], hcrc)
	if _, err := w.f.WriteAt(hdr, 0); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.end, w.commitEnd = walHeaderSize, walHeaderSize
	w.chain, w.commitChain = hcrc, hcrc
	clear(w.index)
	clear(w.pending)
	w.uncommitted = false
	return nil
}
