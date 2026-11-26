package storage

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
)

// errCrash is returned by every memFS operation once the simulated crash
// point has been reached.
var errCrash = errors.New("injected crash")

// memFS is an in-memory fileSystem that models what survives a power failure.
// Each file tracks which writes a Sync has made durable. A crash keeps the
// durable contents plus a random subset of the later writes, some of them
// torn partway through, which is roughly what a real disk can leave behind.
type memFS struct {
	files   map[string]*memFile
	opsLeft int // mutating operations allowed before the crash; negative = no crash
	crashed bool
}

func newMemFS() *memFS {
	return &memFS{files: make(map[string]*memFile), opsLeft: -1}
}

// crashAfter makes the file system fail every operation after n more writes,
// truncates or syncs.
func (fs *memFS) crashAfter(n int) { fs.opsLeft = n }

func (fs *memFS) tick() error {
	if fs.crashed {
		return errCrash
	}
	if fs.opsLeft == 0 {
		fs.crashed = true
		return errCrash
	}
	if fs.opsLeft > 0 {
		fs.opsLeft--
	}
	return nil
}

func (fs *memFS) OpenFile(name string) (file, error) {
	if fs.crashed {
		return nil, errCrash
	}
	f, ok := fs.files[name]
	if !ok {
		f = &memFile{fs: fs}
		fs.files[name] = f
	}
	return f, nil
}

// crash simulates losing power and returns the file system as it would look
// after a reboot. The old file system stops working.
func (fs *memFS) crash(rng *rand.Rand) *memFS {
	after := newMemFS()
	for name, f := range fs.files {
		data := bytes.Clone(f.durable)
		for _, w := range f.pending {
			switch rng.Intn(3) {
			case 0: // never reached the disk
			case 1:
				data = w.apply(data)
			case 2: // torn: only the first few 512-byte sectors made it
				if !w.truncate {
					w.data = w.data[:min(len(w.data), rng.Intn(len(w.data)/512+1)*512)]
				}
				data = w.apply(data)
			}
		}
		after.files[name] = &memFile{fs: after, data: data, durable: bytes.Clone(data)}
	}
	fs.crashed = true
	return after
}

type memWrite struct {
	off      int64
	data     []byte
	truncate bool // a truncation to off bytes rather than a write
}

func (w memWrite) apply(data []byte) []byte {
	end := w.off + int64(len(w.data))
	if w.truncate {
		end = w.off
	}
	if end > int64(len(data)) {
		data = append(data, make([]byte, end-int64(len(data)))...)
	}
	if w.truncate {
		return data[:w.off]
	}
	copy(data[w.off:], w.data)
	return data
}

type memFile struct {
	fs      *memFS
	data    []byte     // contents as reads see them
	durable []byte     // contents guaranteed to survive a crash
	pending []memWrite // changes since the last Sync, oldest first
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if f.fs.crashed {
		return 0, errCrash
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.fs.tick(); err != nil {
		return 0, err
	}
	w := memWrite{off: off, data: bytes.Clone(p)}
	f.data = w.apply(f.data)
	f.pending = append(f.pending, w)
	return len(p), nil
}

func (f *memFile) Truncate(size int64) error {
	if err := f.fs.tick(); err != nil {
		return err
	}
	w := memWrite{off: size, truncate: true}
	f.data = w.apply(f.data)
	f.pending = append(f.pending, w)
	return nil
}

func (f *memFile) Sync() error {
	if err := f.fs.tick(); err != nil {
		return err
	}
	f.durable = bytes.Clone(f.data)
	f.pending = nil
	return nil
}

func (f *memFile) Size() (int64, error) {
	if f.fs.crashed {
		return 0, errCrash
	}
	return int64(len(f.data)), nil
}

func (f *memFile) Close() error { return nil }
