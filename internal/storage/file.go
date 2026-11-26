package storage

import "os"

// file is the subset of *os.File the storage engine uses. Tests substitute an
// in-memory implementation that can simulate a power failure.
type file interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Truncate(size int64) error
	Sync() error
	Size() (int64, error)
	Close() error
}

type fileSystem interface {
	// OpenFile opens name for reading and writing, creating it if needed.
	OpenFile(name string) (file, error)
}

type osFS struct{}

func (osFS) OpenFile(name string) (file, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
