package storage

import (
	"cmp"
	"container/list"
	"slices"
)

// bufferPool is a fixed-capacity page cache with least-recently-used (LRU)
// eviction. It only keeps track of pages; the pager decides what to do with a
// dirty page when it is evicted.
//
// Pages are never pinned: the B+ tree decodes a page into its own memory as
// soon as it reads it, and replaces whole page buffers instead of modifying
// them, so evicting a page can never pull data out from under a caller.
type bufferPool struct {
	capacity int
	lru      *list.List // front = most recently used; values are *poolPage
	pages    map[uint32]*list.Element
	dirty    map[uint32]*poolPage
}

type poolPage struct {
	id    uint32
	data  []byte
	dirty bool // changed since the last commit
}

func newBufferPool(capacity int) *bufferPool {
	return &bufferPool{
		capacity: capacity,
		lru:      list.New(),
		pages:    make(map[uint32]*list.Element),
		dirty:    make(map[uint32]*poolPage),
	}
}

// get returns the cached page and marks it most recently used.
func (b *bufferPool) get(id uint32) (*poolPage, bool) {
	e, ok := b.pages[id]
	if !ok {
		return nil, false
	}
	b.lru.MoveToFront(e)
	return e.Value.(*poolPage), true
}

// put caches data as the contents of page id. If that pushes the pool over
// capacity, it removes the least recently used page and returns it.
func (b *bufferPool) put(id uint32, data []byte, dirty bool) (victim *poolPage) {
	if e, ok := b.pages[id]; ok {
		pg := e.Value.(*poolPage)
		pg.data = data
		if dirty {
			pg.dirty = true
			b.dirty[id] = pg
		}
		b.lru.MoveToFront(e)
		return nil
	}

	pg := &poolPage{id: id, data: data, dirty: dirty}
	b.pages[id] = b.lru.PushFront(pg)
	if dirty {
		b.dirty[id] = pg
	}
	if b.lru.Len() <= b.capacity {
		return nil
	}
	e := b.lru.Back()
	victim = e.Value.(*poolPage)
	b.lru.Remove(e)
	delete(b.pages, victim.id)
	delete(b.dirty, victim.id)
	return victim
}

// dirtyPages returns the pages changed since the last commit, in page order.
func (b *bufferPool) dirtyPages() []*poolPage {
	pages := make([]*poolPage, 0, len(b.dirty))
	for _, pg := range b.dirty {
		pages = append(pages, pg)
	}
	slices.SortFunc(pages, func(x, y *poolPage) int { return cmp.Compare(x.id, y.id) })
	return pages
}

// markClean records that every dirty page has been committed.
func (b *bufferPool) markClean() {
	for _, pg := range b.dirty {
		pg.dirty = false
	}
	clear(b.dirty)
}
