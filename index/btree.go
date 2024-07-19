package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-db/wal"
	"sync"
)

type memoryBTree struct {
	btree *btree.BTree
	mu    sync.RWMutex
}

type keyChunkPair struct {
	key   []byte
	chunk *wal.Chunk
}

func newMemoryBTree() *memoryBTree {
	return &memoryBTree{
		btree: btree.New(32),
	}
}

func (it keyChunkPair) Less(than btree.Item) bool {
	if than == nil {
		return false
	}

	return bytes.Compare(it.key, than.(*keyChunkPair).key) < 0
}

func (mBTree *memoryBTree) Put(key []byte, chunk *wal.Chunk) *wal.Chunk {
	mBTree.mu.Lock()
	defer mBTree.mu.Unlock()

	if old := mBTree.btree.ReplaceOrInsert(&keyChunkPair{key, chunk}); old != nil {
		return old.(*keyChunkPair).chunk
	}
	return nil
}

func (mBTree *memoryBTree) Get(key []byte) *wal.Chunk {
	mBTree.mu.RLock()
	defer mBTree.mu.RUnlock()

	if v := mBTree.btree.Get(&keyChunkPair{key: key}); v != nil {
		return v.(*keyChunkPair).chunk
	}
	return nil
}

func (mBTree *memoryBTree) Delete(key []byte) (*wal.Chunk, bool) {
	mBTree.mu.Lock()
	defer mBTree.mu.Unlock()

	if v := mBTree.btree.Delete(&keyChunkPair{key: key}); v != nil {
		return v.(*keyChunkPair).chunk, true
	}
	return nil, false
}

func (mBTree *memoryBTree) Size() int {
	mBTree.mu.RLock()
	defer mBTree.mu.RUnlock()
	return mBTree.btree.Len()
}
