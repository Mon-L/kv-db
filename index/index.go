package index

import "kv-db/wal"

type Indexer interface {
	Put(key []byte, chunk *wal.Chunk) *wal.Chunk

	Get(key []byte) *wal.Chunk

	Delete(key []byte) (*wal.Chunk, bool)

	Size() int
}

func NewIndexer() Indexer {
	return newMemoryBTree()
}
