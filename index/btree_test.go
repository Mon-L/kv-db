package index

import (
	"github.com/stretchr/testify/assert"
	"kv-db/wal"
	"strconv"
	"testing"
)

func TestMemoryBTree_Put(t *testing.T) {
	mBTree := newMemoryBTree()

	key := []byte("abc")
	chunk := &wal.Chunk{
		SegmentId:   1,
		BlockIndex:  2,
		BlockOffset: 3,
		Size:        10,
	}
	assert.Nil(t, mBTree.Put(key, chunk))
	assert.Equal(t, 1, mBTree.Size())

	oldChunk := mBTree.Put(key, &wal.Chunk{
		SegmentId:   2,
		BlockIndex:  3,
		BlockOffset: 4,
		Size:        10,
	})
	assert.Equal(t, chunk, oldChunk)
}

func TestMemoryBTree_Get(t *testing.T) {
	mBTree := newMemoryBTree()

	key := []byte("abc")
	chunk := &wal.Chunk{
		SegmentId:   1,
		BlockIndex:  2,
		BlockOffset: 3,
		Size:        10,
	}
	assert.Nil(t, mBTree.Put(key, chunk))

	chunk2 := mBTree.Get(key)
	assert.Equal(t, chunk, chunk2)
}

func TestMemoryBTree_Delete(t *testing.T) {
	mBTree := newMemoryBTree()

	// put
	key := []byte("abc")
	chunk := &wal.Chunk{
		SegmentId:   1,
		BlockIndex:  2,
		BlockOffset: 3,
		Size:        10,
	}
	assert.Nil(t, mBTree.Put(key, chunk))

	// delete
	chunk2, ret := mBTree.Delete(key)
	assert.True(t, ret)
	assert.Equal(t, chunk, chunk2)
	assert.Equal(t, 0, mBTree.Size())

	// delete again, but fail
	chunk2, ret = mBTree.Delete(key)
	assert.False(t, ret)
	assert.Nil(t, chunk2)
	assert.Equal(t, 0, mBTree.Size())
}

func TestMemoryBTree_Size(t *testing.T) {
	mBTree := newMemoryBTree()

	var keys [][]byte
	for i := 1; i <= 100; i++ {
		key := []byte(strconv.Itoa(i))
		chunk := &wal.Chunk{
			SegmentId:   1,
			BlockIndex:  2,
			BlockOffset: 3,
			Size:        10,
		}
		keys = append(keys, key)
		mBTree.Put(key, chunk)
		assert.Equal(t, i, mBTree.Size())
	}

	for i := 0; i < 100; i++ {
		mBTree.Delete(keys[i])
		assert.Equal(t, 100-i-1, mBTree.Size())
	}
}
