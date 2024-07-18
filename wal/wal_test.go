package wal

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"strings"
	"testing"
)

func TestWal_Write(t *testing.T) {
	wal, err := Open(*DefaultOptions)
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data := []byte(strings.Repeat("x", 10))
	chunk, err := wal.Write(data)
	assert.Nil(t, err)
	assert.NotNil(t, chunk)

	data1, err1 := wal.Read(chunk)
	assert.Nil(t, err1)
	assert.NotNil(t, data1)
	assert.Equal(t, data, data1)
}

func TestWal_WriteLarge(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize * 10,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data := []byte(strings.Repeat("x", blockSize*1))
	chunk, err := wal.Write(data)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), chunk.blockIndex)
	assert.Equal(t, uint32(0), chunk.blockOffset)

	chunk2, err2 := wal.Write(data)
	assert.Nil(t, err2)
	assert.Equal(t, uint32(1), chunk2.blockIndex)
	assert.Equal(t, uint32(14), chunk2.blockOffset)
}

func TestWal_WriteTooLarge(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize * 10,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data := []byte(strings.Repeat("x", blockSize*10-chunkHeaderSize))
	chunk, err := wal.Write(data)
	assert.Nil(t, chunk)
	assert.NotNil(t, err)
}

func TestWal_WriteAndSwitchSegment(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize * 5,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data := []byte(strings.Repeat("x", blockSize-chunkHeaderSize))
	chunk, err := wal.Write(data)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), chunk.blockIndex)

	chunk2, err2 := wal.Write(data)
	assert.Nil(t, err2)
	assert.Equal(t, uint32(1), chunk2.blockIndex)
}

func TestWal_Read(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize * 15,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data := []byte("foo")
	loopTime := blockSize

	// write
	positions := make([]*Chunk, loopTime)
	for i := 0; i < loopTime; i++ {
		chunk, err := wal.Write(data)
		assert.Nil(t, err)
		assert.NotNil(t, chunk)
		positions[i] = chunk
	}

	// read
	for i := 0; i < loopTime; i++ {
		val, err := wal.Read(positions[i])
		assert.Nil(t, err)
		assert.Equal(t, data, val)
	}
}

func TestWal_NewIterator(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data := []byte("foo")
	loopTime := blockSize

	// write
	for i := 0; i < loopTime; i++ {
		chunk, err := wal.Write(data)
		assert.Nil(t, err)
		assert.NotNil(t, chunk)
	}

	// read
	readTime := 0
	iter := wal.NewIterator()
	for {
		ret, err := iter.Next()
		if err == io.EOF {
			break
		}
		readTime += 1
		assert.Equal(t, data, ret)
	}
	assert.Equal(t, loopTime, readTime)
}

func TestWal_ReadButFailed(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize * 15,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	data, err := wal.Read(&Chunk{
		segmentId: 10,
	})
	assert.Nil(t, data)
	assert.NotNil(t, err)
}

func TestWal_Close(t *testing.T) {
	wal, err := Open(Options{
		Dir:         os.TempDir(),
		SegmentSize: blockSize * 15,
	})
	assert.Nil(t, err)
	defer func() {
		_ = wal.Delete()
	}()

	// write
	_, err = wal.Write([]byte("abc"))
	assert.Nil(t, err)

	// close
	err = wal.Close()
	assert.Nil(t, err)

	// write
	_, err = wal.Write([]byte("abc"))
	assert.NotNil(t, err)
}

func TestWal_Delete(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "TestWal_Delete")
	assert.Nil(t, err)

	wal, err := Open(Options{
		Dir:         tempDir,
		SegmentSize: blockSize * 15,
	})
	assert.Nil(t, err)

	// write
	_, err = wal.Write([]byte("abc"))
	assert.Nil(t, err)
	// not empty
	assert.NotEqual(t, int64(0), wal.activeSegment.Size())

	// delete
	err = wal.Delete()
	assert.Nil(t, err)

	// reopen
	wal, err = Open(Options{
		Dir:         tempDir,
		SegmentSize: blockSize * 15,
	})
	defer func() {
		_ = wal.Delete()
	}()
	assert.Nil(t, err)
	// empty
	assert.Equal(t, int64(0), wal.activeSegment.Size())
}
