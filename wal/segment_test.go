package wal

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestSegment_Write(t *testing.T) {
	segment, nil := openSegment(os.TempDir(), 1)
	assert.Nil(t, nil)
	defer func() {
		_ = segment.Remove()
	}()

	val := []byte(strconv.Itoa(rand.Intn(10)))
	chunk, err := segment.Write(val)
	assert.Nil(t, err)

	ret1, err1 := segment.Read(chunk.blockIndex, chunk.blockOffset)
	assert.Nil(t, err1)
	assert.EqualValues(t, val, ret1)
}

func TestSegment_WriteFull(t *testing.T) {
	segment, nil := openSegment(os.TempDir(), 0)
	assert.Nil(t, nil)
	defer func() {
		_ = segment.Remove()
	}()

	// write full
	val := []byte(strings.Repeat("a", blockSize-chunkHeaderSize))
	chunk, err := segment.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), chunk.blockIndex)
	assert.Equal(t, uint32(0), chunk.blockOffset)

	// should switch new block
	assert.Equal(t, uint32(1), segment.activeBlockIndex)
	assert.Equal(t, uint32(0), segment.activeBlockOffset)
}

func TestSegment_WritePadding(t *testing.T) {
	segment, nil := openSegment(os.TempDir(), 0)
	assert.Nil(t, nil)
	defer func() {
		_ = segment.Remove()
	}()

	val := []byte(strings.Repeat("a", blockSize-chunkHeaderSize-5))
	chunk, err := segment.Write(val)
	assert.Equal(t, uint32(0), chunk.blockIndex)
	assert.Nil(t, err)

	chunk2, err2 := segment.Write([]byte("a"))
	assert.Equal(t, uint32(1), chunk2.blockIndex)
	assert.Nil(t, err2)
}

func TestSegment_WriteCrossBlock(t *testing.T) {
	segment, nil := openSegment(os.TempDir(), 0)
	assert.Nil(t, nil)
	defer func() {
		_ = segment.Remove()
	}()

	val := []byte(strings.Repeat("a", blockSize-chunkHeaderSize*2-1))
	_, err := segment.Write(val)
	assert.Nil(t, err)

	val2 := []byte("abc")
	chunk2, err2 := segment.Write(val2)
	assert.Nil(t, err2)
	assert.Equal(t, uint32(0), chunk2.blockIndex)

	ret2, err2 := segment.Read(chunk2.blockIndex, chunk2.blockOffset)
	assert.Nil(t, err2)
	assert.EqualValues(t, val2, ret2)
}

func TestSegment_WriteCrossBlock2(t *testing.T) {
	segment, nil := openSegment(os.TempDir(), 0)
	assert.Nil(t, nil)
	defer func() {
		_ = segment.Remove()
	}()

	val := []byte(strings.Repeat("a", blockSize-100))
	_, err := segment.Write(val)
	assert.Nil(t, err)

	val2 := []byte(strings.Repeat("z", (blockSize-chunkHeaderSize)*3))
	chunk2, err2 := segment.Write(val2)

	assert.Nil(t, err2)
	assert.Equal(t, uint32(0), chunk2.blockIndex)
	assert.EqualValues(t, uint32(3), segment.activeBlockIndex)

	ret2, err2 := segment.Read(chunk2.blockIndex, chunk2.blockOffset)
	assert.Nil(t, err2)
	assert.EqualValues(t, val2, ret2)
}
