package kv_db

import (
	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
	"kv-db/wal"
	"testing"
	"time"
)

func TestRecord_isExpired(t *testing.T) {
	r := &logRecord{
		recordType: recordModified,
		expire:     0,
		key:        []byte("a"),
		value:      []byte("v"),
	}

	now := time.Now()
	assert.False(t, r.isExpired(now.UnixNano()))

	r.expire = now.Add(100).UnixNano()
	assert.False(t, r.isExpired(now.Add(99).UnixNano()))
	assert.True(t, r.isExpired(now.Add(100).UnixNano()))
}

func TestRecord_encodeLogRecord(t *testing.T) {
	buffer := bytebufferpool.Get()
	buffer.Reset()
	defer bytebufferpool.Put(buffer)

	header := make([]byte, 20)
	r := &logRecord{
		recordType: recordDeleted,
		expire:     99999,
		key:        []byte("foo"),
		value:      []byte("bar"),
	}

	bytes := encodeLogRecord(r, header, buffer)
	r2 := decodeLogRecord(bytes)

	assert.Equal(t, r, r2)
}

func TestRecord_encodeHintRecord(t *testing.T) {
	key := []byte("abc")
	pos := &wal.Chunk{
		SegmentId:   1,
		BlockIndex:  2,
		BlockOffset: 3,
		Size:        4,
	}

	bytes := encodeHintRecord(key, pos)

	k, v := decodeHintRecord(bytes)

	assert.Equal(t, key, k)
	assert.Equal(t, pos, v)
}
