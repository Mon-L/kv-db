package kv_db

import (
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"kv-db/wal"
)

type recordType = byte

const (
	recordModified recordType = iota
	recordDeleted
)

type logRecord struct {
	recordType recordType
	expire     int64
	key        []byte // UnixNano
	value      []byte
}

func (lr *logRecord) isExpired(now int64) bool {
	return lr.expire > 0 && lr.expire <= now
}

// type    expire   keySize   valueSize   key   value
//  1      10 max     5 max      5 max     ...    ...
func encodeLogRecord(r *logRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	// type
	header[0] = r.recordType
	var index = 1

	// expire
	index += binary.PutVarint(header[index:], r.expire)

	// keySize
	index += binary.PutVarint(header[index:], int64(len(r.key)))

	// valueSize
	index += binary.PutVarint(header[index:], int64(len(r.value)))

	_, _ = buf.Write(header[:index])
	_, _ = buf.Write(r.key)
	_, _ = buf.Write(r.value)

	return buf.B
}

func decodeLogRecord(data []byte) *logRecord {
	recordType := data[0]
	var index = 1

	expire, n := binary.Varint(data[index:])
	index += n

	keySize, n := binary.Varint(data[index:])
	index += n

	valueSize, n := binary.Varint(data[index:])
	index += n

	key := make([]byte, keySize)
	copy(key, data[index:index+int(keySize)])
	index += int(keySize)

	value := make([]byte, valueSize)
	copy(value, data[index:index+int(valueSize)])

	return &logRecord{
		recordType: recordType,
		expire:     expire,
		key:        key,
		value:      value,
	}
}

//segmentId(uint32) / blockIndex(uint32) / blockOffset(uint32) / size(uint32)
//	   5					5					5				    5
func encodeHintRecord(key []byte, pos *wal.Chunk) []byte {
	buf := make([]byte, 20)
	idx := binary.PutUvarint(buf[0:], uint64(pos.SegmentId))
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockIndex))
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockOffset))
	idx += binary.PutUvarint(buf[idx:], uint64(pos.Size))

	ret := make([]byte, idx+len(key))
	copy(ret, buf[:idx])
	copy(ret[idx:], key)
	return ret
}

func decodeHintRecord(data []byte) ([]byte, *wal.Chunk) {
	var idx = 0
	segId, n := binary.Uvarint(data)
	idx += n

	blockIndex, n := binary.Uvarint(data[idx:])
	idx += n

	blockOffset, n := binary.Uvarint(data[idx:])
	idx += n

	Size, n := binary.Uvarint(data[idx:])
	idx += n

	key := data[idx:]

	return key, &wal.Chunk{
		SegmentId:   uint32(segId),
		BlockIndex:  uint32(blockIndex),
		BlockOffset: uint32(blockOffset),
		Size:        uint32(Size),
	}
}
