package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// 7 Bytes： Checksum：4 Bytes； Length：2 Bytes；Type：1 Bytes
	chunkHeaderSize = 7
	blockSize       = 32 * KB

	fileModePerm = 0644
)

const (
	chunkTypeFull byte = iota
	chunkTypeStart
	chunkTypeMiddle
	chunkTypeEnd
)

var (
	segmentIsClosedErr = errors.New("segment file is closed")
	invalidCRC         = errors.New("invalid crc, the data may be damaged")
)

type segment struct {
	id                uint32
	fd                *os.File
	activeBlockIndex  uint32
	activeBlockOffset uint32
	closed            bool
	headerCache       []byte
}

type Chunk struct {
	SegmentId   uint32
	BlockIndex  uint32
	BlockOffset uint32
	Size        uint32
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	blockPool.Put(buf)
}

func openSegment(dirPath string, fileSuffix string, id uint32) (*segment, error) {
	path := JoinSegmentPath(dirPath, fileSuffix, id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, fileModePerm)
	if err != nil {
		return nil, err
	}

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &segment{
		id:                id,
		fd:                file,
		activeBlockIndex:  uint32(offset / blockSize),
		activeBlockOffset: uint32(offset % blockSize),
		closed:            false,
		headerCache:       make([]byte, chunkHeaderSize),
	}, nil
}

func JoinSegmentPath(dir string, fileSuffix string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%09d"+fileSuffix, id))
}

func (seg *segment) Write(data []byte) (*Chunk, error) {
	if seg.closed {
		return nil, segmentIsClosedErr
	}

	buffer := bytebufferpool.Get()
	buffer.Reset()
	defer func() {
		bytebufferpool.Put(buffer)
	}()

	var chunk *Chunk
	var err error

	if chunk, err = seg.writeToBuffer(data, buffer); err != nil {
		return nil, err
	}

	if err = seg.writeToSegment(buffer); err != nil {
		return nil, err
	}

	return chunk, nil
}

func (seg *segment) writeToBuffer(data []byte, buffer *bytebufferpool.ByteBuffer) (*Chunk, error) {
	if seg.closed {
		return nil, segmentIsClosedErr
	}

	// 如果当前 block 不能放下一个 chunk header, 添加 padding
	if chunkHeaderSize+seg.activeBlockOffset >= blockSize && seg.activeBlockOffset < blockSize {
		p := make([]byte, blockSize-seg.activeBlockOffset)
		buffer.B = append(buffer.B, p...)

		// 切换到下一个 block
		seg.activeBlockIndex += 1
		seg.activeBlockOffset = 0
	}

	chunk := &Chunk{
		SegmentId:   seg.id,
		BlockIndex:  seg.activeBlockIndex,
		BlockOffset: seg.activeBlockOffset,
	}

	dataSize := uint32(len(data))
	if seg.activeBlockOffset+dataSize+chunkHeaderSize <= blockSize {
		// 当前 block 能放下整个 chunk
		seg.fillBuffer(buffer, data, chunkTypeFull)
		chunk.Size = dataSize + chunkHeaderSize
	} else {
		// 当前 block 不能放下整个 chunk，使用多个 block 存储
		var chunkCount uint32 = 0
		start := 0
		remaining := len(data)
		offset := (int)(seg.activeBlockOffset)

		for remaining > 0 {
			free := blockSize - offset

			var chunkType byte
			switch {
			case start == 0:
				chunkType = chunkTypeStart
			case free < chunkHeaderSize+remaining:
				chunkType = chunkTypeMiddle
			default:
				chunkType = chunkTypeEnd
			}

			end := start + (free - chunkHeaderSize)
			if end > len(data) {
				end = len(data)
			}
			seg.fillBuffer(buffer, data[start:end], chunkType)
			chunkCount++
			start = end
			offset = (offset + free) % blockSize
			remaining = int(dataSize) - start
		}
		chunk.Size = chunkCount*chunkHeaderSize + dataSize
	}

	seg.activeBlockOffset += chunk.Size
	if seg.activeBlockOffset >= blockSize {
		// 切换 block
		seg.activeBlockIndex += seg.activeBlockOffset / blockSize
		seg.activeBlockOffset = seg.activeBlockOffset % blockSize
	}
	return chunk, nil
}

func (seg *segment) writeToSegment(buffer *bytebufferpool.ByteBuffer) error {
	if _, err := seg.fd.Write(buffer.B); err != nil {
		return err
	}
	return nil
}

func (seg *segment) Sync() error {
	if seg.closed {
		return segmentIsClosedErr
	}
	return seg.fd.Sync()
}

func (seg *segment) Read(blockIndex uint32, offset uint32) ([]byte, error) {
	data, _, err := seg.doRead(blockIndex, offset)
	return data, err
}

func (seg *segment) doRead(blockIndex uint32, offset uint32) ([]byte, *Chunk, error) {
	if seg.closed {
		return nil, nil, segmentIsClosedErr
	}

	block := getBuffer()
	if len(block) != blockSize {
		block = make([]byte, blockSize)
	}
	defer putBuffer(block)

	var data []byte
	nextChunk := &Chunk{
		SegmentId: seg.id,
	}

	for {
		size := int64(blockSize)
		segSize := seg.Size()
		blockOffset := int64(blockIndex) * blockSize

		// 当要读取的数据在最新的 block 中，且最新的 block 未满时，调整要读取的 block 的大小
		if blockSize+blockOffset > segSize {
			size = segSize - blockOffset
		}

		if (int64)(offset) >= size {
			return nil, nil, io.EOF
		}

		if _, err := seg.fd.ReadAt(block[0:size], blockOffset); err != nil {
			return nil, nil, err
		}

		header := block[offset : offset+chunkHeaderSize]

		savedChecksum := binary.LittleEndian.Uint32(header[0:4])
		length := binary.LittleEndian.Uint16(header[4:6])
		chunkType := header[6]

		dataStart := (int64)(offset) + chunkHeaderSize
		dataEnd := dataStart + int64(length)
		checksum := crc32.ChecksumIEEE(block[offset+4 : dataEnd])
		if checksum != savedChecksum {
			return nil, nil, invalidCRC
		}

		data = append(data, block[dataStart:dataEnd]...)
		if chunkType == chunkTypeFull || chunkType == chunkTypeEnd {
			nextChunk.BlockIndex = blockIndex
			nextChunk.BlockOffset = uint32(dataEnd)
			if dataEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockIndex += 1
				nextChunk.BlockOffset = 0
			}
			break
		}
		blockIndex++
		offset = 0
	}

	return data, nextChunk, nil
}

func (seg *segment) calMaxRequiredCapacity(dataSize int) int {
	return chunkHeaderSize + (dataSize/blockSize+1)*chunkHeaderSize + dataSize
}

func (seg *segment) Size() int64 {
	size := int64(seg.activeBlockIndex) * int64(blockSize)
	return size + int64(seg.activeBlockOffset)
}

func (seg *segment) fillBuffer(buffer *bytebufferpool.ByteBuffer, data []byte, chunkType byte) {
	seg.headerCache[6] = chunkType
	binary.LittleEndian.PutUint16(seg.headerCache[4:6], uint16(len(data)))

	sum := crc32.ChecksumIEEE(seg.headerCache[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.headerCache[0:4], sum)

	buffer.B = append(buffer.B, seg.headerCache...)
	buffer.B = append(buffer.B, data...)
}

func (seg *segment) Remove() error {
	if !seg.closed {
		if err := seg.Close(); err != nil {
			return err
		}
	}

	return os.Remove(seg.fd.Name())
}

func (seg *segment) Close() error {
	if !seg.closed {
		seg.closed = true
		return seg.fd.Close()
	}
	return nil
}
