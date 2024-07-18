package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

type Wal struct {
	options          Options
	activeSegment    *segment
	readOnlySegments map[int]*segment
	mu               sync.RWMutex
}

type Iterator struct {
	segments        []*segment
	segmentIdx      int
	nextBlockIdx    uint32
	nextBlockOffset uint32
}

func (iter *Iterator) Next() ([]byte, error) {
	if iter.segmentIdx >= len(iter.segments) {
		return nil, io.EOF
	}

	segment := iter.segments[iter.segmentIdx]
	data, next, err := segment.doRead(iter.nextBlockIdx, iter.nextBlockOffset)
	if err != nil {
		if err == io.EOF {
			iter.segmentIdx++
			iter.nextBlockIdx = 0
			iter.nextBlockOffset = 0
			return iter.Next()
		}
		return nil, err
	} else {
		iter.nextBlockIdx = next.blockIndex
		iter.nextBlockOffset = next.blockOffset
	}

	return data, err
}

func Open(options Options) (*Wal, error) {
	if err := os.MkdirAll(options.Dir, os.ModePerm); err != nil {
		return nil, err
	}

	wal := &Wal{
		options:          options,
		readOnlySegments: make(map[int]*segment),
	}

	if err := initSegments(wal); err != nil {
		return nil, err
	}

	return wal, nil
}

func initSegments(wal *Wal) error {
	options := wal.options
	entries, err := os.ReadDir(options.Dir)
	if err != nil {
		return err
	}

	var ids []int
	for _, entry := range entries {
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+SegmentSuffix, &id)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		firstSegment, err := openSegment(options.Dir, 1)
		if err != nil {
			return err
		}
		wal.activeSegment = firstSegment
		return nil
	} else {
		sort.Ints(ids)
		for i, id := range ids {
			segment, err := openSegment(options.Dir, uint32(id))
			if err != nil {
				return err
			}

			if i == len(ids)-1 {
				wal.activeSegment = segment
			} else {
				wal.readOnlySegments[id] = segment
			}
		}
		return nil
	}
}

func (wal *Wal) NewIterator() *Iterator {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segments []*segment
	segments = append(segments, wal.activeSegment)
	for _, s := range wal.readOnlySegments {
		segments = append(segments, s)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	return &Iterator{
		segments:        segments,
		segmentIdx:      0,
		nextBlockIdx:    0,
		nextBlockOffset: 0,
	}
}

func (wal *Wal) Write(data []byte) (*Chunk, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	dataSize := len(data)
	maxRequiredCapacity := int64(wal.activeSegment.calMaxRequiredCapacity(dataSize))

	if maxRequiredCapacity > wal.options.SegmentSize {
		return nil, errors.New("required capacity is larger than segment size")
	}

	if maxRequiredCapacity+wal.activeSegment.Size() > wal.options.SegmentSize {
		if err := wal.switchNewSegment(); err != nil {
			return nil, err
		}
	}

	chunk, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	return chunk, nil
}

func (wal *Wal) Read(chunk *Chunk) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segment *segment
	if chunk.segmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.readOnlySegments[int(chunk.segmentId)]
		if segment == nil {
			return nil, fmt.Errorf("inexistent segment: %d", chunk.segmentId)
		}
	}

	return segment.Read(chunk.blockIndex, chunk.blockOffset)
}

func (wal *Wal) switchNewSegment() error {
	oldSegment := wal.activeSegment
	if err := oldSegment.Sync(); err != nil {
		return err
	}

	newSegment, err := openSegment(wal.options.Dir, oldSegment.id+1)
	if err != nil {
		return err
	}
	wal.activeSegment = newSegment
	wal.readOnlySegments[int(oldSegment.id)] = oldSegment
	return nil
}

func (wal *Wal) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func (wal *Wal) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, seg := range wal.readOnlySegments {
		if !seg.closed {
			if err := seg.Close(); err != nil {
				return err
			}
		}
	}

	return wal.activeSegment.Close()
}

func (wal *Wal) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, seg := range wal.readOnlySegments {
		if err := seg.Remove(); err != nil {
			return err
		}
	}

	return wal.activeSegment.Remove()
}
