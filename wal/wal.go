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
	options       Options
	activeSegment *segment
	olderSegments map[int]*segment
	mu            sync.RWMutex
	byteWritten   uint32
}

type Iterator struct {
	segments        []*segment
	segmentIdx      int
	nextBlockIdx    uint32
	nextBlockOffset uint32
}

func (wal *Wal) NewIterator() *Iterator {
	return wal.NewIteratorLessEqual(4294967295)
}

func (wal *Wal) NewIteratorLessEqual(maxId uint32) *Iterator {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segments []*segment
	segments = append(segments, wal.activeSegment)
	for _, s := range wal.olderSegments {
		if s.id <= maxId {
			segments = append(segments, s)
		}
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

func (iter *Iterator) SkipSegmentLessEqual(id uint32) {
	if iter.segmentIdx != 0 {
		return
	}

	for i, seg := range iter.segments {
		if seg.id > id {
			iter.segmentIdx = i
			break
		}
	}
}

func (iter *Iterator) Next() ([]byte, *Chunk, error) {
	if iter.segmentIdx >= len(iter.segments) {
		return nil, nil, io.EOF
	}

	pos := &Chunk{
		BlockIndex:  iter.nextBlockIdx,
		BlockOffset: iter.nextBlockOffset,
	}

	segment := iter.segments[iter.segmentIdx]
	data, next, err := segment.doRead(iter.nextBlockIdx, iter.nextBlockOffset)
	if err != nil {
		if err != io.EOF {
			return nil, nil, err
		}

		iter.segmentIdx++
		iter.nextBlockIdx = 0
		iter.nextBlockOffset = 0
		return iter.Next()
	}

	pos.SegmentId = segment.id
	pos.Size = uint32(len(data))
	iter.nextBlockIdx = next.BlockIndex
	iter.nextBlockOffset = next.BlockOffset
	return data, pos, err
}

func Open(options Options) (*Wal, error) {
	if err := os.MkdirAll(options.Dir, os.ModePerm); err != nil {
		return nil, err
	}

	wal := &Wal{
		options:       options,
		olderSegments: make(map[int]*segment),
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
		if _, err := fmt.Sscanf(entry.Name(), "%d"+wal.options.SegmentFileSuffix, &id); err != nil {
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		firstSegment, err := openSegment(options.Dir, options.SegmentFileSuffix, 1)
		if err != nil {
			return err
		}
		wal.activeSegment = firstSegment
	} else {
		sort.Ints(ids)
		for i, id := range ids {
			segment, err := openSegment(options.Dir, options.SegmentFileSuffix, uint32(id))
			if err != nil {
				return err
			}

			if i == len(ids)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[id] = segment
			}
		}
	}
	return nil
}

func (wal *Wal) Write(data []byte) (*Chunk, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	dataSize := len(data)
	maxRequiredCapacity := int64(wal.activeSegment.calMaxRequiredCapacity(dataSize))

	if maxRequiredCapacity > wal.options.SegmentSize {
		return nil, errors.New("required capacity is larger than segment Size")
	}

	if maxRequiredCapacity+wal.activeSegment.Size() > wal.options.SegmentSize {
		if err := wal.switchNewSegment(); err != nil {
			return nil, err
		}
	}

	pos, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	needSync := false
	if wal.options.Sync == 1 {
		needSync = true
	} else if wal.options.Sync == 2 {
		wal.byteWritten += pos.Size
		if wal.byteWritten >= wal.options.BytesBeforeSync {
			needSync = true
			wal.byteWritten = 0
		}
	}

	if needSync {
		if err = wal.Sync(); err != nil {
			return nil, err
		}
	}

	return pos, nil
}

func (wal *Wal) Read(chunk *Chunk) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segment *segment
	if chunk.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[int(chunk.SegmentId)]
		if segment == nil {
			return nil, fmt.Errorf("inexistent segment: %d", chunk.SegmentId)
		}
	}

	return segment.Read(chunk.BlockIndex, chunk.BlockOffset)
}

func (wal *Wal) switchNewSegment() error {
	oldSegment := wal.activeSegment
	if err := oldSegment.Sync(); err != nil {
		return err
	}

	newSegment, err := openSegment(wal.options.Dir, wal.options.SegmentFileSuffix, oldSegment.id+1)
	if err != nil {
		return err
	}

	wal.activeSegment = newSegment
	wal.olderSegments[int(oldSegment.id)] = oldSegment
	return nil
}

func (wal *Wal) SwitchNewSegmentForce() (uint32, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	prevSegId := wal.activeSegment.id
	return prevSegId, wal.switchNewSegment()
}

func (wal *Wal) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func (wal *Wal) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, seg := range wal.olderSegments {
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

	for _, seg := range wal.olderSegments {
		if err := seg.Remove(); err != nil {
			return err
		}
	}

	return wal.activeSegment.Remove()
}
