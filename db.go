package kv_db

import (
	"errors"
	"github.com/robfig/cron/v3"
	"github.com/valyala/bytebufferpool"
	"io"
	"kv-db/index"
	"kv-db/wal"
	"sync"
	"time"
)

var (
	ErrEmptyKey    = errors.New("key can not be empty")
	ErrKeyNotFound = errors.New("key not found")
	ErrDBClosed    = errors.New("db is closed")
)

type DB struct {
	options         Options
	closed          bool
	mu              sync.RWMutex
	batchPool       sync.Pool
	indexer         index.Indexer
	wal             *wal.Wal
	hintWal         *wal.Wal
	walMergeTask    *cron.Cron
	logRecordHeader []byte
	recordPool      sync.Pool
}

func Open(options Options) (*DB, error) {
	db := &DB{
		options:         options,
		indexer:         index.NewIndexer(),
		logRecordHeader: make([]byte, 21),
		recordPool: sync.Pool{New: func() interface{} {
			return &logRecord{}
		}},
	}

	var err error
	if db.wal, err = db.openWalFiles(); err != nil {
		return nil, err
	}

	if err = db.loadIndex(); err != nil {
		return nil, err
	}

	if len(options.AutoMergeExpr) > 0 {
		db.walMergeTask = cron.New(cron.WithParser(cron.NewParser(
			cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		)))

		_, err := db.walMergeTask.AddFunc(options.AutoMergeExpr, func() {
			_ = db.merge()
		})

		if err != nil {
			return nil, err
		}

		db.walMergeTask.Start()
	}

	return db, nil
}

func (db *DB) Close() error {
	if err := db.wal.Close(); err != nil {
		return err
	}

	if db.hintWal != nil {
		if err := db.hintWal.Close(); err != nil {
			return err
		}
	}

	if db.walMergeTask != nil {
		db.walMergeTask.Stop()
	}

	db.closed = true
	return nil
}

func (db *DB) openWalFiles() (*wal.Wal, error) {
	return wal.Open(wal.Options{
		Dir:               db.options.Dir,
		SegmentSize:       db.options.SegmentSize,
		SegmentFileSuffix: wal.SegmentSuffix,
	})
}

func (db *DB) loadIndex() error {
	if db.closed {
		return ErrDBClosed
	}

	if err := db.loadIndexFromHint(); err != nil {
		return err
	}

	if err := db.loadIndexFromWal(); err != nil {
		return err
	}
	return nil
}

func (db *DB) loadIndexFromHint() error {
	hintWal, err := db.openHintWal()
	if err != nil {
		return err
	}
	defer func() {
		_ = hintWal.Close()
	}()

	iter := hintWal.NewIterator()
	for {
		data, _, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, chunk := decodeHintRecord(data)
		db.indexer.Put(key, chunk)
	}
	return nil
}

func (db *DB) loadIndexFromWal() error {
	mergeFinSegId, err := db.readMergeFinFile(db.options.Dir)
	if err != nil {
		return err
	}

	walIter := db.wal.NewIterator()
	walIter.SkipSegmentLessEqual(mergeFinSegId)

	now := time.Now().UnixNano()
	for {
		data, pos, err := walIter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		record := decodeLogRecord(data)
		if record.recordType == recordModified && !record.isExpired(now) {
			db.indexer.Put(record.key, pos)
		} else if record.recordType == recordDeleted {
			db.indexer.Delete(record.key)
		}
	}
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	return db.PutWithTTL(key, value, 0)
}

func (db *DB) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if db.closed {
		return ErrDBClosed
	}

	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.Lock()
	r := db.recordPool.Get().(*logRecord)
	defer func() {
		db.recordPool.Put(r)
		db.mu.Unlock()
	}()

	r.key = key
	r.value = value
	r.recordType = recordModified

	if ttl.Nanoseconds() == 0 {
		r.expire = 0
	} else {
		r.expire = time.Now().Add(ttl).UnixNano()
	}
	return db.writeRecord(r)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed {
		return nil, ErrDBClosed
	}

	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	now := time.Now().UnixNano()
	chunk := db.indexer.Get(key)
	if chunk == nil {
		return nil, ErrKeyNotFound
	}

	data, err := db.wal.Read(chunk)
	if err != nil {
		return nil, err
	}

	r := decodeLogRecord(data)
	if r.recordType == recordDeleted {
		panic("Deleted data must not be found from the index")
	}

	if r.isExpired(now) {
		db.indexer.Delete(r.key)
		return nil, ErrKeyNotFound
	}
	return r.value, nil
}

func (db *DB) Delete(key []byte) error {
	if db.closed {
		return ErrDBClosed
	}

	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.Lock()
	r := db.recordPool.Get().(*logRecord)
	defer func() {
		db.recordPool.Put(r)
		db.mu.Unlock()
	}()

	r.key = key
	r.recordType = recordDeleted
	r.value = nil
	r.expire = 0
	return db.writeRecord(r)
}

func (db *DB) writeRecord(r *logRecord) error {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	bytes := encodeLogRecord(r, db.logRecordHeader, buf)
	pos, err := db.wal.Write(bytes)
	if err != nil {
		return err
	}

	// write index
	if r.recordType == recordDeleted {
		db.indexer.Delete(r.key)
	} else {
		db.indexer.Put(r.key, pos)
	}
	return nil
}
