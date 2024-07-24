package kv_db

import (
	"io"
	"io/ioutil"
	"kv-db/index"
	"kv-db/util"
	"kv-db/wal"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func (db *DB) merge() error {
	mergeDir := filepath.Join(db.options.Dir, "merge")
	if err := db.cleanDir(mergeDir); err != nil {
		return err
	}
	if err := db.doMerge(mergeDir); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.replaceSegmentFile(mergeDir); err != nil {
		return err
	}

	walFile, err := db.openWalFiles()
	if err != nil {
		return err
	}
	db.wal = walFile

	db.indexer = index.NewIndexer()
	return db.loadIndex()
}

func (db *DB) doMerge(mergeDir string) error {
	mergeDB, err := Open(Options{
		Dir:           mergeDir,
		SegmentSize:   db.options.SegmentSize,
		AutoMergeExpr: "",
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	if mergeDB.hintWal, err = mergeDB.openHintWal(); err != nil {
		return err
	}

	prevSegId, err := db.wal.SwitchNewSegmentForce()
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	walIter := db.wal.NewIteratorLessEqual(prevSegId)
	for {
		data, oldPos, err := walIter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		record := decodeLogRecord(data)
		if record.recordType == recordModified && (record.expire == 0 || !record.isExpired(now)) {
			db.mu.Lock()
			newPos := db.indexer.Get(record.key)
			db.mu.Unlock()

			if newPos != nil &&
				newPos.SegmentId == oldPos.SegmentId &&
				newPos.BlockIndex == oldPos.BlockIndex &&
				newPos.BlockOffset == oldPos.BlockOffset {

				newChunk, err := mergeDB.wal.Write(data)
				if err != nil {
					return err
				}

				_, err = mergeDB.hintWal.Write(encodeHintRecord(record.key, newChunk))
				if err != nil {
					return err
				}
			}
		}
	}

	return db.writeMergeFinFile(mergeDir, prevSegId)
}

func (db *DB) replaceSegmentFile(mergeDir string) error {
	dbDir := db.options.Dir
	if _, err := os.Stat(mergeDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() {
		_ = os.RemoveAll(mergeDir)
	}()

	maxSegId, err := db.readMergeFinFile(mergeDir)
	if err != nil {
		return err
	}

	for i := uint32(1); i <= maxSegId; i++ {
		path := wal.JoinSegmentPath(dbDir, wal.SegmentSuffix, i)
		if _, err = os.Stat(path); err == nil {
			if err = os.Remove(path); err != nil {
				return err
			}
		}
	}

	for i := uint32(1); i <= maxSegId; i++ {
		err := util.CopyFile(wal.JoinSegmentPath(mergeDir, wal.SegmentSuffix, i), wal.JoinSegmentPath(dbDir, wal.SegmentSuffix, i))
		if err != nil {
			return err
		}
	}

	err = util.CopyFile(wal.JoinSegmentPath(mergeDir, HintSuffix, 1), wal.JoinSegmentPath(dbDir, HintSuffix, 1))
	if err != nil {
		return err
	}

	return util.CopyFile(filepath.Join(mergeDir, MergeFinSuffix), filepath.Join(dbDir, MergeFinSuffix))
}

func (db *DB) openHintWal() (*wal.Wal, error) {
	w, err := wal.Open(wal.Options{
		Dir:               db.options.Dir,
		SegmentSize:       1 * wal.GB,
		SegmentFileSuffix: HintSuffix,
	})

	if err != nil {
		return nil, err
	}
	return w, err
}

func (db *DB) cleanDir(mergeDir string) error {
	if err := os.MkdirAll(mergeDir, os.ModePerm); err != nil {
		return err
	}

	if err := os.RemoveAll(mergeDir); err != nil {
		return err
	}

	return nil
}

func (db *DB) writeMergeFinFile(dir string, maxSegmentId uint32) error {
	f, err := os.OpenFile(filepath.Join(dir, MergeFinSuffix), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = f.WriteString(strconv.Itoa(int(maxSegmentId)))
	return err
}

func (db *DB) readMergeFinFile(dir string) (uint32, error) {
	bytes, err := ioutil.ReadFile(filepath.Join(dir, MergeFinSuffix))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	id, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, err
	}
	return uint32(id), err
}
