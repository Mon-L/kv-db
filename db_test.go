package kv_db

import (
	"github.com/stretchr/testify/assert"
	"kv-db/wal"
	"os"
	"strconv"
	"testing"
)

func dbTestOpenDB() (*DB, error) {
	tempDir, err := os.MkdirTemp("", "TestDB")
	if err != nil {
		return nil, err
	}

	return Open(Options{
		Dir:         tempDir,
		SegmentSize: 1024 * wal.KB,
	})
}

func TestDB_Put(t *testing.T) {
	db, err := dbTestOpenDB()
	assert.Nil(t, err)
	defer deleteDB(db)

	val := []byte("xxxx")
	for i := 1; i <= 100; i++ {
		err = db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	assert.Equal(t, 100, db.indexer.Size())
}

func TestDB_Open(t *testing.T) {
	db, err := dbTestOpenDB()
	assert.Nil(t, err)
	defer deleteDB(db)

	val := []byte("xxxx")
	for i := 1; i <= 100000; i++ {
		err = db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	// close db
	err = db.Close()
	assert.Nil(t, err)

	// reopen db
	db, err = Open(Options{
		Dir:         db.options.Dir,
		SegmentSize: 1024 * wal.KB,
	})
	assert.Nil(t, err)

	for i := 1; i <= 100000; i++ {
		ret, err := db.Get([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, val, ret)
	}
}

func TestDB_Get(t *testing.T) {
	db, err := dbTestOpenDB()
	assert.Nil(t, err)
	defer deleteDB(db)

	val := []byte("xxxx")
	for i := 1; i <= 100000; i++ {
		err = db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	for i := 1; i <= 100000; i++ {
		ret, err := db.Get([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, val, ret)
	}
}

func TestDB_Delete(t *testing.T) {
	db, err := dbTestOpenDB()
	assert.Nil(t, err)
	defer deleteDB(db)

	val := []byte("xxxx")
	for i := 1; i <= 1; i++ {
		err = db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	for i := 1; i <= 1; i++ {
		key := []byte(strconv.Itoa(i))

		_, err := db.Get(key)
		assert.Nil(t, err)

		err = db.Delete(key)
		assert.Nil(t, err)

		_, err = db.Get(key)
		assert.NotNil(t, err)
	}
}
