package kv_db

import (
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func deleteDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.Dir)
}

func openDB(options Options) (*DB, error) {
	return Open(options)
}

func TestDB_MergeAllValid(t *testing.T) {
	options := DefaultOptions
	db, err := openDB(options)
	assert.Nil(t, err)
	defer deleteDB(db)

	// put
	val := []byte("abc")
	for i := 0; i < 10000; i++ {
		err := db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	// merge
	err = db.merge()
	assert.Nil(t, err)

	// reopen db
	err = db.Close()
	assert.Nil(t, err)
	db, err = openDB(options)
	assert.Nil(t, err)

	assert.Equal(t, 10000, db.indexer.Size())
	for i := 0; i < 10000; i++ {
		data, err := db.Get([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, val, data)
	}
}

func TestDB_MergeAllInvalid(t *testing.T) {
	options := DefaultOptions
	db, err := openDB(options)
	assert.Nil(t, err)
	defer deleteDB(db)

	// put
	val := []byte("abc")
	for i := 0; i < 10000; i++ {
		err := db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	// delete all
	for i := 0; i < 10000; i++ {
		err := db.Delete([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
	}

	// merge
	err = db.merge()
	assert.Nil(t, err)

	// empty keys
	assert.Equal(t, 0, db.indexer.Size())

	// reopen db
	err = db.Close()
	assert.Nil(t, err)
	db, err = openDB(options)

	// empty keys
	assert.Equal(t, 0, db.indexer.Size())
}

func TestDB_Merge(t *testing.T) {
	options := DefaultOptions
	db, err := openDB(options)
	assert.Nil(t, err)
	defer deleteDB(db)

	// put 0 ~ 19999
	val := []byte("abc")
	for i := 0; i < 20000; i++ {
		err := db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(t, err)
	}

	// delete 10000 ~ 19999
	for i := 10000; i < 20000; i++ {
		err := db.Delete([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
	}

	// replace 0 ~ 4999
	val2 := []byte("abc2")
	for i := 0; i < 4999; i++ {
		err := db.Put([]byte(strconv.Itoa(i)), val2)
		assert.Nil(t, err)
	}

	// merge
	err = db.merge()
	assert.Nil(t, err)
	assert.Equal(t, 10000, db.indexer.Size())

	for i := 0; i < 4999; i++ {
		data, err := db.Get([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, val2, data)
	}

	for i := 5000; i < 10000; i++ {
		data, err := db.Get([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, val, data)
	}
}
