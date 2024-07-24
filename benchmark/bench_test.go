package benchmark

import (
	"errors"
	"github.com/stretchr/testify/assert"
	kvDB "kv-db"
	"os"
	"strconv"
	"testing"
)

var db *kvDB.DB
var dir = "/tmp/kvdb"

func openDB() {
	var options = kvDB.DefaultOptions
	options.Dir = dir

	var err error
	db, err = kvDB.Open(options)
	if err != nil {
		panic(err)
	}
}

func deleteDB() {
	_ = db.Close()
	_ = os.RemoveAll(dir)
}

func BenchmarkPutGet(b *testing.B) {
	openDB()
	defer deleteDB()

	b.Run("get", benchmarkGet)
	b.Run("put", benchmarkPut)
}

func benchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	val := []byte("abc")
	for i := 0; i < b.N; i++ {
		err := db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(b, err)
	}
}

func benchmarkGet(b *testing.B) {
	val := []byte("abc")
	for i := 0; i < 10000; i++ {
		err := db.Put([]byte(strconv.Itoa(i)), val)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get([]byte(strconv.Itoa(i)))
		if err != nil && !errors.Is(err, kvDB.ErrKeyNotFound) {
			b.Fatal(err)
		}
	}
}
