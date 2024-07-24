package kv_db

import (
	"kv-db/wal"
	"os"
)

const (
	HintSuffix     = ".hint"
	MergeFinSuffix = ".fin"
)

type Options struct {
	Dir           string
	SegmentSize   int64
	AutoMergeExpr string
}

var DefaultOptions = Options{
	Dir:           os.TempDir(),
	SegmentSize:   1 * wal.GB,
	AutoMergeExpr: "",
}
