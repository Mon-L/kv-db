package wal

import "os"

const (
	B             = 1
	KB            = 1024 * B
	MB            = 1024 * KB
	GB            = 1024 * MB
	SegmentSuffix = ".seg"
)

type Options struct {
	Dir         string
	SegmentSize int64
}

var DefaultOptions = &Options{
	Dir:         os.TempDir(),
	SegmentSize: GB,
}
