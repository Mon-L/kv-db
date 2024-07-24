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
	Dir               string
	SegmentSize       int64
	SegmentFileSuffix string

	// 0-不需要同步数据到硬盘，1-每次写入都需要同步数据到硬盘，2-当写入多少字节后需要同步硬盘，与BytesBeforeSync配合使用
	Sync            int
	BytesBeforeSync uint32
}

var DefaultOptions = &Options{
	Dir:               os.TempDir(),
	SegmentSize:       GB,
	SegmentFileSuffix: SegmentSuffix,
	Sync:              0,
}
