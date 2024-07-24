# KV-DB

KV 存储指的是键值对数据类型的存储。Key 是唯一标识符，Value 是该标识符对应的值。 一般 KV 存储模型分为两种，一种是基于 B+Tree 的，另一种是基于 LSM Tree的。
和 B+Tree 不同，在 LSM Tree中数据的插入、更新、删除都会被记录成一条日志，然后追加写入到磁盘文件当中，这样所有的操作都是顺序 IO，因此 LSM Tree的写入速度
远超 B+Tree，但是 LSM Tree读取效率较低。

在 KV 存储中可以将所有 key 加载到内存中，加快读取效率。Key 的数据结构在内存中可以选择 B+Tree 或 HashMap。 相比 Redis 将所有数据都放在内存中可节省大量内存，
同时又实现了持久化功能。

本项目的 LSM Tree 是基于 [Bitcask](https://riak.com/assets/bitcask-intro.pdf) 模型的，最初由商业化存储公司 Riak 提出的。

## Segment

存储 LSM Tree日志，一个 DB 由多个 Segment 文件组成。一个 Segment 文件写满后再新建另一个 Segment 文件。

```
  Segment01    Segment02
+----------+  +----------+
|  chunk   |  |          |
|----------|  |----------|
|  chunk   |  |  chunk   |
|----------|  |----------|
|  chunk   |  |  chunk   |
|----------|  |----------|
|  chunk   |  |  chunk   |
+----------+  +----------+
```

## Chunk

存储每一条日志。当 Payload 大于 MaxChunkSize 时，分割成多个 Chunk 存储。
一个 Chunk 是每次读取的最小单元。

```
+---------+-------------+-----------+--- ... ---+
|  CRC32  |    Size     |   Type    |  Payload  |
+---------+-------------+-----------+--- ... ---+

CRC32: 4 Bytes
Size: 2 Bytes
Type: 1 Bytes
Payload： byte[]
```

## Add Log

## Merge Log