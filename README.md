# Big Data Binary Index
A hands on project to learn how large files are handled by converting to a binary format, building an index for fast querying and benchmarking before and after indexing.
This repository contains two approaches:
1. Parquet based - uses pyarrow to convert JSON -> Parquet. Good compression, columnar layout and built in row group access.
2. Custom binary + index - writes records as fixed length bytes to a binary file. Builds an external index mapping a chosen key to file offsets.

## Key Design Decisions
 - Memory safety / Streaming: Always stream input JSON instead of loading the whole file into RAM. Use ijson or line-by-line NDJSON parsing.
 - Index: For custom binary, use SQLite mapping key -> offset, length. For Parquet, build a small index mapping key -> (row_group, row_index).
 - Benchmarks: Measure index build time, disk usage, average latency for point-lookup queries (random keys) and throughput for bulk reads.
