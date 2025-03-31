# CC-Store System Architecture

CC-Store is a high-performance storage system designed for efficiently managing and querying billions of Common Crawl web page records. This document outlines the overall architecture, data flow, storage structure, and key components of the system.

## Overview

CC-Store is built around several key design principles:

1. **Domain-centric storage organization** for efficient querying
2. **Optimized HTML storage** with high compression
3. **Multi-level metadata management**
4. **Support for incremental data updates**
5. **Apache Spark optimization**

## System Architecture

The system is organized into several logical layers:

```
+---------------------+
|     应用层          |
|   +-------------+   |
|   |  CCStore API|   |
|   +-------------+   |
+----------+----------+
           |
+----------v----------+
|     核心层          |
| +------------------+|
| |StorageBackend    ||
| +------------------+|
| |MetadataManager   ||
| +------------------+|
+----------+----------+
           |
+----------v----------+
|     存储层          |
| +------------------+|
| |ParquetStorage    ||
| +------------------+|
| |MetadataManager   ||
| +------------------+|
+----------+----------+
           |
+----------v----------+
|     物理存储        |
| +------------------+|
| |  S3/HDFS/本地存储||
| +------------------+|
+---------------------+
```

### Key Components

1. **CCStore API (Application Layer)**
   - Main entry point for users
   - Handles high-level operations like reading and writing documents
   - Manages coordination between components

2. **Core Layer**
   - **StorageBackend**: Manages storage of document data in Parquet format
   - **MetadataManager**: Tracks domain and file metadata 

3. **Storage Layer**
   - Implements specific storage strategies for different data types
   - Manages partitioning and optimization for performance

4. **Physical Storage**
   - Actual storage backend (S3, HDFS, local filesystem)
   - Raw binary data storage

## Data Flow

### Read Flow

1. User requests data for a specific domain through `CCStore` API
2. System queries `MetadataManager` to locate the relevant files
3. `StorageBackend` reads appropriate Parquet files based on metadata information
4. HTML content can be optionally included or excluded to optimize query performance
5. Results are returned to the user as a Spark DataFrame

### Write Flow

1. User submits data to be written through `CCStore` API
2. `StorageBackend` groups data by domain and date
3. Data is partitioned and written as Parquet files with optimized compression
4. `MetadataManager` updates domain and file metadata

## Storage Structure

CC-Store organizes data in a hierarchical structure optimized for domain-based queries:

```
storage_path/
├── data/                                     # Main data storage
│   ├── domain_bucket=000/                    # Domain buckets (hash-based sharding)
│   │   ├── domain=example.com/
│   │   │   ├── date=20230101/
│   │   │   │   ├── part-0000.parquet         # Partition files with all content
│   │   │   │   ├── part-0001.parquet
│   │   │   │   └── part-0002.parquet
│   │   │   └── date=20230102/
│   │   │       ├── part-0000.parquet
│   │   │       └── part-0001.parquet
│   │   └── domain=another.com/
│   │       └── ...
│   └── domain_bucket=001/
│       └── ...
└── metadata/                                 # Metadata storage
    ├── domains/                              # Domain metadata
    │   └── ...
    └── files/                                # File metadata
        └── ...
```

### Partitioning Strategy

The system automatically partitions data based on several factors:

1. **Domain-based partitioning**: Data is first organized by domain name
2. **Date-based partitioning**: Within each domain, data is organized by date
3. **Size-based partitioning**: For each domain and date, data is further partitioned into multiple part files

The size-based partitioning is determined by:
- Target part file size (default: 128MB)
- Minimum records per partition
- Estimated DataFrame size

This approach allows for:
- Efficient querying of specific domains and date ranges
- Parallel processing of large datasets
- Optimization of file sizes for storage and processing efficiency

### Storage Optimization

CC-Store employs several techniques to optimize storage:

1. **Advanced Parquet Format Configuration**
   - Column-level compression settings
   - Optimized page and row group sizes
   - Dictionary encoding for repeated values
   - Bloom filters for key columns like URLs

2. **HTML Column Optimization**
   - Special handling for large HTML content
   - High compression ratio using zstd or other algorithms
   - Options to exclude HTML content from queries when not needed

3. **Query Optimization**
   - Partition pruning to read only relevant files
   - Column projection to read only required columns
   - Predicate pushdown for filtering at storage level

## Metadata Model

CC-Store maintains several types of metadata to track the state of the system:

### Domain Metadata

Stores information about each domain in the system:
- Domain ID
- Bucket ID
- Total files, records, and size
- Date range (min/max dates)
- Other statistics

### File Metadata

Tracks information about each Parquet file:
- Domain ID
- Date
- Part ID
- File path and size
- Record count
- Timestamp range
- Checksum and other file attributes

## Performance Considerations

### Reading Performance

- Column-based storage allows selective reading of required fields
- HTML content can be excluded from queries when not needed
- Predefined partition structure enables efficient date range and domain queries
- Bloom filters accelerate point lookups

### Writing Performance

- Size-based partitioning automatically scales with data volume
- Domain and date isolation prevents large batch writes from impacting unrelated domains
- Advanced compression reduces storage costs with minimal CPU overhead
- Multi-level architecture enables parallel writing for different domains

## Conclusion

The CC-Store architecture is designed for high performance and scalability when working with large web crawl datasets. By organizing data around domains and dates, and employing advanced storage optimization techniques, it provides efficient storage and fast query capabilities for Common Crawl data. 