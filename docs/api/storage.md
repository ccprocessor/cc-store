# Storage API

This document describes the storage backend features of CC-Store.

## Overview

CC-Store's storage system is designed for efficiently storing and retrieving billions of Common Crawl documents. The system is optimized for domain-level access patterns and uses a hierarchical directory structure to organize data by domain.

Key features:
- Domain-centric storage organization
- Partitioning by domain hash for even distribution
- Support for multiple files per domain per date
- Automatic file splitting for large data volumes
- Metadata tracking for efficient access

## Storage Structure

CC-Store organizes data in a hierarchical structure:

```
s3://your-bucket/cc-data/data/
  ├── domain_bucket=001/        # First level: domain hash bucket
  │   ├── domain=01-news.ru/    # Second level: specific domain
  │   │   ├── data_20230101.parquet
  │   │   ├── data_20230201.parquet
  │   │   └── ...
  │   ├── domain=02-blog.com/
  │   │   └── ...
  │   └── ...
  ├── domain_bucket=002/
  │   └── ...
  └── ...
```

For domains with large amounts of data for a specific date, the system automatically splits the data into multiple files:

```
domain=example.com/
  ├── data_20230101_part0.parquet  # 2GB
  ├── data_20230101_part1.parquet  # 2GB
  ├── data_20230101_part2.parquet  # Remaining data
  └── data_20230202.parquet        # Under 2GB, no splitting needed
```

## API Reference

### StorageBackend

The core interface for storage backends.

```python
from cc_store.core import StorageBackend
```

#### read_by_domain

Read data for a specific domain.

```python
df = storage.read_by_domain(
    domain="example.com",
    date_range=("20230101", "20230131"),
    fields=["url", "html", "date"],
    limit=1000
)
```

##### Parameters

- `domain` (str): Domain name to retrieve data for
- `date_range` (tuple, optional): Tuple of (start_date, end_date) in YYYYMMDD format
- `fields` (list, optional): List of fields to include
- `limit` (int, optional): Maximum number of records to return

##### Returns

- `SparkDataFrame`: Spark DataFrame containing the requested data

#### read_by_domains

Read data for multiple domains.

```python
df = storage.read_by_domains(
    domains=["example.com", "example.org"],
    date_range=("20230101", "20230131"),
    fields=["url", "html", "date"]
)
```

##### Parameters

- `domains` (list): List of domain names to retrieve data for
- `date_range` (tuple, optional): Tuple of (start_date, end_date) in YYYYMMDD format
- `fields` (list, optional): List of fields to include

##### Returns

- `SparkDataFrame`: Spark DataFrame containing the requested data

#### write_data

Write data to storage.

```python
stats = storage.write_data(
    dataframe=df,
    overwrite=False
)
```

Parameters:
- `dataframe` (DataFrame): Spark DataFrame with data to write
- `overwrite` (bool, optional): Whether to overwrite existing data, defaults to False
- `partition_size_hint` (int, optional): Target size for partitions in MB

Returns:
- `dict`: Dictionary with statistics about the write operation

#### append_data

Append incremental data to storage.

```python
stats = storage.append_data(
    dataframe=df,
    deduplicate=True
)
```

##### Parameters

- `dataframe` (SparkDataFrame): Spark DataFrame containing the data to append
- `deduplicate` (bool, optional): Whether to deduplicate HTML content, defaults to True

##### Returns

- `dict`: Dictionary with statistics about the append operation

### ParquetStorageBackend

Direct access to the storage backend. This is useful for advanced use cases.

```python
from cc_store.storage import ParquetStorageBackend
```

### write_data

```python
stats = storage.write_data(
    dataframe=df,
    overwrite=False
)
```

Parameters:
- `dataframe` (DataFrame): Spark DataFrame with data to write
- `overwrite` (bool, optional): Whether to overwrite existing data, defaults to False
- `partition_size_hint` (int, optional): Target size for partitions in MB

Returns:
- `dict`: Dictionary with statistics about the write operation

## Document Schema

CC-Store uses a standardized schema for Common Crawl documents:

```python
schema = StructType([
    StructField("raw_warc_path", StringType(), False),
    StructField("track_id", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("url", StringType(), False),
    StructField("status", IntegerType(), False),
    StructField("response_header", StringType(), True),  # JSON string
    StructField("date", LongType(), False),
    StructField("content_length", IntegerType(), False),
    StructField("content_charset", StringType(), True),
    StructField("html", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("remark", StringType(), True)  # JSON string
])
```

## Utility Functions

CC-Store provides utility functions for working with storage.

```python
from cc_store.utils import estimate_dataframe_size, optimize_partitions
```

### estimate_dataframe_size

Estimate the size of a Spark DataFrame in bytes.

```python
size_bytes = estimate_dataframe_size(
    df=dataframe,
    sample_ratio=0.1,
    min_sample_rows=1000
)
```

#### Parameters

- `df` (SparkDataFrame): Spark DataFrame
- `sample_ratio` (float, optional): Ratio of rows to sample, defaults to 0.1
- `min_sample_rows` (int, optional): Minimum number of rows to sample, defaults to 1000

#### Returns

- `int`: Estimated size in bytes

### optimize_partitions

Optimize the number of partitions in a DataFrame.

```python
optimized_df = optimize_partitions(
    df=dataframe,
    target_partition_size_mb=128,
    min_partitions=1
)
```

#### Parameters

- `df` (SparkDataFrame): Spark DataFrame
- `target_partition_size_mb` (int, optional): Target partition size in MB, defaults to 128
- `min_partitions` (int, optional): Minimum number of partitions, defaults to 1

#### Returns

- `SparkDataFrame`: DataFrame with optimized partitions

## Examples

### Reading Data

```python
from cc_store.api import CCStore
from pyspark.sql.functions import col

# Initialize store
store = CCStore(
    storage_path="s3://your-bucket/cc-data",
    metadata_backend="hbase",
    metadata_connection="hbase-host:2181"
)

# Read data for a specific domain
df = store.get_data_by_domain(
    domain="example.com",
    date_range=("20230101", "20230131")
)

# Filter and process
filtered_df = df.filter(col("status") == 200)
    .select("url", "html", "date")
    .limit(1000)

# Print results
filtered_df.show()

# Read data for multiple domains
multi_domain_df = store.get_data_by_domains(
    domains=["example.com", "example.org"],
    date_range=("20230101", "20230131"),
    fields=["domain", "url", "status"]
)

# Count by domain
domain_counts = multi_domain_df.groupBy("domain").count()
domain_counts.show()
```

### Writing Data

```python
from pyspark.sql import SparkSession
import datetime

# Initialize Spark
spark = SparkSession.builder.appName("WriteCC").getOrCreate()

# Create sample data
data = [
    {
        "raw_warc_path": "s3://cc-raw/path/to/warc.gz",
        "track_id": "12345",
        "domain": "example.com",
        "url": "http://example.com/page1",
        "status": 200,
        "response_header": '{"Content-Type": "text/html"}',
        "date": int(datetime.datetime.now().timestamp()),
        "content_length": 5000,
        "content_charset": "utf-8",
        "html": "<html><body>Example page 1</body></html>",
        "remark": '{}'
    },
    # ... more records
]

# Create DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
schema = StructType([
    StructField("raw_warc_path", StringType(), False),
    StructField("track_id", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("url", StringType(), False),
    StructField("status", IntegerType(), False),
    StructField("response_header", StringType(), True),
    StructField("date", LongType(), False),
    StructField("content_length", IntegerType(), False),
    StructField("content_charset", StringType(), True),
    StructField("html", StringType(), True),
    StructField("remark", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# Write data
from cc_store.api import CCStore

store = CCStore(
    storage_path="s3://your-bucket/cc-data",
    metadata_backend="hbase",
    metadata_connection="hbase-host:2181"
)

stats = store.write_data(df, deduplication=True)
print(f"Write stats: {stats}")
```

### Working with Large Files

```python
from cc_store.utils import estimate_dataframe_size, optimize_partitions

# Estimate size of DataFrame
size_bytes = estimate_dataframe_size(df)
size_gb = size_bytes / (1024**3)
print(f"Estimated size: {size_gb:.2f} GB")

# Optimize partitions
if size_gb > 10:
    # Use smaller partitions for very large data
    optimized_df = optimize_partitions(df, target_partition_size_mb=64)
else:
    # Use default partition size
    optimized_df = optimize_partitions(df)

# Write optimized data
stats = store.write_data(optimized_df, deduplication=True)
```

### Direct Access to Storage Backend

```python
from cc_store.storage import ParquetStorageBackend
from cc_store.storage import HBaseMetadataManager

# Initialize components
metadata_manager = HBaseMetadataManager(
    connection_string="hbase-host:2181",
    spark=spark
)

# Create storage backend
storage = ParquetStorageBackend(
    base_path="s3://your-bucket/cc-data/data",
    spark=spark,
    max_file_size_gb=2.0,
    metadata_manager=metadata_manager
)

# Use storage backend directly
df = storage.read_by_domain("example.com")
stats = storage.write_data(new_data)
``` 