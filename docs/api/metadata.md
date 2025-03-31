# Metadata API

This document describes the metadata management features of CC-Store.

## Overview

CC-Store includes a comprehensive metadata management system that tracks information about stored data at multiple levels:

1. **Domain Metadata**: Aggregate information about domains
2. **File Metadata**: Details about individual files
3. **Date Index**: Time-based organization of data

The metadata system enables efficient querying and data management by providing fast access to information about where and how data is stored.

## Metadata Structure

### Domain Metadata

Domain metadata provides high-level information about a domain:

```json
{
  "domain_id": "example.com",
  "bucket_id": "042",
  "total_files": 15,
  "total_records": 25000,
  "total_size_bytes": 150000000,
  "min_date": "20230101",
  "max_date": "20230630",
  "date_count": 6,
  "last_updated": "2023-07-01T12:34:56",
  "stats": {
    "status_counts": {"200": 24500, "404": 500}
  }
}
```

### File Metadata

File metadata contains details about each stored file:

```json
{
  "domain_id": "example.com",
  "date": "20230101",
  "part_id": 0,
  "file_path": "s3://bucket/data/domain_bucket=042/domain=example.com/data_20230101.parquet",
  "file_size_bytes": 10485760,
  "records_count": 5000,
  "min_timestamp": "2023-01-01T00:00:12",
  "max_timestamp": "2023-01-01T23:59:45",
  "created_at": "2023-01-02T12:34:56",
  "checksum": "abcdef1234567890",
  "file_format_version": "1.0",
  "compression": "snappy",
  "statistics": {
    "column_stats": {
      "status": {"min": 200, "max": 500}
    }
  }
}
```

### Date Index

The date index provides time-based organization of data:

```json
{
  "date": "20230101",
  "domain_id": "example.com",
  "files_count": 3,
  "total_size_bytes": 30000000,
  "records_count": 15000
}
```

## API Reference

### MetadataManager

The core interface for metadata management.

```python
from cc_store.core import MetadataManager
```

#### get_domain_metadata

Get metadata for a specific domain.

```python
metadata = manager.get_domain_metadata("example.com")
```

##### Parameters

- `domain` (str): Domain name to retrieve metadata for

##### Returns

- `dict`: Dictionary containing domain metadata

#### list_domains

List domains in the store.

```python
domains = manager.list_domains(
    prefix="example",
    limit=100,
    offset=0
)
```

##### Parameters

- `prefix` (str, optional): Domain name prefix filter
- `limit` (int, optional): Maximum number of domains to return, defaults to 100
- `offset` (int, optional): Offset to start from for pagination, defaults to 0

##### Returns

- `list`: List of domain names

#### get_domain_files

Get file metadata for a specific domain.

```python
files = manager.get_domain_files(
    domain="example.com",
    date_range=("20230101", "20230131")
)
```

##### Parameters

- `domain` (str): Domain name to retrieve file metadata for
- `date_range` (tuple, optional): Tuple of (start_date, end_date) in YYYYMMDD format

##### Returns

- `list`: List of dictionaries containing file metadata

#### update_metadata_after_write

Update metadata after writing files.

```python
manager.update_metadata_after_write(
    domain="example.com",
    date="20230101",
    files_info=[file_metadata1, file_metadata2]
)
```

##### Parameters

- `domain` (str): Domain name
- `date` (str): Date in YYYYMMDD format
- `files_info` (list): List of dictionaries containing file metadata

### HBaseMetadataManager

Implementation of MetadataManager using HBase as the backend.

```python
from cc_store.storage import HBaseMetadataManager
```

#### Initialization

```python
metadata_manager = HBaseMetadataManager(
    connection_string="hbase-host:2181",
    domain_table="domain_metadata",
    file_table="file_metadata",
    date_index_table="date_index",
    spark=spark
)
```

##### Parameters

- `connection_string` (str): HBase connection string
- `domain_table` (str, optional): Table name for domain metadata, defaults to 'domain_metadata'
- `file_table` (str, optional): Table name for file metadata, defaults to 'file_metadata'
- `date_index_table` (str, optional): Table name for date index, defaults to 'date_index'
- `spark` (SparkSession, optional): SparkSession instance

### ParquetMetadataManager

Implementation of MetadataManager using Parquet files as the backend.

```python
from cc_store.storage import ParquetMetadataManager
```

#### Initialization

```python
metadata_manager = ParquetMetadataManager(
    base_path="s3://your-bucket/cc-data/metadata",
    spark=spark
)
```

##### Parameters

- `base_path` (str): Base path for storing metadata
- `spark` (SparkSession, optional): SparkSession instance

## Data Model Classes

CC-Store includes data model classes for working with metadata.

```python
from cc_store.core.models import DomainMetadata, FileMetadata
```

### DomainMetadata

Class representing domain metadata.

```python
metadata = DomainMetadata(
    domain_id="example.com",
    bucket_id="042",
    total_files=15,
    total_records=25000,
    total_size_bytes=150000000,
    min_date="20230101",
    max_date="20230630",
    date_count=6,
    last_updated=datetime.datetime.now(),
    stats={}
)

# Convert to dictionary
metadata_dict = metadata.to_dict()

# Create from dictionary
metadata2 = DomainMetadata.from_dict(metadata_dict)
```

### FileMetadata

Class representing file metadata.

```python
file_metadata = FileMetadata(
    domain_id="example.com",
    date="20230101",
    part_id=0,
    file_path="s3://bucket/path/to/file.parquet",
    file_size_bytes=10485760,
    records_count=5000,
    min_timestamp=datetime.datetime.now() - datetime.timedelta(days=1),
    max_timestamp=datetime.datetime.now(),
    created_at=datetime.datetime.now(),
    checksum="abcdef1234567890",
    file_format_version="1.0",
    compression="snappy",
    statistics={}
)

# Convert to dictionary
file_dict = file_metadata.to_dict()

# Create from dictionary
file_metadata2 = FileMetadata.from_dict(file_dict)
```

## Examples

### Querying Metadata

```python
from cc_store.api import CCStore

# Initialize store
store = CCStore(
    storage_path="s3://your-bucket/cc-data",
    metadata_backend="hbase",
    metadata_connection="hbase-host:2181"
)

# Get metadata for a domain
metadata = store.get_domain_metadata("example.com")

# Print domain statistics
print(f"Domain: {metadata['domain_id']}")
print(f"Total records: {metadata['total_records']}")
print(f"Date range: {metadata['min_date']} to {metadata['max_date']}")

# List all domains
domains = store.list_domains(limit=1000)
print(f"Found {len(domains)} domains")

# Get most recent domains
from cc_store.storage import HBaseMetadataManager

metadata_manager = HBaseMetadataManager(
    connection_string="hbase-host:2181",
    spark=spark
)

# Get file metadata for domain
files = metadata_manager.get_domain_files(
    domain="example.com",
    date_range=("20230601", "20230630")
)

# Print file details
for file_info in files:
    print(f"File: {file_info['file_path']}")
    print(f"  Size: {file_info['file_size_bytes'] / (1024*1024):.2f} MB")
    print(f"  Records: {file_info['records_count']}")
```

### Advanced Metadata Query

```python
# Use SparkSQL to query metadata
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MetadataQuery").getOrCreate()

# Load domain metadata into DataFrame
from cc_store.storage import ParquetMetadataManager

metadata_manager = ParquetMetadataManager(
    base_path="s3://your-bucket/cc-data/metadata",
    spark=spark
)

# Register as temporary view
domain_metadata_df = spark.read.parquet(metadata_manager.domain_path)
domain_metadata_df.createOrReplaceTempView("domain_metadata")

# Query largest domains
largest_domains = spark.sql("""
SELECT domain_id, total_records, total_size_bytes
FROM domain_metadata
ORDER BY total_records DESC
LIMIT 10
""")

largest_domains.show()

# Query domains with most dates
date_coverage = spark.sql("""
SELECT domain_id, date_count, 
       min_date, max_date,
       datediff(to_date(max_date, 'yyyyMMdd'), to_date(min_date, 'yyyyMMdd')) as date_span
FROM domain_metadata
ORDER BY date_count DESC
LIMIT 10
""")

date_coverage.show()
``` 