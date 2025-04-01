# CC-Store: Efficient Domain-Centric Web Data Storage

CC-Store is a domain-centric storage system designed for efficiently storing and accessing web data from Common Crawl or similar web archives. It offers optimized data management with domain-level partitioning for faster queries and reduced storage overhead.

## Features

- **Domain-Centric Storage**: Organizes data by domains for efficient access patterns
- **Optimized Storage Format**: Uses Parquet files with compression for space efficiency
- **Flexible Query API**: Filter by domain, date range, and other criteria
- **Scalability**: Built on Apache Spark for distributed processing
- **Metadata Management**: Track statistics and availability of domain data
- **Multiple Metadata Backends**: Support for file system, Redis, and RocksDB

## Project Structure

```
cc-store/
├── cc_store/            # Main package
│   ├── core/            # Core abstractions and interfaces
│   ├── storage/         # Storage implementations
│   ├── api/             # Public API interfaces
│   └── utils/           # Utility functions
├── docs/                # Documentation
│   └── api/             # API documentation
└── tests/               # Test suite
```

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/cc-store.git
cd cc-store

# Install dependencies
pip install -r requirements.txt

# For Redis metadata backend
pip install redis

# For RocksDB metadata backend
pip install python-rocksdb
```

## Quick Start

```python
from cc_store.core import CCStore
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("CCStore").getOrCreate()

# Initialize the store
store = CCStore(storage_path="/path/to/data", spark=spark)

# Write new data
store.write_documents(new_data_df)

# Read domain data
domain_df = store.read_domain("example.com")
domain_df.show()
```

## Metadata Backend Options

CC-Store supports multiple metadata backend options to suit different deployment scenarios:

### 1. File System Backend (Default)

```python
# Use file system metadata backend (default)
cc_store = CCStore(
    storage_path="/path/to/storage",
    metadata_backend="file"
)
```

The file system backend stores metadata as JSON files alongside the data. This is suitable for:
- Local development
- Simple deployments
- Environments where no database infrastructure is available

### 2. Redis Backend

```python
# Use Redis metadata backend
cc_store = CCStore(
    storage_path="/path/to/storage",
    metadata_backend="redis",
    metadata_config={
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": "optional-password"
    }
)
```

The Redis backend offers:
- Fast in-memory metadata operations
- Improved performance for metadata-heavy workloads
- Better scalability with millions of domains
- Easy monitoring and management

### 3. RocksDB Backend

```python
# Use RocksDB metadata backend
cc_store = CCStore(
    storage_path="/path/to/storage",
    metadata_backend="rocksdb",
    metadata_config={
        "db_path": "/path/to/rocksdb",
        "max_open_files": 300
    }
)
```

The RocksDB backend provides:
- Persistent key-value storage
- High performance for large metadata sets
- Low overhead compared to full database systems
- Good for embedded or single-machine deployments

## Examples

Check the `examples/` directory for more detailed usage examples:

- `examples/basic_usage.py`: Basic operations with CC-Store
- `examples/domain_analysis.py`: Analyze domain structure and statistics
- `examples/metadata_example.py`: Demonstrates using different metadata backends

## Documentation

Full documentation is available in the [docs](docs/) directory.

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file. 