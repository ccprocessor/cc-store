# CC-Store

CC-Store (Common Crawl Store) is a high-performance storage system designed for efficiently managing and querying billions of Common Crawl web page records. The system is optimized for domain-level access patterns.

## Features

- Domain-centric storage organization for efficient querying
- Spark integration for scalable data processing
- Support for incremental data updates
- Advanced compression and optimization for space efficiency
- High-performance query capabilities

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
# Install from source
git clone https://github.com/username/cc-store.git
cd cc-store
pip install -e .
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

## Documentation

Full documentation is available in the [docs](docs/) directory.

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file. 