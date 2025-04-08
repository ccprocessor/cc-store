# CC-Store API Documentation

This directory contains documentation for the CC-Store API.

## Contents

- [Core API](core.md)
- [Storage API](storage.md)
- [Utilities API](utilities.md)

## Overview

CC-Store provides a simple and flexible API for storing and retrieving Common Crawl data.

Key features:
- Domain-centric storage and retrieval
- Date-based partitioning
- Advanced querying capabilities
- Scalable with Spark

## Quick Start

```python
from cc_store.core import CCStore
import datetime

# Initialize the store
store = CCStore(storage_path="/path/to/data")

# Write new data
import_stats = store.write_data(new_data_df)
print(f"Wrote {import_stats['total_records']} records")

# Read data for a specific domain
domain_df = store.read_domain("example.com")
print(f"Retrieved {domain_df.count()} records")

# Read data for a date range
date_df = store.read_domain(
    "example.com",
    start_date="20230101",
    end_date="20230131"
)
```

### Installation

```bash
# Install from source
git clone https://github.com/username/cc-store.git
cd cc-store
pip install -e .
```

For more details about specific API components, please refer to the respective documentation pages listed in the table of contents. 