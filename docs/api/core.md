# Core API

This document describes the core API of CC-Store.

## CCStore

The main class for interacting with CC-Store.

```python
from cc_store.core import CCStore
```

### Initialization

```python
store = CCStore(
    storage_path="/path/to/data",
    spark=spark,
    target_part_size_bytes=128*1024*1024,  # 128MB
    min_records_per_part=1000
)
```

Parameters:
- `storage_path` (str): Base path for storing data
- `spark` (SparkSession, optional): SparkSession instance to use
- `target_part_size_bytes` (int, optional): Target size for each part file in bytes, defaults to 128MB
- `min_records_per_part` (int, optional): Minimum number of records per part file, defaults to 1000

### write_documents

Write documents to storage.

```python
file_metadata = store.write_documents(df)
```

Parameters:
- `df` (DataFrame): DataFrame with documents to write. Must have columns:
  - `domain`: Domain name
  - `url`: URL of the document
  - `date`: Date string in format "YYYYMMDD"
  - `html`: HTML content (optional)

Returns:
- `list`: List of FileMetadata objects for the written files

### read_domain

Read documents for a domain.

```python
df = store.read_domain(
    domain="example.com",
    start_date="20230101",
    end_date="20230131",
    filters=[df.status == 200],
    with_html=True,
    limit=1000
)
```

Parameters:
- `domain` (str): Domain name
- `start_date` (str, optional): Start date in YYYYMMDD format
- `end_date` (str, optional): End date in YYYYMMDD format
- `filters` (list, optional): List of pyspark SQL filter conditions
- `with_html` (bool, optional): Whether to include HTML content in the result, defaults to True
- `limit` (int, optional): Maximum number of records to return

Returns:
- `DataFrame`: DataFrame with documents

### get_domains

Get a list of all domains in the store.

```python
domains = store.get_domains()
```

Returns:
- `list`: List of domain names

### get_domain_metadata

Get metadata for a domain.

```python
metadata = store.get_domain_metadata("example.com")
```

Parameters:
- `domain` (str): Domain name

Returns:
- `DomainMetadata`: DomainMetadata object or None if domain not found

### get_domain_statistics

Get statistics for a domain.

```python
stats = store.get_domain_statistics("example.com")
```

Parameters:
- `domain` (str): Domain name

Returns:
- `dict`: Dictionary with domain statistics

### get_dates_for_domain

Get available dates for a domain.

```python
dates = store.get_dates_for_domain("example.com")
```

Parameters:
- `domain` (str): Domain name

Returns:
- `list`: List of date strings in format "YYYYMMDD"

### search_content

Search for keyword in HTML content.

```python
results = store.search_content(
    domain="example.com",
    keyword="example",
    start_date="20230101",
    end_date="20230131"
)
```

Parameters:
- `domain` (str): Domain name
- `keyword` (str): Keyword to search for
- `start_date` (str, optional): Start date in YYYYMMDD format
- `end_date` (str, optional): End date in YYYYMMDD format

Returns:
- `DataFrame`: DataFrame with matching documents

## DomainMetadata

Class representing metadata for a domain.

```python
from cc_store.core.models import DomainMetadata
```

### Attributes

- `domain` (str): Domain name
- `total_records` (int): Total number of records for the domain
- `total_files` (int): Total number of files for the domain
- `total_size_bytes` (int): Total size in bytes of all files for the domain
- `min_date` (str): Earliest date in the store for the domain
- `max_date` (str): Latest date in the store for the domain
- `date_count` (int): Number of distinct dates in the store for the domain
- `stats` (dict): Additional statistics for the domain

## FileMetadata

Class representing metadata for a file.

```python
from cc_store.core.models import FileMetadata
```

### Attributes

- `domain` (str): Domain name
- `date` (str): Date string in format "YYYYMMDD"
- `file_path` (str): Path to the file
- `file_size_bytes` (int): Size of the file in bytes
- `num_records` (int): Number of records in the file
- `created_at` (str): Timestamp when the file was created 