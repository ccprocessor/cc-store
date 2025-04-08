# Utilities API

This document describes the utility functions and helper classes provided by CC-Store.

## Overview

CC-Store includes various utility functions to simplify common operations when working with Common Crawl data. These utilities fall into several categories:

1. **Hashing Utilities**: Functions for content hashing and similarity detection
2. **Spark Utilities**: Functions for working with Spark DataFrames
3. **Compression Utilities**: Functions for compressing and decompressing content

## Hashing Utilities

```python
from cc_store.utils import compute_content_hash
from cc_store.utils.hash_utils import compute_simhash, hamming_distance, is_similar
```

### compute_content_hash

Compute a hash for the given content.

```python
hash_value = compute_content_hash(
    content=html_string,
    method="murmur3",
    seed=42
)
```

#### Parameters

- `content` (str): Content to hash
- `method` (str, optional): Hash method ('sha256', 'murmur3'), defaults to 'murmur3'
- `seed` (int, optional): Seed for MurmurHash, defaults to 42

#### Returns

- `str`: String hash value

### compute_simhash

Compute a SimHash for fuzzy matching.

```python
simhash_value = compute_simhash(
    content=html_string,
    shingle_size=4,
    seed=42
)
```

#### Parameters

- `content` (str): Content to hash
- `shingle_size` (int, optional): Size of text shingles, defaults to 4
- `seed` (int, optional): Seed for hash function, defaults to 42

#### Returns

- `int`: Integer SimHash value

### hamming_distance

Calculate the Hamming distance between two hashes.

```python
distance = hamming_distance(hash1, hash2)
```

#### Parameters

- `hash1` (int): First hash
- `hash2` (int): Second hash

#### Returns

- `int`: Hamming distance

### is_similar

Check if two hashes are similar based on Hamming distance.

```python
similar = is_similar(
    hash1=simhash_value1,
    hash2=simhash_value2,
    threshold=3
)
```

#### Parameters

- `hash1` (int): First hash
- `hash2` (int): Second hash
- `threshold` (int, optional): Maximum Hamming distance for similarity, defaults to 3

#### Returns

- `bool`: True if the hashes are similar, False otherwise

## Spark Utilities

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

### Content Hashing and Similarity

```python
from cc_store.utils import compute_content_hash
from cc_store.utils.hash_utils import compute_simhash, is_similar

# Original HTML
html1 = "<html><body><h1>Hello World</h1><p>This is example 1</p></body></html>"

# Similar HTML (minor changes)
html2 = "<html><body><h1>Hello World</h1><p>This is example 2</p></body></html>"

# Different HTML
html3 = "<html><body><h1>Welcome</h1><p>This is a completely different page</p></body></html>"

# Compute exact hashes
hash1 = compute_content_hash(html1)
hash2 = compute_content_hash(html2)
hash3 = compute_content_hash(html3)

print(f"Exact hash comparison: {hash1 == hash2}")  # False, not identical

# Compute similarity hashes
simhash1 = compute_simhash(html1)
simhash2 = compute_simhash(html2)
simhash3 = compute_simhash(html3)

# Check similarity
print(f"1 and 2 similar: {is_similar(simhash1, simhash2)}")  # True, similar content
print(f"1 and 3 similar: {is_similar(simhash1, simhash3)}")  # False, different content
```

### DataFrame Size Estimation and Optimization

```python
from pyspark.sql import SparkSession
from cc_store.utils import estimate_dataframe_size, optimize_partitions

# Initialize Spark
spark = SparkSession.builder.appName("DataFrameOpt").getOrCreate()

# Load data
df = spark.read.parquet("s3://your-bucket/cc-data/sample.parquet")

# Estimate size
size_bytes = estimate_dataframe_size(df)
size_mb = size_bytes / (1024 * 1024)
print(f"Estimated DataFrame size: {size_mb:.2f} MB")

# Check current partitions
current_partitions = df.rdd.getNumPartitions()
print(f"Current partitions: {current_partitions}")

# Optimize partitions based on size
optimized_df = optimize_partitions(
    df=df,
    target_partition_size_mb=128,  # 128 MB per partition
    min_partitions=5               # At least 5 partitions
)

# Check new partitions
new_partitions = optimized_df.rdd.getNumPartitions()
print(f"Optimized partitions: {new_partitions}")

# Process optimized DataFrame
result = optimized_df.groupBy("domain").count()
result.show()
```

### Performance Optimization

```python
from cc_store.api import CCStore
from cc_store.utils import optimize_partitions
from pyspark.sql import SparkSession

# Initialize Spark with optimized settings
spark = SparkSession.builder \
    .appName("CCStoreOpt") \
    .config("spark.sql.files.maxPartitionBytes", "128m") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()

# Initialize store
store = CCStore(
    storage_path="s3://your-bucket/cc-data",
    metadata_backend="hbase",
    metadata_connection="hbase-host:2181",
    spark=spark
)

# Read data for multiple domains
df = store.get_data_by_domains(
    domains=["example.com", "example.org", "example.net"],
    date_range=("20230101", "20230131")
)

# Optimize partitions for processing
from cc_store.utils import estimate_dataframe_size, optimize_partitions

# Estimate size
size_bytes = estimate_dataframe_size(df)
size_gb = size_bytes / (1024**3)
print(f"Data size: {size_gb:.2f} GB")

# Optimize partitions based on cluster size
# Assuming 10 executor nodes with 4 cores each = 40 cores
executor_cores = 40
partition_size_mb = 128  # 128 MB per partition

# Calculate ideal partitions (cores * 2 to 4 is recommended)
ideal_partitions = executor_cores * 3
target_partitions = max(ideal_partitions, int((size_gb * 1024) / partition_size_mb))

# Repartition
optimized_df = df.repartition(target_partitions)

# Process data and write results
results = optimized_df.groupBy("domain", "status").count()
results.write.parquet("s3://your-bucket/cc-data/results/domain_status_counts")
``` 