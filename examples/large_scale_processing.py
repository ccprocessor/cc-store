#!/usr/bin/env python
"""
Large-Scale Processing Examples for CC-Store.

This example demonstrates distributed large-scale processing techniques:
1. Parallel domain processing
2. Time-partitioned calculations
3. Content similarity clustering
"""

import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re

from cc_store.core.cc_store import CCStore


def parallel_domain_processing(cc_store, domains, date, process_fn, with_html=True):
    """
    Process multiple domains in parallel using Spark.
    
    Args:
        cc_store: CCStore instance
        domains: List of domain names to process
        date: Date to process in YYYYMMDD format
        process_fn: Function to apply to each domain's data
        with_html: Whether to include HTML content
        
    Returns:
        DataFrame with combined results from all domains
    """
    print(f"Processing {len(domains)} domains in parallel for date {date}...")
    
    # Create an empty result DataFrame to union with
    result_schema = process_fn.output_schema
    result_df = cc_store.spark.createDataFrame([], result_schema)
    
    # Process each domain in parallel using Spark
    domain_rdd = cc_store.spark.sparkContext.parallelize(domains)
    
    def process_domain(domain):
        print(f"Processing domain: {domain}")
        try:
            # Read domain data
            domain_df = cc_store.read_domain(domain, date, date, with_html=with_html)
            if domain_df.count() > 0:
                # Apply the processing function
                result = process_fn(domain, domain_df)
                return [(domain, result)]
            else:
                print(f"No data found for domain {domain} on {date}")
                return []
        except Exception as e:
            print(f"Error processing domain {domain}: {e}")
            return []
    
    # Process domains and collect results
    results_rdd = domain_rdd.flatMap(process_domain)
    
    # If there are no results, return empty DataFrame
    if results_rdd.isEmpty():
        return result_df
    
    # Convert results to DataFrame
    results = results_rdd.collect()
    domains_processed = []
    
    for domain, result_data in results:
        domains_processed.append(domain)
        # Union with the result DataFrame
        result_df = result_df.union(result_data)
    
    print(f"Processed {len(domains_processed)} domains successfully")
    return result_df


# Define a processing function for document counting
class CountDocumentsProcessor:
    """Counts documents and extracts basic stats for each domain."""
    
    @property
    def output_schema(self):
        """Define the output schema for this processor."""
        return T.StructType([
            T.StructField("domain", T.StringType(), False),
            T.StructField("total_urls", T.IntegerType(), False),
            T.StructField("total_bytes", T.LongType(), False),
            T.StructField("avg_document_size", T.IntegerType(), False),
            T.StructField("content_types", T.ArrayType(T.StringType()), True)
        ])
    
    def __call__(self, domain, df):
        """Process a domain DataFrame to count documents and extract stats."""
        # Extract content types
        df_with_type = df.withColumn(
            "content_type", 
            F.regexp_extract(F.concat_ws(" ", F.col("response_header")), r"Content-Type: ([^;]+)", 1)
        )
        
        content_types = [row[0] for row in df_with_type.select("content_type").distinct().collect()]
        
        # Calculate statistics
        total_urls = df.count()
        total_bytes = df.select(F.sum(F.length("html"))).collect()[0][0] if "html" in df.columns else 0
        avg_size = int(total_bytes / total_urls) if total_urls > 0 else 0
        
        # Create result DataFrame
        result_data = [(domain, total_urls, total_bytes, avg_size, content_types)]
        return df.sparkSession.createDataFrame(result_data, self.output_schema)


def time_partitioned_processing(cc_store, domain, date_range, interval_days=30):
    """
    Process data in time-partitioned chunks to handle large volumes efficiently.
    
    Args:
        cc_store: CCStore instance
        domain: Domain to process
        date_range: List of available dates in YYYYMMDD format
        interval_days: Number of days to process in each partition
        
    Returns:
        Combined DataFrame with results from all time partitions
    """
    print(f"Performing time-partitioned processing for {domain} across {len(date_range)} dates...")
    
    # Sort dates and convert to datetime
    sorted_dates = sorted(date_range)
    datetime_dates = [pd.to_datetime(date, format="%Y%m%d") for date in sorted_dates]
    
    if not datetime_dates:
        print("No dates available for processing")
        return None
    
    # Create time partitions
    start_date = datetime_dates[0]
    end_date = datetime_dates[-1]
    
    print(f"Date range: {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')}")
    
    # Create partition boundaries
    current_date = start_date
    partition_boundaries = []
    
    while current_date <= end_date:
        partition_boundaries.append(current_date)
        current_date += pd.Timedelta(days=interval_days)
    
    if partition_boundaries[-1] < end_date:
        partition_boundaries.append(end_date)
    
    # Create partitions
    partitions = []
    for i in range(len(partition_boundaries) - 1):
        start = partition_boundaries[i]
        end = partition_boundaries[i+1]
        
        # Find dates in this partition
        partition_dates = [
            date for date, dt in zip(sorted_dates, datetime_dates)
            if start <= dt <= end
        ]
        
        if partition_dates:
            partitions.append((
                start.strftime("%Y%m%d"),
                end.strftime("%Y%m%d"),
                partition_dates
            ))
    
    print(f"Created {len(partitions)} time partitions")
    
    # Process each partition and combine results
    results = []
    
    for i, (partition_start, partition_end, partition_dates) in enumerate(partitions):
        print(f"Processing partition {i+1}/{len(partitions)}: {partition_start} to {partition_end}")
        print(f"  Contains {len(partition_dates)} dates")
        
        # Define processing for this partition
        word_count_results = analyze_word_frequency_over_time(
            cc_store, domain, partition_dates
        )
        
        if word_count_results is not None:
            results.append(word_count_results)
    
    # Combine results from all partitions
    if not results:
        return None
    
    combined_results = results[0]
    for result in results[1:]:
        combined_results = combined_results.union(result)
    
    return combined_results


def analyze_word_frequency_over_time(cc_store, domain, date_list):
    """
    Analyze word frequency trends over time.
    
    Args:
        cc_store: CCStore instance
        domain: Domain to analyze
        date_list: List of dates in YYYYMMDD format
        
    Returns:
        DataFrame with word frequency by date
    """
    if not date_list:
        return None
    
    print(f"Analyzing word frequency for {domain} across {len(date_list)} dates...")
    
    # Get data for all dates in this partition
    combined_df = None
    
    for date in date_list:
        df = cc_store.read_domain(domain, date, date, with_html=True)
        
        if df.count() > 0:
            # Extract date component
            df = df.withColumn("extract_date", F.lit(date))
            
            # Union with combined DataFrame
            if combined_df is None:
                combined_df = df
            else:
                combined_df = combined_df.union(df)
    
    if combined_df is None or combined_df.count() == 0:
        print(f"No data found for domain {domain} in the specified dates")
        return None
    
    # Register UDF for text extraction
    @F.udf(returnType=T.StringType())
    def extract_text(html):
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text()
        except:
            return ""
    
    # Extract text from HTML
    text_df = combined_df.withColumn("text", extract_text(F.col("html")))
    
    # Register UDF for word count
    @F.udf(returnType=T.MapType(T.StringType(), T.IntegerType()))
    def count_words(text):
        if not text:
            return {}
        
        # Normalize text
        text = text.lower()
        # Remove punctuation and split into words
        words = re.findall(r'\b\w+\b', text)
        
        # Count word occurrences
        word_counts = {}
        for word in words:
            if len(word) > 3:  # Filter out very short words
                word_counts[word] = word_counts.get(word, 0) + 1
        
        return word_counts
    
    # Count words in each document
    word_counts_df = text_df.withColumn("word_counts", count_words(F.col("text")))
    
    # Explode the word counts map into rows
    exploded_df = word_counts_df.select(
        "extract_date",
        F.explode(F.col("word_counts")).alias("word", "count")
    )
    
    # Group by date and word, and sum counts
    word_trends = exploded_df.groupBy("extract_date", "word").agg(
        F.sum("count").alias("frequency")
    )
    
    # Count total words per date for normalization
    total_words_per_date = exploded_df.groupBy("extract_date").agg(
        F.sum("count").alias("total_words")
    )
    
    # Join with total words to normalize frequencies
    word_trends = word_trends.join(total_words_per_date, "extract_date")
    
    # Calculate normalized frequency (per 10,000 words)
    word_trends = word_trends.withColumn(
        "normalized_frequency",
        (F.col("frequency") / F.col("total_words")) * 10000
    )
    
    return word_trends


def content_similarity_clustering(cc_store, domain, date, num_clusters=5, max_docs=1000):
    """
    Cluster documents by content similarity using TF-IDF and K-means.
    
    Args:
        cc_store: CCStore instance
        domain: Domain to analyze
        date: Date to analyze in YYYYMMDD format
        num_clusters: Number of clusters to create
        max_docs: Maximum number of documents to process
        
    Returns:
        DataFrame with documents and their assigned clusters
    """
    print(f"Clustering documents by content similarity for {domain} on {date}...")
    
    # Read domain data with HTML content
    df = cc_store.read_domain(domain, date, date, with_html=True)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} on {date}")
        return None
    
    # Limit number of documents to process
    if df.count() > max_docs:
        print(f"Limiting to {max_docs} documents")
        df = df.limit(max_docs)
    
    # Extract text from HTML
    @F.udf(returnType=T.StringType())
    def extract_text(html):
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text()
        except:
            return ""
    
    # Extract text from HTML
    text_df = df.withColumn("text", extract_text(F.col("html")))
    
    # Filter out empty documents
    text_df = text_df.filter(F.length("text") > 100)
    
    if text_df.count() == 0:
        print("No valid text content found to cluster")
        return None
    
    # Tokenize text
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    words_df = tokenizer.transform(text_df)
    
    # Calculate term frequency
    hashingTF = HashingTF(inputCol="words", outputCol="tf", numFeatures=10000)
    tf_df = hashingTF.transform(words_df)
    
    # Calculate IDF
    idf = IDF(inputCol="tf", outputCol="features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)
    
    # Cluster documents using K-means
    kmeans = KMeans(k=num_clusters, seed=42, featuresCol="features")
    kmeans_model = kmeans.fit(tfidf_df)
    clustered_df = kmeans_model.transform(tfidf_df)
    
    # Create output DataFrame with cluster information
    result_df = clustered_df.select(
        "url", 
        "prediction",
        F.substring("text", 1, 200).alias("text_preview")
    )
    
    # Count documents per cluster
    cluster_counts = result_df.groupBy("prediction").count().orderBy("prediction")
    print("Cluster sizes:")
    cluster_counts.show()
    
    return result_df


def compute_storage_efficiency(cc_store, domain, date):
    """
    Compute storage efficiency metrics for a domain.
    
    Args:
        cc_store: CCStore instance
        domain: Domain to analyze
        date: Date to analyze in YYYYMMDD format
        
    Returns:
        Dictionary with storage efficiency metrics
    """
    print(f"Computing storage efficiency metrics for {domain} on {date}...")
    
    # Read domain data with HTML content
    df = cc_store.read_domain(domain, date, date, with_html=True)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} on {date}")
        return None
    
    # Calculate raw HTML size
    df = df.withColumn("html_size", F.length(F.col("html")))
    
    # Calculate total size and count
    total_docs = df.count()
    total_html_bytes = df.select(F.sum("html_size")).collect()[0][0]
    
    # Estimate Parquet file size
    storage_path = os.path.join(cc_store.storage_path, "data", domain, date)
    
    import subprocess
    try:
        # Get total size of parquet files
        result = subprocess.run(
            ["du", "-sb", storage_path], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        parquet_size = int(result.stdout.split()[0])
    except:
        # Fallback estimation if command fails
        parquet_size = total_html_bytes * 0.3  # Rough estimate that Parquet is ~30% of raw size
    
    # Calculate compression ratio
    compression_ratio = total_html_bytes / parquet_size if parquet_size > 0 else 0
    
    # Measure read performance
    start_time = time.time()
    _ = cc_store.read_domain(domain, date, date, with_html=False).count()
    metadata_only_time = time.time() - start_time
    
    start_time = time.time()
    _ = cc_store.read_domain(domain, date, date, with_html=True).count()
    full_read_time = time.time() - start_time
    
    # Calculate metrics
    metrics = {
        "domain": domain,
        "date": date,
        "document_count": total_docs,
        "raw_html_size_mb": total_html_bytes / (1024 * 1024),
        "parquet_size_mb": parquet_size / (1024 * 1024),
        "compression_ratio": compression_ratio,
        "average_document_size_kb": (total_html_bytes / total_docs) / 1024 if total_docs > 0 else 0,
        "metadata_only_read_time_sec": metadata_only_time,
        "full_read_time_sec": full_read_time,
        "html_read_overhead_ratio": full_read_time / metadata_only_time if metadata_only_time > 0 else 0
    }
    
    print("\nStorage Efficiency Metrics:")
    for key, value in metrics.items():
        if isinstance(value, (int, float)):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    return metrics


def plot_clustering_results(clustered_df, domain, date):
    """
    Plot clustering results.
    
    Args:
        clustered_df: DataFrame with clustering results
        domain: Domain name
        date: Date analyzed
        
    Returns:
        None (saves plot to file)
    """
    # Convert to pandas for plotting
    pdf = clustered_df.select("prediction").toPandas()
    
    # Count documents per cluster
    cluster_counts = pdf["prediction"].value_counts().sort_index()
    
    # Plot cluster sizes
    plt.figure(figsize=(10, 6))
    bars = plt.bar(cluster_counts.index, cluster_counts.values, color='skyblue')
    
    # Add count labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2.,
            height + 0.1,
            f'{int(height)}',
            ha='center', va='bottom'
        )
    
    plt.title(f'Document Cluster Sizes for {domain} on {date}')
    plt.xlabel('Cluster ID')
    plt.ylabel('Number of Documents')
    plt.xticks(cluster_counts.index)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(f"clusters_{domain}_{date}.png")
    print(f"Clustering results plot saved as clusters_{domain}_{date}.png")


def plot_word_frequencies(word_trends_df, domain, top_n=10):
    """
    Plot word frequency trends over time.
    
    Args:
        word_trends_df: DataFrame with word frequencies by date
        domain: Domain name
        top_n: Number of top words to include
        
    Returns:
        None (saves plot to file)
    """
    # Convert to pandas for analysis and plotting
    pdf = word_trends_df.toPandas()
    
    # Convert date strings to datetime for better plotting
    pdf['extract_date'] = pd.to_datetime(pdf['extract_date'], format='%Y%m%d')
    
    # Find overall top words
    top_words = pdf.groupby('word')['frequency'].sum() \
        .sort_values(ascending=False).head(top_n).index.tolist()
    
    # Filter to top words and pivot data for plotting
    plot_data = pdf[pdf['word'].isin(top_words)] \
        .pivot(index='extract_date', columns='word', values='normalized_frequency')
    
    # Plot word trends
    plt.figure(figsize=(12, 8))
    
    for word in plot_data.columns:
        plt.plot(plot_data.index, plot_data[word], marker='o', label=word)
    
    plt.title(f'Top {top_n} Word Frequency Trends for {domain}')
    plt.xlabel('Date')
    plt.ylabel('Frequency (per 10,000 words)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(f"word_trends_{domain}.png")
    print(f"Word frequency trends plot saved as word_trends_{domain}.png")


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("CC-Store Large-Scale Processing") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Parse command line arguments or use defaults
    storage_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cc_store_example"
    domain = sys.argv[2] if len(sys.argv) > 2 else "example.com"
    date = sys.argv[3] if len(sys.argv) > 3 else "20230101"
    
    # Initialize CCStore
    cc_store = CCStore(
        storage_path=storage_path,
        spark=spark
    )
    
    # Example 1: Parallel Domain Processing
    print("\n=== Example 1: Parallel Domain Processing ===")
    
    # Get list of domains (replace with actual code to get domains)
    domains = cc_store.get_available_domains()[:5]  # Limit to 5 domains for example
    
    if not domains:
        print("No domains found in the store")
        # Use example domains for demonstration
        domains = ["example.com", "example.org", "example.net"]
    
    print(f"Processing domains: {domains}")
    
    # Define the processor
    processor = CountDocumentsProcessor()
    
    # Process domains in parallel
    result_df = parallel_domain_processing(cc_store, domains, date, processor)
    
    if result_df and result_df.count() > 0:
        print("\nDomain Processing Results:")
        result_df.show()
    else:
        print("No results from parallel domain processing")
    
    # Example 2: Content Similarity Clustering
    print("\n=== Example 2: Content Similarity Clustering ===")
    
    # Run clustering on a single domain
    clustered_df = content_similarity_clustering(cc_store, domain, date)
    
    if clustered_df and clustered_df.count() > 0:
        print("\nSample of clustered documents:")
        clustered_df.show(10, truncate=50)
        
        # Plot clustering results
        try:
            plot_clustering_results(clustered_df, domain, date)
        except Exception as e:
            print(f"Could not plot clustering results: {e}")
    else:
        print(f"No clustering results for {domain} on {date}")
    
    # Example 3: Time-Partitioned Processing
    print("\n=== Example 3: Time-Partitioned Processing ===")
    
    # Get dates for domain
    dates = cc_store.get_dates_for_domain(domain)
    
    if len(dates) > 1:
        # Run time-partitioned processing
        word_trends = time_partitioned_processing(cc_store, domain, dates)
        
        if word_trends and word_trends.count() > 0:
            print("\nWord frequency trends sample:")
            word_trends.orderBy(F.col("normalized_frequency").desc()).show(10)
            
            # Plot word frequency trends
            try:
                plot_word_frequencies(word_trends, domain)
            except Exception as e:
                print(f"Could not plot word frequency trends: {e}")
        else:
            print(f"No word frequency trends for {domain}")
    else:
        print(f"Not enough dates available for {domain} to perform time-partitioned processing")
    
    # Example 4: Storage Efficiency Metrics
    print("\n=== Example 4: Storage Efficiency Metrics ===")
    
    # Compute storage efficiency metrics
    metrics = compute_storage_efficiency(cc_store, domain, date)
    
    # Clean up resources
    spark.stop()
    
    print("\nLarge-scale processing examples completed.")


if __name__ == "__main__":
    main()