#!/usr/bin/env python
"""
Content Mining Examples for CC-Store.

This example demonstrates text and content mining techniques:
1. Keyword trend analysis
2. Content similarity detection
3. Entity extraction from HTML content
"""

import os
import datetime
import re
from collections import Counter
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from cc_store.core.cc_store import CCStore


def analyze_keyword_trend(cc_store, domain, keyword, start_date, end_date):
    """
    Analyze the trend of a keyword over time.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        keyword: Keyword to search for
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        DataFrame with keyword trend statistics by date
    """
    print(f"Analyzing trend for keyword '{keyword}' on {domain} from {start_date} to {end_date}...")
    
    # Search for the keyword in HTML content
    search_df = cc_store.search_content(domain, keyword, start_date, end_date)
    
    if search_df.count() == 0:
        print(f"No occurrences of keyword '{keyword}' found in the specified date range")
        return None
    
    # Group by date and count occurrences
    trend = search_df.groupBy("date").agg(
        F.count("*").alias("keyword_occurrences"),
        F.countDistinct("url").alias("unique_pages_with_keyword")
    ).orderBy("date")
    
    # Get total pages by date for context
    total_pages = cc_store.read_domain(
        domain, start_date, end_date, with_html=False
    ).select("date", "url").distinct()
    
    total_by_date = total_pages.groupBy("date").count().withColumnRenamed("count", "total_pages")
    
    # Join to calculate proportions
    trend = trend.join(total_by_date, "date")
    trend = trend.withColumn(
        "occurrence_ratio", 
        F.col("unique_pages_with_keyword") / F.col("total_pages")
    )
    
    return trend


def detect_similar_content(cc_store, domain, date, similarity_threshold=0.8):
    """
    Detect similar content within a domain on a specific date.
    
    This is a simplified example that uses string length as a very basic
    similarity measure. In a real application, you would use more sophisticated
    methods like MinHash, SimHash, or text embeddings.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        date: Date in YYYYMMDD format
        similarity_threshold: Threshold for considering content similar (0-1)
        
    Returns:
        DataFrame with groups of similar URLs
    """
    print(f"Detecting similar content for {domain} on {date}...")
    
    # Read domain data with HTML content
    df = cc_store.read_domain(domain, date, date, with_html=True)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} on {date}")
        return None
    
    # Extract features for similarity comparison
    # For demonstration, we'll use simple metrics like content length
    # In a real application, you would use more sophisticated feature extraction
    
    @F.udf(returnType=T.StructType([
        T.StructField("content_length", T.IntegerType()),
        T.StructField("title_length", T.IntegerType()),
        T.StructField("link_count", T.IntegerType()),
        T.StructField("heading_count", T.IntegerType())
    ]))
    def extract_features(html):
        if not html:
            return (0, 0, 0, 0)
        
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Extract title length
            title = soup.title.string if soup.title else ""
            title_length = len(title) if title else 0
            
            # Count links
            links = soup.find_all("a")
            link_count = len(links)
            
            # Count headings
            headings = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
            heading_count = len(headings)
            
            return (len(html), title_length, link_count, heading_count)
        except:
            return (len(html) if html else 0, 0, 0, 0)
    
    # Apply feature extraction
    feature_df = df.withColumn("features", extract_features(F.col("html")))
    feature_df = feature_df.select(
        "url",
        F.col("features.content_length").alias("content_length"),
        F.col("features.title_length").alias("title_length"),
        F.col("features.link_count").alias("link_count"),
        F.col("features.heading_count").alias("heading_count")
    )
    
    # Collect to perform similarity analysis
    # Note: This approach works for small datasets but wouldn't scale
    # For large datasets, use a proper clustering algorithm in Spark
    features = feature_df.collect()
    
    # Group by similarity
    similar_groups = []
    used_indices = set()
    
    for i in range(len(features)):
        if i in used_indices:
            continue
            
        url_i = features[i]["url"]
        content_len_i = features[i]["content_length"]
        title_len_i = features[i]["title_length"]
        link_count_i = features[i]["link_count"]
        heading_count_i = features[i]["heading_count"]
        
        similar_urls = [url_i]
        
        for j in range(i + 1, len(features)):
            if j in used_indices:
                continue
                
            url_j = features[j]["url"]
            content_len_j = features[j]["content_length"]
            title_len_j = features[j]["title_length"]
            link_count_j = features[j]["link_count"]
            heading_count_j = features[j]["heading_count"]
            
            # Calculate simple similarity score based on feature differences
            # This is a very simplified approach
            content_sim = min(content_len_i, content_len_j) / max(content_len_i, content_len_j) if max(content_len_i, content_len_j) > 0 else 0
            title_sim = min(title_len_i, title_len_j) / max(title_len_i, title_len_j) if max(title_len_i, title_len_j) > 0 else 0
            link_sim = min(link_count_i, link_count_j) / max(link_count_i, link_count_j) if max(link_count_i, link_count_j) > 0 else 0
            heading_sim = min(heading_count_i, heading_count_j) / max(heading_count_i, heading_count_j) if max(heading_count_i, heading_count_j) > 0 else 0
            
            # Average similarity score
            similarity = (content_sim * 0.4 + title_sim * 0.3 + link_sim * 0.15 + heading_sim * 0.15)
            
            if similarity >= similarity_threshold:
                similar_urls.append(url_j)
                used_indices.add(j)
        
        if len(similar_urls) > 1:  # Only consider groups with at least 2 similar items
            similar_groups.append({
                "group_id": i,
                "urls": similar_urls,
                "count": len(similar_urls)
            })
            
        used_indices.add(i)
    
    # Convert to DataFrame
    if similar_groups:
        similar_df = cc_store.spark.createDataFrame(similar_groups)
        return similar_df
    else:
        print("No similar content groups found.")
        return None


def extract_entities(cc_store, domain, date, entity_types=None):
    """
    Extract named entities from HTML content.
    
    This example uses simple regex patterns for demonstration.
    In a real application, you would use NLP libraries like spaCy or NLTK.
    
    Args:
        cc_store: CCStore instance
        domain: Domain name to analyze
        date: Date in YYYYMMDD format
        entity_types: Types of entities to extract (default: all)
        
    Returns:
        DataFrame with extracted entities
    """
    print(f"Extracting entities from {domain} content on {date}...")
    
    # Default entity types
    if entity_types is None:
        entity_types = ["email", "url", "date"]
    
    # Read domain data with HTML content
    df = cc_store.read_domain(domain, date, date, with_html=True)
    
    if df.count() == 0:
        print(f"No data found for domain {domain} on {date}")
        return None
    
    # Define regex patterns for entity extraction
    patterns = {
        "email": r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        "url": r'https?://[^\s<>"]+|www\.[^\s<>"]+',
        "date": r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{2,4}[/-]\d{1,2}[/-]\d{1,2}',
        "phone": r'\+?[0-9]{1,3}[-.\s]?[(]?[0-9]{1,3}[)]?[-.\s]?[0-9]{1,4}[-.\s]?[0-9]{1,4}'
    }
    
    # Filter to only requested entity types
    patterns = {k: v for k, v in patterns.items() if k in entity_types}
    
    # Function to extract text from HTML
    @F.udf(returnType=T.StringType())
    def extract_text(html):
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()
            # Get text
            text = soup.get_text()
            # Break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            # Remove blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            return text
        except:
            return html if isinstance(html, str) else ""
    
    # Extract text from HTML
    text_df = df.withColumn("text", extract_text(F.col("html")))
    
    # Function to extract entities
    def extract_entity_udf(entity_type, pattern):
        @F.udf(returnType=T.ArrayType(T.StringType()))
        def _extract(text):
            if not text:
                return []
            matches = re.findall(pattern, text)
            return list(set(matches))  # Deduplicate
        return _extract
    
    # Apply entity extraction for each pattern
    entities_list = []
    
    for entity_type, pattern in patterns.items():
        # Create UDF for this entity type
        extract_func = extract_entity_udf(entity_type, pattern)
        
        # Apply extraction
        entity_df = text_df.withColumn(f"{entity_type}_entities", extract_func(F.col("text")))
        
        # Explode array to get one row per entity
        entity_df = entity_df.select(
            "url", 
            "date", 
            F.explode_outer(F.col(f"{entity_type}_entities")).alias("entity")
        ).filter(F.col("entity").isNotNull())
        
        # Add entity type column
        entity_df = entity_df.withColumn("entity_type", F.lit(entity_type))
        
        # Collect entities
        if entity_df.count() > 0:
            entities_list.append(entity_df)
    
    # Combine all entity dataframes
    if entities_list:
        all_entities = entities_list[0]
        for df in entities_list[1:]:
            all_entities = all_entities.union(df)
            
        # Group by entity type and count occurrences
        entity_counts = all_entities.groupBy("entity_type", "entity").count().orderBy(
            "entity_type", F.col("count").desc()
        )
        
        return entity_counts
    else:
        print("No entities found.")
        return None


def plot_keyword_trend(trend_df):
    """Plot keyword trend over time."""
    # Convert to pandas for plotting
    trend_pd = trend_df.toPandas()
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot occurrence count
    ax1.plot(trend_pd["date"], trend_pd["keyword_occurrences"], "b-", marker="o")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Keyword Occurrences", color="b")
    ax1.tick_params(axis="y", colors="b")
    
    # Plot occurrence ratio on secondary y-axis
    ax2 = ax1.twinx()
    ax2.plot(trend_pd["date"], trend_pd["occurrence_ratio"], "r-", marker="s")
    ax2.set_ylabel("Occurrence Ratio", color="r")
    ax2.tick_params(axis="y", colors="r")
    
    plt.title("Keyword Trend Analysis")
    plt.tight_layout()
    plt.savefig("keyword_trend.png")
    print("Keyword trend plot saved as keyword_trend.png")


def plot_entity_distribution(entity_df):
    """Plot distribution of extracted entities."""
    # Convert to pandas for plotting
    entity_pd = entity_df.toPandas()
    
    # Group by entity type
    entity_types = entity_pd["entity_type"].unique()
    
    # Create a figure with subplots for each entity type
    fig, axs = plt.subplots(len(entity_types), 1, figsize=(12, 5 * len(entity_types)))
    
    # Handle case where there's only one entity type
    if len(entity_types) == 1:
        axs = [axs]
    
    for i, entity_type in enumerate(entity_types):
        # Filter data for this entity type
        type_data = entity_pd[entity_pd["entity_type"] == entity_type]
        
        # Sort by count and take top 10
        type_data = type_data.sort_values("count", ascending=False).head(10)
        
        # Plot horizontal bar chart
        axs[i].barh(type_data["entity"], type_data["count"], color="g")
        axs[i].set_title(f"Top 10 {entity_type.capitalize()} Entities")
        axs[i].set_xlabel("Count")
        axs[i].set_ylabel(entity_type.capitalize())
        
        # Format entity labels if they're too long
        labels = axs[i].get_yticklabels()
        for label in labels:
            if len(label.get_text()) > 30:
                label.set_text(label.get_text()[:27] + "...")
    
    plt.tight_layout()
    plt.savefig("entity_distribution.png")
    print("Entity distribution plot saved as entity_distribution.png")


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("CC-Store Content Mining") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    # Parse command line arguments or use defaults
    import sys
    storage_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cc_store_example"
    domain = sys.argv[2] if len(sys.argv) > 2 else "example.com"
    date = sys.argv[3] if len(sys.argv) > 3 else "20230101"
    keyword = sys.argv[4] if len(sys.argv) > 4 else "example"
    
    # Initialize CCStore
    cc_store = CCStore(
        storage_path=storage_path,
        spark=spark
    )
    
    # Get domain date range for trend analysis
    metadata = cc_store.get_domain_metadata(domain)
    if metadata:
        start_date = metadata.min_date
        end_date = metadata.max_date
        print(f"Using date range from metadata: {start_date} to {end_date}")
    else:
        start_date = date
        end_date = date
        print(f"No metadata found, using single date: {date}")
    
    # Run keyword trend analysis
    trend_df = analyze_keyword_trend(cc_store, domain, keyword, start_date, end_date)
    if trend_df:
        print("\nKeyword Trend Analysis:")
        trend_df.show()
        plot_keyword_trend(trend_df)
    
    # Run similar content detection
    similar_df = detect_similar_content(cc_store, domain, date)
    if similar_df:
        print("\nSimilar Content Groups:")
        similar_df.show(truncate=False)
    
    # Run entity extraction
    entity_df = extract_entities(cc_store, domain, date)
    if entity_df:
        print("\nExtracted Entities:")
        entity_df.show(truncate=False)
        plot_entity_distribution(entity_df)
    
    print("\nContent mining completed.")


if __name__ == "__main__":
    main() 