"""
Spark utility functions.
"""

from typing import Dict, Optional, Union
import math

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, ArrayType, MapType,
    StringType, IntegerType, LongType, DoubleType, BooleanType
)


def estimate_dataframe_size(
    df: SparkDataFrame,
    sample_ratio: float = 0.1,
    min_sample_rows: int = 1000
) -> int:
    """
    Estimate the size of a Spark DataFrame in bytes.
    
    Args:
        df: Spark DataFrame
        sample_ratio: Ratio of rows to sample
        min_sample_rows: Minimum number of rows to sample
        
    Returns:
        Estimated size in bytes
    """
    schema = df.schema
    
    # Get row count
    row_count = df.count()
    
    # Determine sample size
    sample_size = max(min_sample_rows, int(row_count * sample_ratio))
    sample_size = min(sample_size, row_count)  # Don't sample more than we have
    
    if sample_size == 0:
        return 0
    
    # Take a sample
    sample_df = df.limit(sample_size)
    
    # Calculate average row size
    avg_row_size = _calculate_avg_row_size(sample_df, schema)
    
    # Estimate total size
    total_size = avg_row_size * row_count
    
    return total_size


def _calculate_avg_row_size(df: SparkDataFrame, schema: StructType) -> float:
    """
    Calculate the average row size of a DataFrame.
    
    Args:
        df: Spark DataFrame
        schema: Schema of the DataFrame
        
    Returns:
        Average row size in bytes
    """
    # First, collect the sample data
    sample_data = df.collect()
    
    if not sample_data:
        return 0
    
    total_size = 0
    
    for row in sample_data:
        row_size = _calculate_row_size(row, schema)
        total_size += row_size
    
    return total_size / len(sample_data)


def _calculate_row_size(row: Dict, schema: StructType) -> int:
    """
    Calculate the size of a row in bytes.
    
    Args:
        row: Row data
        schema: Schema of the row
        
    Returns:
        Size in bytes
    """
    total_size = 0
    
    for field in schema.fields:
        field_name = field.name
        field_type = field.dataType
        field_value = row[field_name] if field_name in row else None
        
        if field_value is None:
            # Null values still take some space for the field name and null indicator
            total_size += len(field_name) + 1
            continue
        
        # Calculate size based on field type
        if isinstance(field_type, StringType):
            total_size += len(str(field_value))
        elif isinstance(field_type, IntegerType):
            total_size += 4
        elif isinstance(field_type, LongType):
            total_size += 8
        elif isinstance(field_type, DoubleType):
            total_size += 8
        elif isinstance(field_type, BooleanType):
            total_size += 1
        elif isinstance(field_type, ArrayType):
            # For arrays, calculate size of each element
            if field_value:
                for elem in field_value:
                    elem_size = _calculate_element_size(elem, field_type.elementType)
                    total_size += elem_size
        elif isinstance(field_type, MapType):
            # For maps, calculate size of keys and values
            if field_value:
                for key, value in field_value.items():
                    key_size = _calculate_element_size(key, field_type.keyType)
                    value_size = _calculate_element_size(value, field_type.valueType)
                    total_size += key_size + value_size
        elif isinstance(field_type, StructType):
            # For structs, recursively calculate size
            if field_value:
                total_size += _calculate_row_size(field_value, field_type)
        else:
            # Default to string representation for other types
            total_size += len(str(field_value))
    
    return total_size


def _calculate_element_size(value, data_type) -> int:
    """
    Calculate the size of a single element.
    
    Args:
        value: Element value
        data_type: Data type of the element
        
    Returns:
        Size in bytes
    """
    if value is None:
        return 1  # Null indicator
    
    if isinstance(data_type, StringType):
        return len(str(value))
    elif isinstance(data_type, IntegerType):
        return 4
    elif isinstance(data_type, LongType):
        return 8
    elif isinstance(data_type, DoubleType):
        return 8
    elif isinstance(data_type, BooleanType):
        return 1
    else:
        # Default to string representation for other types
        return len(str(value))


def optimize_partitions(
    df: SparkDataFrame,
    target_partition_size_mb: int = 128,
    min_partitions: int = 1
) -> SparkDataFrame:
    """
    Optimize the number of partitions in a DataFrame.
    
    Args:
        df: Spark DataFrame
        target_partition_size_mb: Target partition size in MB
        min_partitions: Minimum number of partitions
        
    Returns:
        DataFrame with optimized partitions
    """
    # Estimate the DataFrame size
    estimated_size_bytes = estimate_dataframe_size(df)
    estimated_size_mb = estimated_size_bytes / (1024 * 1024)
    
    # Calculate the optimal number of partitions
    target_partitions = max(
        min_partitions,
        math.ceil(estimated_size_mb / target_partition_size_mb)
    )
    
    # Get the current number of partitions
    current_partitions = df.rdd.getNumPartitions()
    
    # Only repartition if needed
    if current_partitions != target_partitions:
        return df.repartition(target_partitions)
    else:
        return df 