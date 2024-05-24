# Incremental Batch Processing Pipeline with PySpark

This project demonstrates how to build an incremental batch processing pipeline using PySpark. The pipeline reads data from an S3 folder in Parquet format, performs various transformations, and writes the transformed data back to a new S3 location.

## Overview

The incremental batch processing pipeline consists of the following steps:

1. **Reading Data**: Data is read from an S3 folder in Parquet format.
2. **Transformations**: Various transformations are applied to the data, including correcting datetime format, filtering by city, creating new columns for full name and matching gender, and mapping customer ID to state address from another Parquet file.
3. **Partitioning**: The transformed data is partitioned by date and city.
4. **Writing Data**: The transformed DataFrame is written back to a new S3 location in Parquet format partitioned by date and city.
