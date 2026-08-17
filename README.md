# Automation Data Pipeline

A production-style Python demonstration project for ingesting, validating, storing, and reporting on structured business data.

## What It Demonstrates

- Python automation
- JSON and API data ingestion
- Data validation and quality control
- Duplicate detection
- SQL database operations
- Logging and exception handling
- Automated business reporting
- Modular workflow design

## Workflow

JSON / API Source  
↓  
Schema & Data Validation  
↓  
Duplicate Detection  
↓  
SQLite Persistence  
↓  
SQL Aggregation  
↓  
Automated Summary Report

## Example Validation

The included sample dataset intentionally contains:

- 4 valid records
- 1 duplicate record
- 1 record with a missing required field
- 1 record with an invalid timestamp

The pipeline detects and handles each condition rather than blindly accepting incoming data.

## Run It

Requires Python 3.

```bash
python pipeline.py sample_data.json
