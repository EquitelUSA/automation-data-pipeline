"""
automation-data-pipeline

A small production-style Python pipeline demonstrating:

- JSON and API ingestion
- Data validation
- Duplicate detection
- SQLite persistence
- Logging and error handling
- Automated summary reporting

Public portfolio demonstration project by Equitel USA.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen


DB_PATH = Path("pipeline.db")
LOG_PATH = Path("pipeline.log")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def load_json(source: str) -> list[dict[str, Any]]:
    """
    Load JSON records from either a local file or an HTTP/HTTPS endpoint.
    """

    logger.info("Loading data from %s", source)

    if source.startswith(("http://", "https://")):
        with urlopen(source, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    else:
        with open(source, "r", encoding="utf-8") as file:
            payload = json.load(file)

    if not isinstance(payload, list):
        raise ValueError("Input JSON must contain a list of records.")

    logger.info("Loaded %d raw records", len(payload))
    return payload


def validate_record(record: dict[str, Any]) -> tuple[bool, str]:
    """
    Validate a business-data record before database insertion.
    """

    required_fields = {
        "record_id",
        "source",
        "category",
        "value",
        "timestamp",
    }

    missing = required_fields - record.keys()

    if missing:
        return False, f"Missing fields: {', '.join(sorted(missing))}"

    if not isinstance(record["record_id"], str) or not record["record_id"].strip():
        return False, "record_id must be a non-empty string"

    if not isinstance(record["source"], str) or not record["source"].strip():
        return False, "source must be a non-empty string"

    if not isinstance(record["category"], str) or not record["category"].strip():
        return False, "category must be a non-empty string"

    if not isinstance(record["value"], (int, float)):
        return False, "value must be numeric"

    try:
        datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False, "timestamp must be ISO-8601 formatted"

    return True, "valid"


def initialize_database(connection: sqlite3.Connection) -> None:
    """
    Create the destination table if it does not already exist.
    """

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS business_records (
            record_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            category TEXT NOT NULL,
            value REAL NOT NULL,
            timestamp TEXT NOT NULL,
            metadata TEXT,
            inserted_at TEXT NOT NULL
        )
        """
    )

    connection.commit()


def insert_records(
    connection: sqlite3.Connection,
    records: list[dict[str, Any]],
) -> dict[str, int]:
    """
    Validate, deduplicate, and persist records.
    """

    stats = {
        "received": len(records),
        "inserted": 0,
        "duplicates": 0,
        "invalid": 0,
    }

    for record in records:
        valid, reason = validate_record(record)

        if not valid:
            stats["invalid"] += 1
            logger.warning(
                "Rejected record %s: %s",
                record.get("record_id", "<unknown>"),
                reason,
            )
            continue

        try:
            connection.execute(
                """
                INSERT INTO business_records (
                    record_id,
                    source,
                    category,
                    value,
                    timestamp,
                    metadata,
                    inserted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["record_id"],
                    record["source"],
                    record["category"],
                    float(record["value"]),
                    record["timestamp"],
                    json.dumps(record.get("metadata", {})),
                    datetime.utcnow().isoformat(),
                ),
            )

            stats["inserted"] += 1

        except sqlite3.IntegrityError:
            stats["duplicates"] += 1
            logger.info(
                "Duplicate skipped: %s",
                record["record_id"],
            )

    connection.commit()
    return stats


def generate_report(connection: sqlite3.Connection) -> dict[str, Any]:
    """
    Generate summary metrics from validated database records.
    """

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(value), 0),
            COALESCE(AVG(value), 0)
        FROM business_records
        """
    )

    count, total_value, average_value = cursor.fetchone()

    cursor.execute(
        """
        SELECT
            category,
            COUNT(*) AS record_count,
            ROUND(SUM(value), 2) AS total_value
        FROM business_records
        GROUP BY category
        ORDER BY total_value DESC
        """
    )

    categories = [
        {
            "category": row[0],
            "record_count": row[1],
            "total_value": row[2],
        }
        for row in cursor.fetchall()
    ]

    return {
        "total_records": count,
        "total_value": round(total_value, 2),
        "average_value": round(average_value, 2),
        "categories": categories,
    }


def run_pipeline(source: str) -> None:
    """
    Execute the complete ingestion and reporting workflow.
    """

    try:
        records = load_json(source)

        with sqlite3.connect(DB_PATH) as connection:
            initialize_database(connection)

            stats = insert_records(connection, records)
            report = generate_report(connection)

        logger.info("Pipeline completed successfully.")

        print("\nINGESTION RESULTS")
        print("=" * 50)
        print(json.dumps(stats, indent=2))

        print("\nBUSINESS SUMMARY")
        print("=" * 50)
        print(json.dumps(report, indent=2))

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate and analyze JSON business data."
    )

    parser.add_argument(
        "source",
        help="Path to a JSON file or an HTTP/HTTPS JSON endpoint.",
    )

    args = parser.parse_args()
    run_pipeline(args.source)


if __name__ == "__main__":
    main()
