import os
import sys
import time

import psycopg2
from psycopg2.extras import Json


def seed_database() -> None:
    pghost = os.environ.get("PGHOST", "localhost")
    pgport = os.environ.get("PGPORT", "5432")
    pguser = os.environ.get("PGUSER", "postgres")
    pgpassword = os.environ.get("PGPASSWORD", "postgres")
    pgdatabase = os.environ.get("PGDATABASE", "postgres")

    max_retries = 10
    conn = None
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=pghost,
                port=pgport,
                user=pguser,
                password=pgpassword,
                dbname=pgdatabase,
            )
            break
        except psycopg2.OperationalError as e:
            print(f"Database not ready yet (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2)

    if not conn:
        print("Failed to connect to the database after multiple attempts.")
        sys.exit(1)

    print("Connected to PostgreSQL successfully.")

    with conn.cursor() as cur:
        # Create schema and table
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.bronze_livertox_raw (
                coreason_id TEXT,
                uid TEXT,
                raw_data JSONB,
                extracted_blocks JSONB,
                ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Clear existing data
        cur.execute("TRUNCATE TABLE bronze.bronze_livertox_raw;")

        # Mock Data Scenarios (Complex and Edge Cases)
        mock_records = [
            # Standard case
            (
                "id-001",
                "uid-001",
                {"BookDocument": {"ArticleTitle": "Standard Drug A"}},
                {
                    "hepatotoxicity_summary": "Standard summary.",
                    "likelihood_score": "Likelihood score: A",
                    "mechanism_of_injury": "Standard mechanism",
                },
            ),
            # Modifier case [HD]
            (
                "id-002",
                "uid-002",
                {"BookDocument": {"Book": {"BookTitle": "Drug B [HD]"}}},
                {
                    "hepatotoxicity_summary": "Summary B.",
                    "likelihood_score": "Likelihood score: B[HD]",
                    "mechanism_of_injury": "Mechanism B",
                },
            ),
            # Modifier case *
            (
                "id-003",
                "uid-003",
                {"article": {"front": {"article-meta": {"title-group": {"article-title": "Drug C Asterisk"}}}}},
                {
                    "hepatotoxicity_summary": "Summary C.",
                    "likelihood_score": "Likelihood score: C*",
                    "mechanism_of_injury": "Mechanism C",
                },
            ),
            # Missing fields (Should fallback to Unknown Agent)
            (
                "id-004",
                "uid-004",
                {},  # Missing Title
                {"hepatotoxicity_summary": None, "likelihood_score": None, "mechanism_of_injury": None},
            ),
            # Unexpected whitespace in score
            (
                "id-005",
                "uid-005",
                {"BookDocument": {"ArticleTitle": "Whitespace Drug"}},
                {
                    "hepatotoxicity_summary": "Summary W.",
                    "likelihood_score": "Likelihood score:    E",
                    "mechanism_of_injury": "Mechanism W",
                },
            ),
            # Non-matching score
            (
                "id-006",
                "uid-006",
                {"BookDocument": {"ArticleTitle": "Invalid Score Drug"}},
                {
                    "hepatotoxicity_summary": "Summary Inv.",
                    "likelihood_score": "Likelihood score: Z",
                    "mechanism_of_injury": "Mechanism Inv",
                },
            ),
        ]

        for record in mock_records:
            cur.execute(
                """
                INSERT INTO bronze.bronze_livertox_raw (coreason_id, uid, raw_data, extracted_blocks)
                VALUES (%s, %s, %s, %s);
            """,
                (record[0], record[1], Json(record[2]), Json(record[3])),
            )

        conn.commit()
    conn.close()
    print("Seeded database successfully with mock scenarios.")


if __name__ == "__main__":
    seed_database()
