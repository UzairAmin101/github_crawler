import asyncio
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

from db import get_connection

DB_URL = os.getenv("POSTGRES_CONN_STRING")
EXPORT_FILE_NAME = os.getenv("OUTPUT_FILE_NAME")

if not DB_URL:
    raise Exception("DB URL is missing.")

if not EXPORT_FILE_NAME:
    raise Exception("Export File Name is missing.")


def datetime_handler(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


async def export_all_rows(batch_size=1000):
    all_rows = []
    conn = None

    try:
        conn = get_connection()

        with conn.cursor(
            name="repo_export_cursor", cursor_factory=RealDictCursor
        ) as cur:
            cur.execute(
                "SELECT node_id, repo_name, star_count, updated_at FROM repositories;"
            )

            while True:
                rows = cur.fetchmany(batch_size)

                if not rows:
                    break

                all_rows.extend(rows)

        with open(EXPORT_FILE_NAME, "w") as f:
            json.dump(all_rows, f, indent=4, default=datetime_handler)

        print("Export successful!")
        return True

    except Exception as e:
        print(f"Error exporting data: {e}")
        return False
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    asyncio.run(export_all_rows())
