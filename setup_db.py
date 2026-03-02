from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

DB_URL = os.getenv("POSTGRES_CONN_STRING", None)

if not DB_URL:
    raise Exception("DB URL not found in environment variables")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS repositories (
    node_id VARCHAR(128) PRIMARY KEY,
    repo_name TEXT NOT NULL,
    star_count INTEGER NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""


def setup():
    conn = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    conn.close()
    print("Database schema initialized.")


if __name__ == "__main__":
    setup()
