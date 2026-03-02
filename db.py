from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()


DB_URL = os.getenv("POSTGRES_CONN_STRING", None)


def get_connection():
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise


async def postgres_insert(query, variables=None):
    conn = None
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, variables)
                conn.commit()
                return True
    except Exception as e:
        print(f"Error writing to postgres: {e}")
        return False
    finally:
        if conn:
            conn.close()


async def postgres_insert_many(query, variables=None):
    conn = None
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, variables)
                conn.commit()
                return True
    except Exception as e:
        print(f"Error writing to postgres: {e}")
        return False
    finally:
        if conn:
            conn.close()


async def postgres_fetch_all(query):
    conn = None
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)

                return cur.fetchall()
    except Exception as e:
        print(f"Error reading from postgres: {e}")
        return None
    finally:
        if conn:
            conn.close()
