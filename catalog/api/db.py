import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db():
    host = os.getenv('POSTGRES_HOST', '').strip()
    if not host:
        raise RuntimeError(
            "POSTGRES_HOST is not set. "
            "Set it to the database hostname (e.g. the Railway private host or 'postgres' in docker-compose)."
        )
    conn = psycopg2.connect(
        host=host,
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB', 'globant_analytics'),
        user=os.getenv('POSTGRES_USER', 'globant'),
        password=os.getenv('POSTGRES_PASSWORD', 'globant_password'),
        cursor_factory=RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()
