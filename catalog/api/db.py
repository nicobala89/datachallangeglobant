import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db():
    """Get database connection"""
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
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
