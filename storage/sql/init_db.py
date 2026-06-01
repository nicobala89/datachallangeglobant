import os
import sys
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

def init_db():
    user = os.getenv("POSTGRES_USER", "globant")
    password = os.getenv("POSTGRES_PASSWORD", "globant_password")
    db_name = os.getenv("POSTGRES_DB", "globant_analytics")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")

    print(f"Connecting to database {db_name} on {host}:{port} as {user}...")
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read and execute globant_schema.sql
        schema_path = os.path.join(os.path.dirname(__file__), "globant_schema.sql")
        print(f"Reading schema from {schema_path}...")
        with open(schema_path, "r") as f:
            schema_sql = f.read()
            
        print("Executing schema SQL...")
        cursor.execute(schema_sql)
        print("Database schemas and tables initialized successfully!")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error initializing database: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    init_db()
