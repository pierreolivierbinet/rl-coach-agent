import os
import psycopg2
from dotenv import load_dotenv

def init_db():
    load_dotenv()
    
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found at {schema_path}")
        return

    try:
        # Connect to DB
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read schema
        with open(schema_path, "r") as schema_file:
            sql_script = schema_file.read()
            
        print("Dropping existing table if it exists...")
        cursor.execute("DROP TABLE IF EXISTS player_stats;")
        
        print("Applying schema...")
        cursor.execute(sql_script)
        
        print("Schema applied successfully! The database is ready for ingestion.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Database initialization failed: {e}")

if __name__ == "__main__":
    init_db()
