import os
import psycopg2
from dotenv import load_dotenv

def test_connection():
    load_dotenv()
    
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Database connection successful!")
        conn.close()
    except Exception as e:
        print(f"Database connection failed: {e}")

if __name__ == "__main__":
    test_connection()
