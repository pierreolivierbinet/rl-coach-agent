"""
migrate_add_playlist_id.py
--------------------------
One-off migration: adds the playlist_id column to the live player_stats table.
Safe to run multiple times (uses IF NOT EXISTS via psycopg2 logic).
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME", "rl_coach_db"),
    user=os.getenv("DB_USER", "coach_admin"),
    password=os.getenv("DB_PASSWORD", "development_password"),
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
)
conn.autocommit = True

try:
    with conn.cursor() as cur:
        # Check if column already exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'player_stats' AND column_name = 'playlist_id';
        """)
        if cur.fetchone():
            print("Column 'playlist_id' already exists — nothing to do.")
        else:
            cur.execute("ALTER TABLE player_stats ADD COLUMN playlist_id VARCHAR(50);")
            print("Column 'playlist_id' added successfully.")
finally:
    conn.close()
