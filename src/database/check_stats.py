import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "rl_coach_db")
DB_USER = os.getenv("DB_USER", "coach_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "development_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def fetch_latest_stats():
    query = """
        SELECT 
            -- Core
            core_goals,
            core_saves,
            core_shooting_pct,
            -- Boost
            boost_avg_amount,
            boost_bpm,
            boost_time_zero_boost,
            -- Movement
            mov_avg_speed,
            mov_percent_supersonic_speed,
            -- Meta
            player_name,
            created_at
        FROM player_stats
        ORDER BY created_at DESC
        LIMIT 1;
    """
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            if result:
                # Map column names to values manually for clarity
                columns = [
                    "core_goals", "core_saves", "core_shooting_pct",
                    "boost_avg_amount", "boost_bpm", "boost_time_zero_boost",
                    "mov_avg_speed", "mov_percent_supersonic_speed",
                    "player_name", "created_at"
                ]
                return dict(zip(columns, result))
            return None
    except Exception as e:
        print(f"Database error: {e}")
        return None
    finally:
        conn.close()

def main():
    stats = fetch_latest_stats()
    
    if not stats:
        print("No stats found in the database. Please run fetch_replays.py first.")
        return

    print("="*50)
    print(f"LATEST REPLAY STATS FOR: {stats['player_name']}")
    print(f"Ingested at: {stats['created_at']}")
    print("="*50)
    
    print("\n--- CORE STATS ---")
    print(f"Goals:                {stats['core_goals']}")
    print(f"Saves:                {stats['core_saves']}")
    print(f"Shooting Percentage:  {stats['core_shooting_pct']:.2f}%" if stats['core_shooting_pct'] else "Shooting Percentage:  N/A")
    
    print("\n--- BOOST STATS ---")
    print(f"Avg Amount:           {stats['boost_avg_amount']:.2f}")
    print(f"Boost Per Minute:     {stats['boost_bpm']:.2f}")
    print(f"Time at Zero Boost:   {stats['boost_time_zero_boost']:.2f}s")
    
    print("\n--- MOVEMENT STATS ---")
    print(f"Avg Speed:            {stats['mov_avg_speed']:.2f}")
    print(f"Pct Supersonic Speed: {stats['mov_percent_supersonic_speed']:.2f}%" if stats['mov_percent_supersonic_speed'] else "Pct Supersonic Speed: N/A")
    
    print("\n" + "="*50)
    
    # Calculate Custom KPI: Boost-to-Speed Efficiency
    bpm = float(stats['boost_bpm']) if stats['boost_bpm'] else 0.0
    avg_speed = float(stats['mov_avg_speed']) if stats['mov_avg_speed'] else 0.0
    
    if avg_speed > 0:
        # A higher ratio of Boost Per Minute relative to Speed means wasting boost.
        # Multiplying by 100 for readability (e.g., 0.25 becomes 25.0)
        efficiency = (bpm / avg_speed) * 100
        print(f"Boost-to-Speed Inefficiency Index: {efficiency:.2f}")
        
        # 365 BPM / 1454 Speed = 0.25 (or 25.0 scaled). Threshold of > 10 scaled means > 0.1 ratio.
        # It was returning 0.25 without scaling, so a check for > 10 wasn't triggering.
        if efficiency > 10:
            print("\nWARNING: High boost consumption detected relative to speed. You might be wasting boost!")
    else:
        print("Could not calculate Boost-to-Speed Efficiency: Avg speed is zero or missing.")

if __name__ == "__main__":
    main()
