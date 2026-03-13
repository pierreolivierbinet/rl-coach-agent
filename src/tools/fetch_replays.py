import os
import requests
import psycopg2
import uuid
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BALLCHASING_API_KEY = os.getenv("BALLCHASING_API_KEY")
PLAYER_NAME = os.getenv("PLAYER_NAME")

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

def fetch_latest_replay(player_name):
    print(f"Fetching latest replay for player: {player_name}...")
    headers = {"Authorization": BALLCHASING_API_KEY}
    params = {"player-name": player_name, "count": 1}
    
    response = requests.get("https://ballchasing.com/api/replays", headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    if not data.get("list"):
        print(f"No replays found for player {player_name}.")
        return None
        
    return data["list"][0]["id"]

def fetch_replay_stats(replay_id):
    print(f"Fetching detailed stats for replay ID: {replay_id}...")
    headers = {"Authorization": BALLCHASING_API_KEY}
    
    response = requests.get(f"https://ballchasing.com/api/replays/{replay_id}", headers=headers)
    response.raise_for_status()
    
    return response.json()

def safe_get(d, keys, default=0):
    """Safely get nested dictionary values, defaulting to 0 or None for missing stats."""
    for key in keys:
        if d is None or not isinstance(d, dict):
            return default
        d = d.get(key)
        if d is None:
            return default
    return d

def map_json_to_sql(replay_data, target_player_name=None):
    sql_data_list = []
    
    for color in ["blue", "orange"]:
        if color not in replay_data or "players" not in replay_data[color]:
            continue
            
        for player_data in replay_data[color]["players"]:
            if target_player_name and player_data.get("name", "").lower() != target_player_name.lower():
                # We can choose to filter, but let's just parse everyone if we want lobby data
                # Actually, the user asked to fetch all players, so we MUST parse all players.
                pass
                
            stats = player_data.get("stats", {})
            
            # Determine if player won
            blue_goals = replay_data.get("blue", {}).get("goals", 0)
            orange_goals = replay_data.get("orange", {}).get("goals", 0)
            if color == "blue":
                match_win = blue_goals > orange_goals
            else:
                match_win = orange_goals > blue_goals

            player_id_obj = player_data.get("id", {})
            platform = player_id_obj.get("platform", "unknown")
            pid = player_id_obj.get("id", "unknown")
            
            sql_data = {
                "id": str(uuid.uuid4()),
                "ballchasing_id": replay_data.get("id"),
                "player_name": player_data.get("name"),
                "player_id": f"{platform}:{pid}",
                
                "core_shots": safe_get(stats, ["core", "shots"]),
                "core_goals": safe_get(stats, ["core", "goals"]),
                "core_saves": safe_get(stats, ["core", "saves"]),
                "core_assists": safe_get(stats, ["core", "assists"]),
                "core_score": safe_get(stats, ["core", "score"]),
                "core_mvp": safe_get(stats, ["core", "mvp"], False),
                "core_shooting_pct": safe_get(stats, ["core", "shooting_percentage"]),
                
                "boost_bpm": safe_get(stats, ["boost", "bpm"]),
                "boost_bcpm": safe_get(stats, ["boost", "bcpm"]),
                "boost_avg_amount": safe_get(stats, ["boost", "avg_amount"]),
                "boost_amount_collected": safe_get(stats, ["boost", "amount_collected"]),
                "boost_amount_stolen": safe_get(stats, ["boost", "amount_stolen"]),
                "boost_amount_overfill": safe_get(stats, ["boost", "amount_overfill"]),
                "boost_amount_overfill_stolen": safe_get(stats, ["boost", "amount_overfill_stolen"]),
                "boost_amount_used_supersonic": safe_get(stats, ["boost", "amount_used_while_supersonic"]),
                "boost_time_zero_boost": safe_get(stats, ["boost", "time_zero_boost"]),
                "boost_time_full_boost": safe_get(stats, ["boost", "time_full_boost"]),
                "boost_time_boost_0_25": safe_get(stats, ["boost", "time_boost_0_25"]),
                "boost_time_boost_25_50": safe_get(stats, ["boost", "time_boost_25_50"]),
                "boost_time_boost_50_75": safe_get(stats, ["boost", "time_boost_50_75"]),
                "boost_time_boost_75_100": safe_get(stats, ["boost", "time_boost_75_100"]),
                "boost_big_pads_collected": safe_get(stats, ["boost", "count_collected_big"]),
                "boost_small_pads_collected": safe_get(stats, ["boost", "count_collected_small"]),
                "boost_big_pads_stolen": safe_get(stats, ["boost", "count_stolen_big"]),
                "boost_small_pads_stolen": safe_get(stats, ["boost", "count_stolen_small"]),
                
                "pos_avg_dist_ball": safe_get(stats, ["positioning", "avg_distance_to_ball"]),
                "pos_avg_dist_ball_possession": safe_get(stats, ["positioning", "avg_distance_to_ball_possession"]),
                "pos_avg_dist_ball_no_possession": safe_get(stats, ["positioning", "avg_distance_to_ball_no_possession"]),
                "pos_avg_dist_teammates": safe_get(stats, ["positioning", "avg_distance_to_mates"]),
                "pos_time_behind_ball": safe_get(stats, ["positioning", "time_behind_ball"]),
                "pos_time_front_ball": safe_get(stats, ["positioning", "time_in_front_ball"]),
                "pos_time_most_back": safe_get(stats, ["positioning", "time_most_back"]),
                "pos_time_most_forward": safe_get(stats, ["positioning", "time_most_forward"]),
                "pos_time_closest_to_ball": safe_get(stats, ["positioning", "time_closest_to_ball"]),
                "pos_time_farthest_from_ball": safe_get(stats, ["positioning", "time_farthest_from_ball"]),
                "pos_percent_defensive_third": safe_get(stats, ["positioning", "percent_defensive_third"]),
                "pos_percent_neutral_third": safe_get(stats, ["positioning", "percent_neutral_third"]),
                "pos_percent_offensive_third": safe_get(stats, ["positioning", "percent_offensive_third"]),
                "pos_percent_defensive_half": safe_get(stats, ["positioning", "percent_defensive_half"]),
                "pos_percent_offensive_half": safe_get(stats, ["positioning", "percent_offensive_half"]),
                
                "mov_avg_speed": safe_get(stats, ["movement", "avg_speed"]),
                "mov_total_distance": safe_get(stats, ["movement", "total_distance"]),
                "mov_time_supersonic_speed": safe_get(stats, ["movement", "time_supersonic_speed"]),
                "mov_time_boost_speed": safe_get(stats, ["movement", "time_boost_speed"]),
                "mov_time_slow_speed": safe_get(stats, ["movement", "time_slow_speed"]),
                "mov_percent_supersonic_speed": safe_get(stats, ["movement", "percent_supersonic_speed"]),
                "mov_percent_boost_speed": safe_get(stats, ["movement", "percent_boost_speed"]),
                "mov_percent_slow_speed": safe_get(stats, ["movement", "percent_slow_speed"]),
                "mov_time_ground": safe_get(stats, ["movement", "time_ground"]),
                "mov_time_low_air": safe_get(stats, ["movement", "time_low_air"]),
                "mov_time_high_air": safe_get(stats, ["movement", "time_high_air"]),
                "mov_percent_ground": safe_get(stats, ["movement", "percent_ground"]),
                "mov_percent_low_air": safe_get(stats, ["movement", "percent_low_air"]),
                "mov_percent_high_air": safe_get(stats, ["movement", "percent_high_air"]),
                "mov_time_powerslide": safe_get(stats, ["movement", "time_powerslide"]),
                "mov_count_powerslide": safe_get(stats, ["movement", "count_powerslide"]),
                "mov_avg_powerslide_duration": safe_get(stats, ["movement", "avg_powerslide_duration"]),
                
                "demo_inflicted": safe_get(stats, ["demo", "inflicted"]),
                "demo_taken": safe_get(stats, ["demo", "taken"]),
                
                "match_win": match_win,
                "match_duration": replay_data.get("duration", 0),
                "team_color": color,
                "playlist_id": replay_data.get("playlist_id", None),
            }
            sql_data_list.append(sql_data)
            
    return sql_data_list

def insert_into_db(sql_data_list):
    if not sql_data_list:
        return 0
        
    if isinstance(sql_data_list, dict):
        sql_data_list = [sql_data_list]
        
    total_affected = 0
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for sql_data in sql_data_list:
                columns = list(sql_data.keys())
                values = tuple(sql_data.values())
                
                placeholders = ", ".join(["%s"] * len(columns))
                columns_str = ", ".join(columns)
                
                # We migrated the constraint to be unique on (ballchasing_id, player_name)
                query = f"""
                    INSERT INTO player_stats ({columns_str})
                    VALUES ({placeholders})
                    ON CONFLICT (ballchasing_id, player_name) DO NOTHING;
                """
                cur.execute(query, values)
                total_affected += cur.rowcount
            conn.commit()
            return total_affected
    finally:
        conn.close()

def main():
    if not BALLCHASING_API_KEY or not PLAYER_NAME:
        print("BALLCHASING_API_KEY or PLAYER_NAME not set in .env")
        return

    try:
        replay_id = fetch_latest_replay(PLAYER_NAME)
        if not replay_id:
            return
            
        replay_data = fetch_replay_stats(replay_id)
        
        sql_data_list = map_json_to_sql(replay_data, None)
        
        row_count = insert_into_db(sql_data_list)
        
        if row_count > 0:
            print(f"SUCCESS: Replay '{replay_id}' successfully ingested and mapped {row_count} players.")
            print(f"Number of columns mapped successfully: {len(sql_data_list[0]) if sql_data_list else 0}")
        else:
            print(f"SUCCESS: Replay '{replay_id}' was already in the database. (0 rows inserted)")
            
    except Exception as e:
        print(f"Error during data ingestion: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
