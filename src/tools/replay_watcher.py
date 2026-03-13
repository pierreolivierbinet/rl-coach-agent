import os
import sys
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Ensure the module path allows us to import our own tools
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.tools.fetch_replays import map_json_to_sql, insert_into_db

load_dotenv()

API_KEY = os.getenv("BALLCHASING_API_KEY")
PLAYER_NAME = os.getenv("PLAYER_NAME", "PO")
# Note: Ensure the user replaces USERNAME with their actual Windows username in their local .env
REPLAY_DIR = os.getenv("REPLAY_DIR", "").strip('"').strip("'")

UPLOAD_URL = "https://ballchasing.com/api/v2/upload"
REPLAY_STATUS_URL = "https://ballchasing.com/api/replays/{}"

class ReplayHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".replay"):
            return
            
        replay_path = Path(event.src_path)
        print(f"\n[DETECTED] New replay found: {replay_path.name}")
        
        # Rocket League can hold a lock on the file briefly while writing.
        # Simple backoff to ensure it is completely written.
        time.sleep(2)
        
        replay_id = self._upload_replay(replay_path)
        if replay_id:
            self._poll_and_insert(replay_id)

    def _upload_replay(self, filepath: Path) -> str | None:
        headers = {"Authorization": API_KEY}
        try:
            with open(filepath, "rb") as f:
                print(f"[ACTION] Uploading {filepath.name} to Ballchasing...")
                res = requests.post(UPLOAD_URL, headers=headers, files={"file": f})
                
            res.raise_for_status()
            data = res.json()
            replay_id = data.get("id")
            print(f"[UPLOADED] Successfully uploaded. Ballchasing ID: {replay_id}")
            return replay_id
        except requests.exceptions.HTTPError as he:
            if 'res' in locals() and res.status_code == 409:
                print("[INFO] Replay already exists on Ballchasing. Fetching existing ID...")
                try:
                    data = res.json()
                    replay_id = data.get("id")
                    print(f"         Found existing ID: {replay_id}")
                    return replay_id
                except Exception:
                    pass
            print(f"[ERROR] HTTP Error during upload {filepath.name}: {he}")
            return None
        except Exception as e:
            print(f"[ERROR] Failed to upload {filepath.name}: {e}")
            return None

    def _poll_and_insert(self, replay_id: str):
        headers = {"Authorization": API_KEY}
        url = REPLAY_STATUS_URL.format(replay_id)
        
        max_retries = 15
        delay = 5
        
        print(f"[ACTION] Polling for processing status every {delay} seconds...")
        
        for i in range(max_retries):
            time.sleep(delay)
            try:
                res = requests.get(url, headers=headers)
                
                # If still processing, Ballchasing might return a 404 or a different payload without stats
                if res.status_code == 200:
                    data = res.json()
                    status = data.get("status")
                    
                    if status == "ok" or "stats" in data.get("blue", {}).get("players", [{}])[0]:
                        print(f"[PROCESSED] Ballchasing has finished parsing replay '{replay_id}'.")
                        
                        # Data Mapping & DB Insertion
                        try:
                            # Re-using identical logic from fetch_replays.py
                            sql_data = map_json_to_sql(data, PLAYER_NAME)
                            rows = insert_into_db(sql_data)
                            if rows > 0:
                                print(f"[DB_UPDATED] Replay '{replay_id}' stored in PostgreSQL successfully.")
                            else:
                                print(f"[DB_UPDATED] Replay '{replay_id}' was already mapped (Conflict ignored).")
                        except Exception as e:
                            print(f"[ERROR] Failed to map or insert replay '{replay_id}': {e}")
                        
                        return
                    else:
                        print(f"  - Status: '{status}' Processing... ({i+1}/{max_retries})")
                else:
                    print(f"  - Waiting for availability... ({i+1}/{max_retries})")
            except Exception as e:
                print(f"[ERROR] Polling failed: {e}")
                
        print(f"[ERROR] Replay '{replay_id}' did not process completely in time. Manual sync required later.")

def main():
    if not API_KEY or not PLAYER_NAME:
        print("[ERROR] Missing BALLCHASING_API_KEY or PLAYER_NAME in .env")
        return
        
    if not REPLAY_DIR or not os.path.isdir(REPLAY_DIR):
        print(f"[ERROR] REPLAY_DIR in .env is invalid or does not exist: {REPLAY_DIR}")
        print("Please fix the REPLAY_DIR path in your .env file and restart.")
        # If the folder does not exist, watchdog will crash.
        return
        
    print(f"🚀 Initializing RL-Coach-Agent Replay Watcher...")
    print(f"📂 Monitoring directory: {REPLAY_DIR}")
    print("⏳ Waiting for new matches... (Press Ctrl+C to exit)\n")
    
    event_handler = ReplayHandler()
    observer = Observer()
    observer.schedule(event_handler, path=REPLAY_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping watcher...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
