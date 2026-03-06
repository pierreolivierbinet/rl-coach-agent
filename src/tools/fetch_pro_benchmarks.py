"""
fetch_pro_benchmarks.py
-----------------------
Fetches the pro group stats from the Ballchasing API and computes
per-game averages across all players to create a comparison baseline.

Saves result to: data/benchmarks/pro_radiant_averages.json
"""

import os
import json
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

API_KEY = os.getenv("BALLCHASING_API_KEY")
GROUP_ID = os.getenv("PRO_GROUP_ID")

OUTPUT_DIR = Path("data/benchmarks")
OUTPUT_FILE = OUTPUT_DIR / "pro_reference_data.json"

HEADERS = {"Authorization": API_KEY}
BASE_URL = "https://ballchasing.com/api"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def safe_get(d: dict, *keys, default=0.0):
    """Safely traverse nested dict; returns default if any key is missing."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d if d is not None else default


def map_player_game_average(ga: dict) -> dict:
    """
    Map a player's game_average block to our 66-column schema.
    Keys mirror the column names in player_stats (schema.sql).
    """
    return {
        # ── Core ───────────────────────────────────────────────────────────
        "core_shots":          safe_get(ga, "core", "shots"),
        "core_goals":          safe_get(ga, "core", "goals"),
        "core_saves":          safe_get(ga, "core", "saves"),
        "core_assists":        safe_get(ga, "core", "assists"),
        "core_score":          safe_get(ga, "core", "score"),
        "core_shooting_pct":   safe_get(ga, "core", "shooting_percentage"),

        # ── Boost ──────────────────────────────────────────────────────────
        "boost_bpm":                     safe_get(ga, "boost", "bpm"),
        "boost_bcpm":                    safe_get(ga, "boost", "bcpm"),
        "boost_avg_amount":              safe_get(ga, "boost", "avg_amount"),
        "boost_amount_collected":        safe_get(ga, "boost", "amount_collected"),
        "boost_amount_stolen":           safe_get(ga, "boost", "amount_stolen"),
        "boost_amount_overfill":         safe_get(ga, "boost", "amount_overfill"),
        "boost_amount_overfill_stolen":  safe_get(ga, "boost", "amount_overfill_stolen"),
        "boost_amount_used_supersonic":  safe_get(ga, "boost", "amount_used_while_supersonic"),
        "boost_time_zero_boost":         safe_get(ga, "boost", "time_zero_boost"),
        "boost_time_full_boost":         safe_get(ga, "boost", "time_full_boost"),
        "boost_time_boost_0_25":         safe_get(ga, "boost", "time_boost_0_25"),
        "boost_time_boost_25_50":        safe_get(ga, "boost", "time_boost_25_50"),
        "boost_time_boost_50_75":        safe_get(ga, "boost", "time_boost_50_75"),
        "boost_time_boost_75_100":       safe_get(ga, "boost", "time_boost_75_100"),
        "boost_big_pads_collected":      safe_get(ga, "boost", "count_collected_big"),
        "boost_small_pads_collected":    safe_get(ga, "boost", "count_collected_small"),
        "boost_big_pads_stolen":         safe_get(ga, "boost", "count_stolen_big"),
        "boost_small_pads_stolen":       safe_get(ga, "boost", "count_stolen_small"),

        # ── Positioning ────────────────────────────────────────────────────
        "pos_avg_dist_ball":              safe_get(ga, "positioning", "avg_distance_to_ball"),
        "pos_avg_dist_ball_possession":   safe_get(ga, "positioning", "avg_distance_to_ball_possession"),
        "pos_avg_dist_ball_no_possession":safe_get(ga, "positioning", "avg_distance_to_ball_no_possession"),
        "pos_avg_dist_teammates":         safe_get(ga, "positioning", "avg_distance_to_mates"),
        "pos_time_behind_ball":           safe_get(ga, "positioning", "time_behind_ball"),
        "pos_time_front_ball":            safe_get(ga, "positioning", "time_infront_ball"),
        "pos_time_most_back":             safe_get(ga, "positioning", "time_most_back"),
        "pos_time_most_forward":          safe_get(ga, "positioning", "time_most_forward"),
        "pos_time_closest_to_ball":       safe_get(ga, "positioning", "time_closest_to_ball"),
        "pos_time_farthest_from_ball":    safe_get(ga, "positioning", "time_farthest_from_ball"),
        "pos_percent_defensive_third":    safe_get(ga, "positioning", "percent_defensive_third"),
        "pos_percent_neutral_third":      safe_get(ga, "positioning", "percent_neutral_third"),
        "pos_percent_offensive_third":    safe_get(ga, "positioning", "percent_offensive_third"),
        "pos_percent_defensive_half":     safe_get(ga, "positioning", "percent_defensive_half"),
        "pos_percent_offensive_half":     safe_get(ga, "positioning", "percent_offensive_half"),

        # ── Movement ───────────────────────────────────────────────────────
        "mov_avg_speed":               safe_get(ga, "movement", "avg_speed"),
        "mov_total_distance":          safe_get(ga, "movement", "total_distance"),
        "mov_time_supersonic_speed":   safe_get(ga, "movement", "time_supersonic_speed"),
        "mov_time_boost_speed":        safe_get(ga, "movement", "time_boost_speed"),
        "mov_time_slow_speed":         safe_get(ga, "movement", "time_slow_speed"),
        "mov_percent_supersonic_speed":safe_get(ga, "movement", "percent_supersonic_speed"),
        "mov_percent_boost_speed":     safe_get(ga, "movement", "percent_boost_speed"),
        "mov_percent_slow_speed":      safe_get(ga, "movement", "percent_slow_speed"),
        "mov_time_ground":             safe_get(ga, "movement", "time_ground"),
        "mov_time_low_air":            safe_get(ga, "movement", "time_low_air"),
        "mov_time_high_air":           safe_get(ga, "movement", "time_high_air"),
        "mov_percent_ground":          safe_get(ga, "movement", "percent_ground"),
        "mov_percent_low_air":         safe_get(ga, "movement", "percent_low_air"),
        "mov_percent_high_air":        safe_get(ga, "movement", "percent_high_air"),
        "mov_time_powerslide":         safe_get(ga, "movement", "time_powerslide"),
        "mov_count_powerslide":        safe_get(ga, "movement", "count_powerslide"),
        "mov_avg_powerslide_duration": safe_get(ga, "movement", "avg_powerslide_duration"),

        # ── Playstyle ──────────────────────────────────────────────────────
        "demo_inflicted": safe_get(ga, "demo", "inflicted"),
        "demo_taken":     safe_get(ga, "demo", "taken"),
    }


def compute_field_averages(players_mapped: list[dict]) -> dict:
    """Average each stat field across all players."""
    if not players_mapped:
        return {}

    keys = players_mapped[0].keys()
    averages = {}
    for k in keys:
        values = [p[k] for p in players_mapped if isinstance(p[k], (int, float))]
        averages[k] = round(sum(values) / len(values), 6) if values else 0.0
    return averages


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not API_KEY:
        raise RuntimeError("BALLCHASING_API_KEY is not set in .env")
    if not GROUP_ID:
        raise RuntimeError("PRO_GROUP_ID is not set in .env")

    print(f"Fetching group '{GROUP_ID}' from Ballchasing API...")
    url = f"{BASE_URL}/groups/{GROUP_ID}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    group_name = data.get("name", GROUP_ID)
    players_raw = data.get("players", [])

    if not players_raw:
        print("No players found in the group response.")
        return

    print(f"Group: {group_name}")
    print(f"Players found: {len(players_raw)}")

    # Map each player's game_average to our schema
    players_mapped = []      # list of stat dicts (for averaging)
    player_details = []      # metadata list
    individual_players = {}  # name -> stats dict

    for p in players_raw:
        ga = p.get("game_average", {})
        if not ga:
            print(f"  [SKIP] {p.get('name', '?')} -- no game_average, skipping")
            continue
        mapped = map_player_game_average(ga)
        name_key = p.get("name", "").lower()
        players_mapped.append(mapped)
        individual_players[name_key] = mapped
        player_details.append({
            "name": p.get("name"),
            "team": p.get("team"),
            "games": p.get("cumulative", {}).get("games", 0),
        })
        print(f"  [OK]  {p.get('name')} ({p.get('team')}) -- {p.get('cumulative', {}).get('games', 0)} games")

    print(f"\nComputing averages across {len(players_mapped)} players...")
    benchmark = compute_field_averages(players_mapped)

    # Build final JSON output
    output = {
        "metadata": {
            "group_id": GROUP_ID,
            "group_name": group_name,
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "player_count": len(players_mapped),
            "players": player_details,
            "description": (
                "Per-game average stats across all pro players in this group. "
                "Each metric mirrors a column in the player_stats PostgreSQL table."
            ),
        },
        "averages": benchmark,
        "individual_players": individual_players,
    }

    # Save to disk
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n[DONE] Benchmark saved --> {OUTPUT_FILE}")
    print(f"   Metrics per player:  {len(benchmark)}")
    print(f"   Individual players:  {len(individual_players)}")

    # Quick preview of key stats
    print("\n-- Key benchmark values (pro average per game) --")
    preview_keys = [
        "core_goals", "core_saves", "core_shooting_pct",
        "boost_bpm", "boost_avg_amount", "boost_time_zero_boost",
        "mov_avg_speed", "mov_percent_supersonic_speed",
        "pos_percent_defensive_third", "pos_percent_offensive_third",
        "demo_inflicted",
    ]
    for k in preview_keys:
        print(f"  {k:<38} {benchmark.get(k, 'N/A'):.4f}")


if __name__ == "__main__":
    main()
