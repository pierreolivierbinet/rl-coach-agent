"""
fetch_pro_benchmarks.py
-----------------------
Fetches pro group stats from the Ballchasing API and computes per-game
averages across all players, supporting multiple game modes (2v2, 3v3).

Usage:
    uv run src/tools/fetch_pro_benchmarks.py --mode 3v3
    uv run src/tools/fetch_pro_benchmarks.py --mode 2v2

Environment variables required in .env:
    BALLCHASING_API_KEY  — your Ballchasing token
    PRO_2V2_IDS         — comma-separated list of group IDs for 2v2
    PRO_3V3_IDS         — comma-separated list of group IDs for 3v3

Output:
    data/benchmarks/{mode}/pro_reference_data.json
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

API_KEY  = os.getenv("BALLCHASING_API_KEY")
HEADERS  = {"Authorization": API_KEY}
BASE_URL = "https://ballchasing.com/api"

MODE_ENV_KEYS = {
    "2v2": "PRO_2V2_IDS",
    "3v3": "PRO_3V3_IDS",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def safe_get(d: dict, *keys, default=0.0):
    """Safely traverse a nested dict; returns default if any key is missing."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d if d is not None else default


def map_player_game_average(ga: dict) -> dict:
    """
    Map a player's game_average block to our schema.
    Keys mirror the column names defined in schema.sql (player_stats table).
    """
    return {
        # ── Core ─────────────────────────────────────────────────────────────
        "core_shots":          safe_get(ga, "core", "shots"),
        "core_goals":          safe_get(ga, "core", "goals"),
        "core_saves":          safe_get(ga, "core", "saves"),
        "core_assists":        safe_get(ga, "core", "assists"),
        "core_score":          safe_get(ga, "core", "score"),
        "core_shooting_pct":   safe_get(ga, "core", "shooting_percentage"),

        # ── Boost ────────────────────────────────────────────────────────────
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

        # ── Positioning ──────────────────────────────────────────────────────
        "pos_avg_dist_ball":               safe_get(ga, "positioning", "avg_distance_to_ball"),
        "pos_avg_dist_ball_possession":    safe_get(ga, "positioning", "avg_distance_to_ball_possession"),
        "pos_avg_dist_ball_no_possession": safe_get(ga, "positioning", "avg_distance_to_ball_no_possession"),
        "pos_avg_dist_teammates":          safe_get(ga, "positioning", "avg_distance_to_mates"),
        "pos_time_behind_ball":            safe_get(ga, "positioning", "time_behind_ball"),
        "pos_time_front_ball":             safe_get(ga, "positioning", "time_infront_ball"),
        "pos_time_most_back":              safe_get(ga, "positioning", "time_most_back"),
        "pos_time_most_forward":           safe_get(ga, "positioning", "time_most_forward"),
        "pos_time_closest_to_ball":        safe_get(ga, "positioning", "time_closest_to_ball"),
        "pos_time_farthest_from_ball":     safe_get(ga, "positioning", "time_farthest_from_ball"),
        "pos_percent_defensive_third":     safe_get(ga, "positioning", "percent_defensive_third"),
        "pos_percent_neutral_third":       safe_get(ga, "positioning", "percent_neutral_third"),
        "pos_percent_offensive_third":     safe_get(ga, "positioning", "percent_offensive_third"),
        "pos_percent_defensive_half":      safe_get(ga, "positioning", "percent_defensive_half"),
        "pos_percent_offensive_half":      safe_get(ga, "positioning", "percent_offensive_half"),

        # ── Movement ─────────────────────────────────────────────────────────
        "mov_avg_speed":                safe_get(ga, "movement", "avg_speed"),
        "mov_total_distance":           safe_get(ga, "movement", "total_distance"),
        "mov_time_supersonic_speed":    safe_get(ga, "movement", "time_supersonic_speed"),
        "mov_time_boost_speed":         safe_get(ga, "movement", "time_boost_speed"),
        "mov_time_slow_speed":          safe_get(ga, "movement", "time_slow_speed"),
        "mov_percent_supersonic_speed": safe_get(ga, "movement", "percent_supersonic_speed"),
        "mov_percent_boost_speed":      safe_get(ga, "movement", "percent_boost_speed"),
        "mov_percent_slow_speed":       safe_get(ga, "movement", "percent_slow_speed"),
        "mov_time_ground":              safe_get(ga, "movement", "time_ground"),
        "mov_time_low_air":             safe_get(ga, "movement", "time_low_air"),
        "mov_time_high_air":            safe_get(ga, "movement", "time_high_air"),
        "mov_percent_ground":           safe_get(ga, "movement", "percent_ground"),
        "mov_percent_low_air":          safe_get(ga, "movement", "percent_low_air"),
        "mov_percent_high_air":         safe_get(ga, "movement", "percent_high_air"),
        "mov_time_powerslide":          safe_get(ga, "movement", "time_powerslide"),
        "mov_count_powerslide":         safe_get(ga, "movement", "count_powerslide"),
        "mov_avg_powerslide_duration":  safe_get(ga, "movement", "avg_powerslide_duration"),

        # ── Playstyle ────────────────────────────────────────────────────────
        "demo_inflicted": safe_get(ga, "demo", "inflicted"),
        "demo_taken":     safe_get(ga, "demo", "taken"),
    }


def compute_field_averages(players_mapped: list[dict]) -> dict:
    """Compute the mean of each stat field across all players."""
    if not players_mapped:
        return {}

    keys = players_mapped[0].keys()
    averages = {}
    for k in keys:
        values = [p[k] for p in players_mapped if isinstance(p[k], (int, float))]
        averages[k] = round(sum(values) / len(values), 6) if values else 0.0
    return averages


def fetch_group(group_id: str) -> tuple[str, list]:
    """
    Fetch a single group from the Ballchasing API.
    Returns (group_name, players_raw_list).
    """
    url = f"{BASE_URL}/groups/{group_id}"
    print(f"  Fetching group '{group_id}'...")
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    return data.get("name", group_id), data.get("players", [])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Fetch and aggregate pro benchmarks from Ballchasing groups."
    )
    parser.add_argument(
        "--mode",
        choices=["2v2", "3v3"],
        required=True,
        help="Game mode to process. Reads PRO_2V2_IDS or PRO_3V3_IDS from .env.",
    )
    args = parser.parse_args()
    mode = args.mode

    # Validate API key
    if not API_KEY:
        print("ERROR: BALLCHASING_API_KEY is not set in .env")
        sys.exit(1)

    # Load the list of group IDs for the selected mode
    env_key = MODE_ENV_KEYS[mode]
    raw_ids = os.getenv(env_key, "").strip()
    if not raw_ids:
        print(f"ERROR: {env_key} is not set or empty in .env")
        sys.exit(1)

    group_ids = [gid.strip() for gid in raw_ids.split(",") if gid.strip()]
    print(f"\n[Mode: {mode}] Processing {len(group_ids)} group(s) from {env_key}")
    print("=" * 60)

    # Aggregate across all groups
    all_players_mapped   = []   # flat list of per-player stat dicts (for global avg)
    all_player_details   = []   # metadata list
    all_individual_players = {} # name -> stats dict (last group wins on collision)
    groups_fetched = []

    for group_id in group_ids:
        try:
            group_name, players_raw = fetch_group(group_id)
        except requests.HTTPError as e:
            print(f"  [ERROR] Could not fetch group '{group_id}': {e}")
            continue

        groups_fetched.append({"id": group_id, "name": group_name})
        group_count = 0

        for p in players_raw:
            ga = p.get("game_average", {})
            if not ga:
                print(f"    [SKIP] {p.get('name', '?')} -- no game_average data")
                continue

            mapped   = map_player_game_average(ga)
            name_key = p.get("name", "").lower()

            all_players_mapped.append(mapped)
            all_individual_players[name_key] = mapped
            all_player_details.append({
                "name":       p.get("name"),
                "team":       p.get("team"),
                "group_id":   group_id,
                "group_name": group_name,
                "games":      p.get("cumulative", {}).get("games", 0),
            })
            group_count += 1
            print(f"    [OK] {p.get('name')} ({p.get('team', '?')}) -- {p.get('cumulative', {}).get('games', 0)} games")

        print(f"  --> {group_count} player(s) added from '{group_name}'")

    if not all_players_mapped:
        print("\nERROR: No player data collected. Check your group IDs and API key.")
        sys.exit(1)

    print(f"\nComputing global averages across {len(all_players_mapped)} player(s)...")
    benchmark = compute_field_averages(all_players_mapped)

    # Build output JSON
    output = {
        "metadata": {
            "mode":         mode,
            "date":         datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "groups":       groups_fetched,
            "player_count": len(all_players_mapped),
            "players":      all_player_details,
            "description": (
                f"Per-game average stats across all pro players in the {mode} reference groups. "
                "Each metric key mirrors a column in the player_stats PostgreSQL table."
            ),
        },
        "averages":           benchmark,
        "individual_players": all_individual_players,
    }

    # Save to data/benchmarks/{mode}/pro_reference_data.json
    output_dir  = Path("data/benchmarks") / mode
    output_file = output_dir / "pro_reference_data.json"
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n[DONE] Benchmark saved --> {output_file}")
    print(f"   Mode            : {mode}")
    print(f"   Groups processed: {len(groups_fetched)}")
    print(f"   Total players   : {len(all_players_mapped)}")
    print(f"   Metrics/player  : {len(benchmark)}")

    # Quick preview of key stats
    print(f"\n-- Key averages ({mode} pro reference) --")
    preview_keys = [
        "core_goals", "core_saves", "core_shooting_pct",
        "boost_bpm", "boost_avg_amount", "boost_time_zero_boost",
        "mov_avg_speed", "mov_percent_supersonic_speed",
        "pos_percent_defensive_third", "pos_percent_offensive_third",
        "demo_inflicted",
    ]
    for k in preview_keys:
        val = benchmark.get(k)
        print(f"  {k:<38} {val:.4f}" if val is not None else f"  {k:<38} N/A")


if __name__ == "__main__":
    main()
