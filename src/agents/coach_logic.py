"""
coach_logic.py
--------------
Main agent definition for RL-Coach-Agent.
Exposes two tools to the LLM:
  - get_last_match_metrics : reads the latest replay from PostgreSQL, filtered by game mode.
  - get_comparison_data    : reads the correct pro benchmark JSON based on game mode.
"""
import json
import math
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from pydantic_ai import Agent, RunContext

# pydantic-ai >= 1.66 changed the import path for GoogleModel
from pydantic_ai.models.google import GoogleModel

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()

DB_NAME     = os.getenv("DB_NAME",     "rl_coach_db")
DB_USER     = os.getenv("DB_USER",     "coach_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "development_password")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")

# playlist_id values recognized per mode
PLAYLIST_2V2 = {"ranked-doubles", "private-doubles"}
PLAYLIST_3V3 = {"ranked-standard", "ranked-solo-standard", "private-standard"}

BENCHMARK_BASE = Path("data/benchmarks")

# Guard: prevent a crash at import time if the API key is not yet configured
if not os.getenv("GEMINI_API_KEY"):
    os.environ["GEMINI_API_KEY"] = "dummy_key_to_allow_initialization"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _benchmark_path(mode: str) -> Path:
    """Return the benchmark JSON path for the given mode ('2v2' or '3v3')."""
    return BENCHMARK_BASE / mode / "pro_reference_data.json"


def _mode_from_playlist(playlist_id: str | None) -> str:
    """Map a Ballchasing playlist_id string to '2v2' or '3v3'."""
    if playlist_id and playlist_id.lower() in PLAYLIST_2V2:
        return "2v2"
    return "3v3"   # safe default


def calculate_similarity(user_stats: dict, pro_stats: dict, avg_stats: dict) -> tuple[float, str]:
    """Calculate normalized Euclidean similarity and identify the closest matching dimension."""
    metrics = {
        "Boost Efficiency": ["boost_bpm", "boost_avg_amount", "boost_amount_stolen", "boost_time_zero_boost"],
        "Speed & Aggression": ["mov_avg_speed", "mov_percent_supersonic_speed", "mov_percent_boost_speed", "demo_inflicted"],
        "Positioning": ["pos_time_behind_ball", "pos_time_front_ball", "mov_percent_ground", "mov_percent_high_air"]
    }
    
    total_dist_sq = 0.0
    total_count = 0
    dim_scores = {}
    
    for category, m_list in metrics.items():
        cat_dist_sq = 0.0
        cat_count = 0
        for m in m_list:
            u = user_stats.get(m)
            p = pro_stats.get(m)
            a = avg_stats.get(m)
            if u is not None and p is not None and a:
                try:
                    diff = (float(u) - float(p)) / float(a)
                    sq = diff ** 2
                    cat_dist_sq += sq
                    cat_count += 1
                    total_dist_sq += sq
                    total_count += 1
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
        if cat_count > 0:
            dim_scores[category] = cat_dist_sq / cat_count
            
    if total_count == 0:
        return 0.0, "Unknown"
        
    dist = math.sqrt(total_dist_sq / total_count)
    similarity = 100.0 / (1.0 + dist)
    
    best_category = "Balanced profile"
    if dim_scores:
        best_category = min(dim_scores, key=dim_scores.get)
        
    return round(similarity, 1), f"Similar {best_category}"


def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


# ---------------------------------------------------------------------------
# Agent — built once, with a mode-aware system prompt injected at runtime
# ---------------------------------------------------------------------------
model = GoogleModel("gemini-2.5-flash")

SYSTEM_PROMPT = """
"Analysis generated via RAG (Retrieval Augmented Generation) using localized pro benchmarks."

You are a Structured Data RAG (Retrieval-Augmented Generation) Agent for Rocket League performance analysis.
Your purpose is to provide objective, data-driven coaching based on technical benchmarks.

## Structured Data Rules:
1. RAG GROUNDING: Use ONLY the injected user stats and pro averages for comparison. 
2. NO HALLUCINATION: If a specific metric is missing from the provided data, state "Data unavailable" for that metric. NEVER guess or invent values.
3. BENCHMARK INTEGRITY: 'Pro Averages' represent high-level competitive play. Compare the user's performance relative to these values.

## STRICT FORMATTING:
1. NO CONVERSATIONAL FILLER: Start immediately with the disclaimer and the Analysis Table.
2. COMPARISON TABLE:
   - Include a column for Metric, You, Pro Avg, and Status (🔴/🟡/🟢).
   - Ensure clear padding between columns for readability.
   - Ground every observation in the table.
3. KEY TAKEAWAYS: Max 3 punchy, data-backed bullet points.
4. TRAINING PLAN: Provide exactly 2 technical drills for Freeplay. NO phantom codes.

## TONE:
Elite, objective, and performance-oriented. Keep advice punchy and technical. No fluff.
""".strip()

agent = Agent(
    model,
    deps_type=None,
    system_prompt=SYSTEM_PROMPT,
    retries=5,
)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@agent.tool
def get_last_match_metrics(ctx: RunContext, mode: str = "") -> dict:
    """
    Fetch the player's latest replay stats from the PostgreSQL database.

    - mode: optional filter — '2v2' or '3v3'. If empty, returns the single
      most recent replay regardless of playlist.
    - Returns all columns including playlist_id so the agent can detect
      the game mode automatically.
    - If no rows match the mode filter (e.g. playlist_id not yet populated),
      falls back to the most recent row regardless of playlist.
    """
    def _fetch(playlist_filter: str) -> dict | None:
        query = f"""
            SELECT *
            FROM player_stats
            WHERE 1=1 {playlist_filter}
            ORDER BY created_at DESC
            LIMIT 1;
        """
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                if result:
                    colnames = [desc[0] for desc in cur.description]
                    return dict(zip(colnames, result))
                return None
        except Exception as e:
            return {"error": f"Database error: {e}"}
        finally:
            conn.close()

    if mode == "2v2":
        playlist_filter = "AND playlist_id IN ('ranked-doubles', 'private-doubles')"
    elif mode == "3v3":
        playlist_filter = "AND playlist_id IN ('ranked-standard', 'ranked-solo-standard', 'private-standard')"
    else:
        playlist_filter = ""

    row = _fetch(playlist_filter)

    # Fallback: if mode filter returned nothing (e.g. pre-migration NULLs),
    # use the most recent row regardless and trust the session-level mode context.
    if (row is None or "error" in row) and playlist_filter:
        row = _fetch("")
        if row and "error" not in row:
            row["_fallback"] = True
            row["_fallback_reason"] = (
                f"No rows matched the '{mode}' playlist filter. "
                "This replay was ingested before playlist_id tracking. "
                f"Treat it as a {mode} match per session context."
            )

    if row is None:
        return {"error": "No replays found in the database."}

    if "error" not in row:
        # Annotate the detected game mode
        detected = _mode_from_playlist(row.get("playlist_id")) if row.get("playlist_id") else mode or "3v3"
        row["_detected_mode"] = detected

    return row


@agent.tool
def get_comparison_data(
    ctx: RunContext,
    mode: str = "3v3",
    player_name: str = "",
    user_stats: dict | None = None,
) -> dict:
    """
    Return professional reference benchmark data for the given game mode.

    - mode         : '2v2' or '3v3'. Always pass the mode detected from the
                     player's last match. Defaults to '3v3'.
    - player_name  : if provided, returns that pro's individual per-game stats.
                     If empty, returns the global group average for the mode.
    - user_stats   : pass the player's stats dictionary to receive Top 3 Pro matches.
    """
    # Normalize mode
    if mode not in ("2v2", "3v3"):
        mode = "3v3"

    benchmark_file = _benchmark_path(mode)

    if not benchmark_file.exists():
        return {
            "error": (
                f"Benchmark file not found for mode '{mode}': {benchmark_file}. "
                f"Run: uv run src/tools/fetch_pro_benchmarks.py --mode {mode}"
            )
        }

    try:
        with open(benchmark_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"error": f"Failed to read benchmark file: {e}"}

    # --- Specific pro requested ---
    if player_name:
        key = player_name.strip().lower()
        individual = data.get("individual_players", {})
        if key in individual:
            return {
                "mode": mode,
                "comparison_type": "individual",
                "player_name": key,
                "stats": individual[key],
            }
        # Fuzzy fallback: partial name match
        matches = {k: v for k, v in individual.items() if key in k}
        if matches:
            matched_name = next(iter(matches))
            return {
                "mode": mode,
                "comparison_type": "individual",
                "player_name": matched_name,
                "note": f"Exact name '{player_name}' not found — using closest match.",
                "stats": matches[matched_name],
            }
        return {
            "error": (
                f"Player '{player_name}' not found in {mode} benchmark. "
                f"Available: {', '.join(sorted(individual.keys()))}"
            )
        }

    # --- Default: group averages ---
    meta = data.get("metadata", {})
    averages = data.get("averages", {})
    individual = data.get("individual_players", {})
    
    response = {
        "mode": mode,
        "comparison_type": "group_average",
        "group_name": f"Pro {mode} Reference Group",
        "groups": meta.get("groups", []),
        "player_count": meta.get("player_count", 0),
        "stats": averages,
    }

    if user_stats and isinstance(user_stats, dict):
        matches = []
        for name, p_stats in individual.items():
            sim, reason = calculate_similarity(user_stats, p_stats, averages)
            matches.append({
                "name": name.capitalize(),
                "similarity_pct": sim,
                "main_reason": reason
            })
        
        matches.sort(key=lambda x: x["similarity_pct"], reverse=True)
        response["top_3_matches"] = matches[:3]

    return response


# ---------------------------------------------------------------------------
# External API Hook
# ---------------------------------------------------------------------------
async def get_coach_response(user_query: str, user_stats: dict, mode: str = "3v3") -> dict:
    """
    Run the agent specifically from an external frontend hook (UI), bypassing local 
    DB fetch tools by directly injecting the database context to guarantee 
    accuracy for the specific replay ID.
    Returns a dict containing the AI response and the calculated pro_averages for charting.
    """
    if not user_query:
        user_query = "What went wrong? Give me an analysis of this match."
        
    pro_averages = {}
    playlist_id = user_stats.get('playlist_id') if user_stats else None
    player_name = user_stats.get('player_name') if user_stats else None
    
    # --- RAG: Data Ingestion & SQL Aggregation ---
    # We fetch other players from the same lobby/playlist to calculate real-time benchmarks.
    if playlist_id and player_name:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # SQL: Aggregate stats from all players in the session except the focus player.
                # This ensures the 'Pro Average' is relevant to the specific game context.
                cur.execute(
                    "SELECT * FROM player_stats WHERE playlist_id = %s AND player_name != %s",
                    (playlist_id, player_name)
                )
                rows = cur.fetchall()
                if rows:
                    colnames = [desc[0] for desc in cur.description]
                    dicts = [dict(zip(colnames, r)) for r in rows]
                    
                    # Logic: Iterate through all available metrics and compute the arithmetic mean.
                    for k, v in user_stats.items():
                        if k in ('id', 'ballchasing_id', 'created_at', 'updated_at', 'player_id'): 
                            continue
                        try:
                            if v is not None:
                                float(v)
                            vals = []
                            for d in dicts:
                                if d.get(k) is not None:
                                    try:
                                        vals.append(float(d[k]))
                                    except (ValueError, TypeError):
                                        pass
                            if vals:
                                pro_averages[k] = round(sum(vals) / len(vals), 2)
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            print(f"Error fetching pro averages: {e}")
        finally:
            conn.close()

    # Prevent TypeError: Object of type datetime is not JSON serializable
    if user_stats:
        user_stats = {k: v.isoformat() if hasattr(v, 'isoformat') else v for k, v in user_stats.items()}
        
    source_db = "Pro Averages"
        
    mode_context = (
        f"[Session context: the player selected {mode} mode. "
        f"You are analyzing a {mode} match. The 'Pro Average' provided comes specifically from {source_db} data. Adjust your tactical advice accordingly. "
        f"You must use the provided player stats instead of calling get_last_match_metrics. "
        f"Do not guess stats, ONLY use these injected match stats to power your response: {json.dumps(user_stats)}. "
        f"The Pro Average for {playlist_id} in our database ({source_db}) is: {json.dumps(pro_averages)}. "
        f"DO NOT invent names or stats. DO NOT hallucinate names like Rez or Badnezz. ONLY compare the user to the 'Pro Average' values provided from our database.]"
    )
    
    full_prompt = f"{mode_context}\n\nUser question: {user_query}"
    
    try:
        # Pass request_timeout directly via model_settings if valid, 
        # or just rely on agent setup retries.
        from pydantic_ai.settings import ModelSettings
        result = await agent.run(
            full_prompt, 
            model_settings=ModelSettings(timeout=60)
        )
        print(f"DEBUG: AI Raw Response: {result.output}")
        return {"text": result.output, "pro_averages": pro_averages}
    except Exception as e:
        print(f"DEBUG: AI Run Error/Raw Response parsing failed: {e}")
        # Loosened validation: If an unexpected error happens (e.g. max retries), we just print and fallback gracefully.
        return {
            "text": f"**Error connecting to AI:** {e}\n\nMake sure GEMINI_API_KEY is properly configured and the AI isn't timing out. (Validation dropped).", 
            "pro_averages": pro_averages
        }


# ---------------------------------------------------------------------------
# Interactive session with mode selection menu
# ---------------------------------------------------------------------------
def _select_mode() -> str:
    """Show a startup menu and return the selected mode ('2v2' or '3v3')."""
    print("\n" + "=" * 60)
    print("  RL-Coach-Agent — Interactive Session")
    print("=" * 60)
    print("\n  Select game mode:")
    print("  [1] 2v2 — Ranked Doubles")
    print("  [2] 3v3 — Ranked Standard")
    print()

    while True:
        try:
            choice = input("  Your choice (1 or 2): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSession ended.")
            raise SystemExit(0)

        if choice == "1":
            print("\n  Mode: 2v2 — Ranked Doubles selected.")
            print("  Benchmarks: data/benchmarks/2v2/pro_reference_data.json")
            return "2v2"
        elif choice == "2":
            print("\n  Mode: 3v3 — Ranked Standard selected.")
            print("  Benchmarks: data/benchmarks/3v3/pro_reference_data.json")
            return "3v3"
        else:
            print("  Invalid choice. Please enter 1 or 2.")


if __name__ == "__main__":
    selected_mode = _select_mode()

    # Validate benchmark file is available before starting the loop
    bf = _benchmark_path(selected_mode)
    if not bf.exists():
        print(f"\n  [WARNING] Benchmark file not found: {bf}")
        print(f"  Run first: uv run src/tools/fetch_pro_benchmarks.py --mode {selected_mode}\n")

    print(f"\n  Type 'quit', 'exit' or 'q' to end the session.")
    print("=" * 60)

    # Inject mode context into every message so the agent never has to guess
    mode_context = (
        f"[Session context: the player selected {selected_mode} mode. "
        f"Always use get_last_match_metrics(mode='{selected_mode}') and "
        f"get_comparison_data(mode='{selected_mode}') for this session.]"
    )

    while True:
        try:
            user_input = input("\nAsk your question to the coach: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSession ended.")
            break

        if not user_input:
            continue

        if user_input.lower() in ("quit", "exit", "q"):
            print("Session ended. Good luck on the field!")
            break

        print("\nCoach is thinking...\n")
        try:
            # Prepend the mode context so the agent always knows the session mode
            full_prompt = f"{mode_context}\n\nUser question: {user_input}"
            result = agent.run_sync(full_prompt)
            print(f"Coach:\n{result.output}")
        except Exception as e:
            print(f"\nError: {e}")
            print("Make sure GEMINI_API_KEY is set correctly in your .env file.")
