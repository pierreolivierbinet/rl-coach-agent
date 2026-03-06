import os
import json
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

BENCHMARK_FILE = Path("data/benchmarks/pro_reference_data.json")

# Guard: prevent crash if key is missing at import time
if not os.getenv("GEMINI_API_KEY"):
    os.environ["GEMINI_API_KEY"] = "dummy_key_to_allow_initialization"

# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
model = GoogleModel("gemini-2.5-flash")

SYSTEM_PROMPT = """
You are the RL-Coach-Agent, an elite Rocket League performance analyst.
Your mission is to deliver precise, data-driven coaching using the player's
own PostgreSQL stats combined with a professional reference dataset
(the Boston Major group benchmark).

## Règles d'analyse

1. ANALYSE GÉNÉRALE :
   Appuie systématiquement tes conseils techniques sur les moyennes du groupe
   de référence ("Moyenne du groupe de référence"). Ne dis jamais "Elite Standard"
   ou "niveau professionnel générique" — cite les chiffres réels du benchmark.

2. COMPARAISON SPÉCIFIQUE À UN PRO :
   Compare le joueur à un pro individuel UNIQUEMENT si :
   a) Le joueur le demande explicitement (ex: "compare-moi à vatira").
   b) Tu détectes une métrique où le joueur est exceptionnellement proche
      ou éloigné du style d'un pro particulier dans le groupe.
   Dans ce cas, nomme le joueur pro directement (ex: "vatira affiche 412 BPM,
   tu es à 365 — la même école de gestion frugale du boost").

3. OUTILS DISPONIBLES :
   - get_last_match_metrics : récupère les stats du dernier replay en base.
   - get_comparison_data : récupère les données de référence pro.
     → Sans argument : retourne la moyenne du groupe (usage par défaut).
     → Avec player_name="zen" : retourne les stats individuelles de ce pro.

4. TON & STYLE :
   Sois professionnel, pédagogique et ultra-précis. Cite les métriques avec
   leurs valeurs numériques. Propose des exercices concrets pour combler les écarts.
""".strip()

agent = Agent(
    model,
    deps_type=None,
    system_prompt=SYSTEM_PROMPT,
)

# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@agent.tool
def get_last_match_metrics(ctx: RunContext) -> dict:
    """Fetch the player's latest replay stats from the PostgreSQL database."""
    query = """
        SELECT *
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
                colnames = [desc[0] for desc in cur.description]
                return dict(zip(colnames, result))
            return {"error": "No replays found in the database."}
    except Exception as e:
        return {"error": f"Database error: {e}"}
    finally:
        conn.close()


@agent.tool
def get_comparison_data(ctx: RunContext, player_name: str = "") -> dict:
    """
    Return professional reference data from the Boston Major benchmark file.

    - If player_name is empty or omitted  → returns the global group averages.
    - If player_name is provided (e.g. "zen", "vatira") → returns that specific
      pro player's per-game stats for direct comparison.
    """
    if not BENCHMARK_FILE.exists():
        return {"error": f"Benchmark file not found: {BENCHMARK_FILE}"}

    try:
        with open(BENCHMARK_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"error": f"Failed to read benchmark file: {e}"}

    # --- Specific pro requested ---
    if player_name:
        key = player_name.strip().lower()
        individual = data.get("individual_players", {})
        if key in individual:
            return {
                "mode": "individual",
                "player_name": key,
                "stats": individual[key],
            }
        # Fuzzy fallback: partial name match
        matches = {k: v for k, v in individual.items() if key in k}
        if matches:
            matched_name = next(iter(matches))
            return {
                "mode": "individual",
                "player_name": matched_name,
                "note": f"Exact name '{player_name}' not found, using closest match.",
                "stats": matches[matched_name],
            }
        return {
            "error": (
                f"Player '{player_name}' not found in benchmark. "
                f"Available: {', '.join(sorted(individual.keys()))}"
            )
        }

    # --- Default: group averages ---
    return {
        "mode": "group_average",
        "group_name": data.get("metadata", {}).get("group_name", "Pro Group"),
        "player_count": data.get("metadata", {}).get("player_count", 0),
        "stats": data.get("averages", {}),
    }


# ---------------------------------------------------------------------------
# Interactive session
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  RL-Coach-Agent — Session interactive")
    print("  Tapez 'quit', 'exit' ou 'q' pour quitter.")
    print("=" * 60)

    while True:
        try:
            user_input = input("\nPosez votre question au coach : ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSession terminée.")
            break

        if not user_input:
            continue

        if user_input.lower() in ("quit", "exit", "q"):
            print("Session terminée. Bonne chance sur les terrains !")
            break

        print("\nCoach réfléchit...\n")
        try:
            result = agent.run_sync(user_input)
            print(f"Coach :\n{result.output}")
        except Exception as e:
            print(f"\nErreur : {e}")
            print("Vérifiez que GEMINI_API_KEY est bien défini dans votre fichier .env.")
