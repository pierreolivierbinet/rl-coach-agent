import os
import psycopg2
from dotenv import load_dotenv
from pydantic_ai import Agent, RunContext

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "rl_coach_db")
DB_USER = os.getenv("DB_USER", "coach_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "development_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Prevent pydantic-ai from crashing on import if no API key is present
if not os.getenv("OPENAI_API_KEY") and not os.getenv("ANTHROPIC_API_KEY"):
    os.environ["OPENAI_API_KEY"] = "dummy_key_to_allow_initialization"

# Define the Agent
# We use OpenAI by default since it falls back to standard LLM via env vars
# If you are using Anthropic, you can change 'openai:gpt-4o' to 'anthropic:claude-3-5-sonnet-latest'
agent = Agent(
    'openai:gpt-4o',
    deps_type=None,
    system_prompt=(
        "You are the RL-Coach-Agent. Your mission is to help the user reach Radiant rank in Rocket League. "
        "You combine high-level Data Analytics with Buddhist principles (mindfulness, discipline, detachment from tilt). "
        "Your tone is formal, precise, and encouraging. You never guess; you use the provided tools to see real data."
    )
)

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

@agent.tool
def get_last_match_metrics(ctx: RunContext) -> dict:
    """Fetch the latest replay stats from the PostgreSQL database."""
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
            return {"error": "No replays found."}
    except Exception as e:
        return {"error": f"Database error: {e}"}
    finally:
        conn.close()

if __name__ == "__main__":
    query = "Based on my 25.10 inefficiency index and my 33 seconds at zero boost, what is your first lesson on mindfulness in-game?"
    print(f"User: {query}\n")
    print("Coach is thinking...\n")
    
    try:
        # Run the agent
        # We need an OPENAI_API_KEY environment variable set for standard execution.
        # If running without the API key, this will throw an error.
        result = agent.run_sync(query)
        print("Coach:")
        print(result.data)
    except Exception as e:
        print("\nError running agent:")
        print(e)
        print("\nNote: Make sure you have set the appropriate API key in your .env file (e.g., OPENAI_API_KEY).")
