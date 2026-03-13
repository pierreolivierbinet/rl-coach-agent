import os
import sys
import os
import sys

# Ensure root directory is in path to resolve 'src' module
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../.."))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

import psycopg2
from dotenv import load_dotenv
import reflex as rx

from src.agents.coach_logic import get_coach_response

load_dotenv()

DB_NAME = "rl_coach_db"
DB_USER = os.getenv("DB_USER", "coach_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "development_password")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


class State(rx.State):
    """The app state."""
    show_results: bool = False
    mode: str = "3v3"
    scope: str = "Match"
    coach_query: str = ""
    db_error: str = ""
    
    # DB Replay state
    replays: list[dict[str, str]] = []
    selected_replay_id: str = ""
    
    # AI state
    is_loading: bool = False
    ai_response: str = ""
    radar_data: list[dict] = []
    
    # History state
    history_data: list[dict] = []
    
    # Lobby selection
    lobby_players: list[str] = []
    selected_player: str = ""
    selected_player_stats: dict = {}

    async def handle_analyze(self):
        """Simulate starting analysis."""
        if not self.selected_replay_id:
            self.show_results = True
            self.ai_response = "**Error:** Please select a replay from the sidebar first."
            yield
            return

        self.show_results = True
        self.is_loading = True
        self.ai_response = ""
        yield
        
        # 1. DB Fetch Context
        if not self.selected_player:
            self.ai_response = "**Error:** Please select a Focus Player from the lobby."
            self.is_loading = False
            yield
            return
            
        conn = get_db_connection()
        user_stats = {}
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM player_stats WHERE ballchasing_id = %s AND player_name = %s", 
                    (self.selected_replay_id, self.selected_player)
                )
                result = cur.fetchone()
                if result:
                    colnames = [desc[0] for desc in cur.description]
                    user_stats = dict(zip(colnames, result))
                    self.selected_player_stats = user_stats
        except Exception as e:
            pass
        finally:
            conn.close()

        # 2. AI Fetch
        if not user_stats:
            self.ai_response = "**Error:** Could not connect to the database to find this replay."
        else:
            response = await get_coach_response(self.coach_query, user_stats, self.mode)
            if isinstance(response, dict):
                self.ai_response = response.get("text", "")
                pro_avg = response.get("pro_averages", {})
                
                # Radar metrics mapping
                radar_metrics = [
                    {"subject": "Speed", "metric": "mov_avg_speed"},
                    {"subject": "Aggression", "metric": "demo_inflicted"},
                    {"subject": "Positioning", "metric": "pos_time_behind_ball"},
                    {"subject": "Boost Efficiency", "metric": "boost_bpm"}
                ]
                
                new_radar_data = []
                for rm in radar_metrics:
                    metric = rm["metric"]
                    try:
                        u_val = float(user_stats.get(metric, 0))
                        p_val = float(pro_avg.get(metric, 0))
                        
                        # Normalize values roughly against the max of either + 1 for visualization
                        max_val = max(u_val, p_val) + 1
                        new_radar_data.append({
                            "subject": rm["subject"],
                            "You": round((u_val / max_val) * 100, 1),
                            "Pro": round((p_val / max_val) * 100, 1),
                            "fullMark": 100,
                        })
                    except (ValueError, TypeError, ZeroDivisionError):
                        continue
                self.radar_data = new_radar_data
                self.fetch_history_data()
            else:
                 self.ai_response = response
            
        self.is_loading = False
        yield
        
    def fetch_history_data(self):
        """Fetch the last 10 matches for the selected player to track trends."""
        self.history_data = []
        if not self.selected_player:
            return
            
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at, boost_time_zero_boost, pos_time_behind_ball 
                    FROM player_stats 
                    WHERE player_name = %s 
                    ORDER BY created_at ASC 
                    LIMIT 20
                    """, 
                    (self.selected_player,)
                )
                results = cur.fetchall()
                for row in results[-10:]: # Keep last 10
                    try:
                        dt = row[0].strftime("%m-%d") if row[0] else "Unknown"
                        zero_boost = float(row[1]) if row[1] is not None else 0
                        behind_ball = float(row[2]) if row[2] is not None else 0
                        self.history_data.append({
                            "date": dt,
                            "zero_boost": round(zero_boost, 1),
                            "behind_ball": round(behind_ball, 1)
                        })
                    except (ValueError, TypeError):
                        pass
            conn.close()
        except Exception as e:
            print(f"Error fetching history data: {e}")

    def set_mode(self, mode: str):
        self.mode = mode
        # Reset current selection and analytics when switching game modes
        self.selected_replay_id = ""
        self.selected_player = ""
        self.lobby_players = []
        self.ai_response = ""
        self.radar_data = []
        self.history_data = []
        self.show_results = False
        
        self.fetch_replays_from_db()
        
    def set_scope(self, scope: str):
        self.scope = scope
        
    def select_replay(self, replay_id: str):
        self.selected_replay_id = replay_id
        self.fetch_lobby_players()
        
    def set_selected_player(self, player_name: str):
        self.selected_player = player_name
        
    def fetch_lobby_players(self):
        """Fetch all unique player names for the currently selected replay."""
        self.lobby_players = []
        self.selected_player = ""
        
        if not self.selected_replay_id:
            return
            
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT player_name FROM player_stats WHERE ballchasing_id = %s ORDER BY team_color, player_name", 
                    (self.selected_replay_id,)
                )
                results = cur.fetchall()
                # Deduplicate and clean, though schema should enforce uniqueness now
                players = [row[0] for row in results if row[0]]
                self.lobby_players = list(dict.fromkeys(players))
                
                if self.lobby_players:
                    # Attempt to default to the primary player profile if they are in the lobby
                    primary_name = os.getenv("PLAYER_NAME", "")
                    if primary_name in self.lobby_players:
                        self.selected_player = primary_name
                    else:
                        self.selected_player = self.lobby_players[0]
            conn.close()
        except Exception as e:
            print(f"Error fetching lobby players: {e}")
            
    def fetch_replays_from_db(self):
        """Fetch matches based on selected game mode."""
        self.replays = []
        self.db_error = ""
        
        print(f"Attempting to connect to {DB_NAME} at {DB_HOST} with user {DB_USER}...")
        
        player_name = os.getenv("PLAYER_NAME", "PO")
        having_clause = ""
        if self.mode == "2v2":
            having_clause = "HAVING MAX(p.playlist_id) IN ('ranked-doubles', 'private-doubles') OR (MAX(p.playlist_id) = 'private' AND COUNT(*) = 4)"
        elif self.mode == "3v3":
            having_clause = "HAVING MAX(p.playlist_id) IN ('ranked-standard', 'ranked-solo-standard', 'private-standard') OR (MAX(p.playlist_id) = 'private' AND COUNT(*) = 6)"
            
        query = f"""
            SELECT 
                p.ballchasing_id,
                MAX(p.created_at) as created_at,
                SUM(CASE WHEN p.team_color = 'blue' THEN p.core_goals ELSE 0 END) as blue_goals,
                SUM(CASE WHEN p.team_color = 'orange' THEN p.core_goals ELSE 0 END) as orange_goals,
                MAX(CASE WHEN p.player_name = %s THEN CAST(p.match_win AS INT) ELSE NULL END) as user_won,
                MAX(CASE WHEN p.player_name = %s THEN p.team_color ELSE NULL END) as user_team,
                MAX(p.playlist_id) as playlist
            FROM player_stats p
            GROUP BY p.ballchasing_id
            {having_clause}
            ORDER BY MAX(p.created_at) DESC;
        """
        
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(query, (player_name, player_name))
                results = cur.fetchall()
                print(f"DEBUG: Found {len(results)} replays in database for mode {self.mode}.")
                for row in results:
                    bid = str(row[0])
                    dt = row[1]
                    b_goals = int(row[2]) if row[2] is not None else 0
                    o_goals = int(row[3]) if row[3] is not None else 0
                    u_won = row[4]
                    u_team = row[5]
                    playlist = row[6] or "Match"
                    
                    time_str = dt.strftime("%H:%M") if dt else "00:00"
                    
                    if u_team == 'blue':
                        t_score = b_goals
                        o_score = o_goals
                    elif u_team == 'orange':
                        t_score = o_goals
                        o_score = b_goals
                    else:
                        t_score = b_goals
                        o_score = o_goals
                        
                    if u_won == 1:
                        res_str = "Win"
                    elif u_won == 0:
                        res_str = "Loss"
                    else:
                        res_str = "Match"
                        
                    label = f"[{time_str}] {res_str} ({t_score}-{o_score})"

                    self.replays.append({
                        "ballchasing_id": bid,
                        "date": label,
                    })
            conn.close()
        except Exception as e:
            self.db_error = str(e)
            print(f"Error fetching DB: {e}")
            
    def on_load(self):
        """Fetch initial data when page loads."""
        self.fetch_replays_from_db()


def replay_item(replay: dict[str, str]) -> rx.Component:
    """A component to render a single human-readable replay item with active highlighting."""
    is_selected = State.selected_replay_id == replay["ballchasing_id"]
    return rx.box(
        rx.button(
            replay['date'],
            size="2",
            width="100%",
            variant=rx.cond(is_selected, "solid", "outline"),
            color_scheme=rx.cond(is_selected, "purple", "gray"),
            on_click=lambda: State.select_replay(replay["ballchasing_id"]),
            justify="start",
            padding_x="1rem",
            border_radius="md",
            box_shadow=rx.cond(is_selected, "0 0 10px rgba(168, 85, 247, 0.4)", "none"),
        ),
        padding="0.2rem",
        width="100%",
    )

def sidebar() -> rx.Component:
    """The sidebar configuration component."""
    return rx.box(
        rx.vstack(
            rx.heading("Settings", size="4", weight="bold"),
            rx.cond(State.db_error, rx.text(State.db_error, color="red"), rx.text("DB Connected", color="green")),
            rx.divider(margin_y="4"),
            
            rx.text("Mode", size="2", weight="medium", color=rx.color("slate", 11)),
            rx.select(
                ["2v2", "3v3"],
                value=State.mode,
                on_change=State.set_mode,
                width="100%",
                variant="surface",
            ),
            
            rx.box(height="1rem"),
            
            # Dynamic Lobby Player Selection
            rx.cond(
                State.lobby_players,
                rx.vstack(
                    rx.text("Focus Player", size="2", weight="medium", color=rx.color("blue", 11)),
                    rx.select(
                        State.lobby_players,
                        value=State.selected_player,
                        on_change=State.set_selected_player,
                        width="100%",
                        variant="surface",
                        color_scheme="blue"
                    ),
                    width="100%",
                    spacing="2"
                ),
                rx.box()
            ),
            
            rx.box(height="1rem"),
            rx.divider(margin_y="2"),
            rx.hstack(
                rx.text("Recent Replays", size="2", weight="medium", color=rx.color("slate", 11)),
                rx.spacer(),
                rx.button(
                    rx.icon("refresh-cw", size=14),
                    "Refresh",
                    size="1",
                    on_click=State.fetch_replays_from_db,
                    variant="ghost"
                ),
                width="100%",
                align_items="center"
            ),
            
            # Replays List
            rx.scroll_area(
                rx.vstack(
                    rx.cond(
                        State.replays.length() == 0,
                        rx.vstack(
                            rx.text("No replays found in DB", color="gray"),
                            rx.button("Force Fetch All", on_click=State.fetch_replays_from_db, size="1")
                        ),
                        rx.foreach(State.replays, replay_item)
                    ),
                    spacing="2",
                    width="100%"
                ),
                height="300px",
                width="100%",
                type="auto",
            ),
            
            spacing="2",
            align_items="start",
            width="100%",
        ),
        width="260px",
        height="100vh",
        padding="1rem",
        bg=rx.color("slate", 2),
        border_right=f"1px solid {rx.color('slate', 5)}",
        position="fixed",
        left="0",
        top="0",
        z_index="10",
    )

def main_content() -> rx.Component:
    """The central input and analytic grid."""
    
    # Text input area
    query_input = rx.vstack(
        rx.heading("RL Coach Agent", size="8", weight="bold", align="center"),
        rx.text("Ask me about your latest game, or how you compare to the pros.", color=rx.color("slate", 11), align="center"),
        rx.box(height="1rem"),
        rx.hstack(
            rx.input(
                placeholder="E.g., What went wrong? Why are my rotations slow?",
                value=State.coach_query,
                on_change=State.setvar("coach_query"),
                size="3",
                width="100%",
                radius="large",
                variant="surface",
            ),
            rx.button(
                rx.icon("send", size=20),
                "Analyze",
                size="3",
                on_click=State.handle_analyze,
                radius="large",
                variant="solid",
                color_scheme="blue",
            ),
            width="100%",
            align_items="center",
            spacing="2",
        ),
        # When analyzing, visually compress the query input block
        width=rx.cond(State.show_results, "100%", "60%"), 
        max_width="800px",
        padding="2rem",
        transition="all 0.4s ease-in-out",
        align_items="center",
    )

    # Placeholder charts grid (visible only when showing results)
    analytic_grid = rx.cond(
        State.show_results,
        rx.vstack(
            # Loading or Response
            rx.cond(
                State.is_loading,
                rx.center(
                    rx.vstack(
                        rx.spinner(size="3"),
                        rx.text("Analyzing match data...", color=rx.color("slate", 11)),
                        align_items="center"
                    ),
                    padding="2rem",
                    width="100%",
                ),
                rx.vstack(
                    # KPI Dashboard Cards
                    rx.grid(
                        rx.card(
                            rx.hstack(
                                rx.icon("rocket", size=20, color=rx.color("purple", 9)),
                                rx.vstack(
                                    rx.text("Goals", size="1", color=rx.color("slate", 10), weight="medium"),
                                    rx.text(State.selected_player_stats["core_goals"].to_string(), size="6", weight="bold"),
                                    align_items="start",
                                    spacing="0",
                                ),
                                align_items="center",
                                spacing="3",
                            ),
                            variant="surface",
                            padding="1rem",
                        ),
                        rx.card(
                            rx.hstack(
                                rx.icon("shield", size=20, color=rx.color("blue", 9)),
                                rx.vstack(
                                    rx.text("Saves", size="1", color=rx.color("slate", 10), weight="medium"),
                                    rx.text(State.selected_player_stats["core_saves"].to_string(), size="6", weight="bold"),
                                    align_items="start",
                                    spacing="0",
                                ),
                                align_items="center",
                                spacing="3",
                            ),
                            variant="surface",
                            padding="1rem",
                        ),
                        rx.card(
                            rx.hstack(
                                rx.icon("zap", size=20, color=rx.color("yellow", 9)),
                                rx.vstack(
                                    rx.text("Avg Boost", size="1", color=rx.color("slate", 10), weight="medium"),
                                    rx.text(State.selected_player_stats["boost_avg_amount"].to_string().split(".")[0], size="6", weight="bold"),
                                    align_items="start",
                                    spacing="0",
                                ),
                                align_items="center",
                                spacing="3",
                            ),
                            variant="surface",
                            padding="1rem",
                        ),
                        columns="3",
                        spacing="4",
                        width="100%",
                        margin_bottom="1rem",
                    ),
                    rx.box(
                        rx.markdown(
                            State.ai_response,
                            style={
                                "th": {
                                    "padding": "1rem 1.5rem", 
                                    "text-align": "left", 
                                    "border-bottom": f"2px solid {rx.color('slate', 8)}", 
                                    "background-color": "#2D2D2D",
                                    "font-weight": "800",
                                    "color": "#F0F0F0",
                                    "text-transform": "uppercase",
                                    "letter-spacing": "0.05em",
                                },
                                "td": {
                                    "padding": "1rem 1.5rem", 
                                    "border-bottom": f"1px solid {rx.color('slate', 4)}",
                                    "color": "#E0E0E0",
                                },
                                "table": {
                                    "width": "100%", 
                                    "border-collapse": "collapse", 
                                    "margin": "1rem 0",
                                    "background-color": "#121212",
                                    "border-radius": "12px",
                                    "overflow": "hidden",
                                    "border": f"1px solid {rx.color('slate', 6)}",
                                },
                                "tr:nth-child(even)": {
                                    "background-color": "#1A1A1A",
                                },
                                "tr:nth-child(odd)": {
                                    "background-color": "#212121",
                                },
                                "h1, h2, h3": {"margin-top": "1.5rem", "margin-bottom": "0.75rem", "font-weight": "bold", "color": "white"},
                                "p": {"margin-bottom": "1rem", "line-height": "1.6", "color": "#CCCCCC"},
                                "li": {"margin-bottom": "0.5rem", "color": "#CCCCCC"},
                            }
                        ),
                        bg="#0D0D0D",
                        padding="2.5rem",
                        border_radius="20px",
                        box_shadow="0 25px 50px -12px rgba(0, 0, 0, 0.5)",
                        border=f"1px solid {rx.color('slate', 5)}",
                        width="100%",
                    ),
                    width="100%",
                )
            ),
            
            # Additional Charts
            rx.cond(
                State.radar_data,
                rx.grid(
                    rx.card(
                        rx.vstack(
                            rx.text("Performance Radar", weight="bold", size="4", color="white"),
                            rx.recharts.radar_chart(
                                rx.recharts.radar(
                                    data_key="You",
                                    stroke="#8884d8",
                                    fill="rgba(173, 216, 230, 0.2)",
                                    fill_opacity=1,
                                    stroke_width=2,
                                ),
                                rx.recharts.radar(
                                    data_key="Pro",
                                    stroke="#9ca3af",
                                    fill="rgba(200, 200, 200, 0.1)",
                                    fill_opacity=1,
                                    stroke_width=1,
                                ),
                                rx.recharts.polar_grid(stroke=rx.color("slate", 5)),
                                rx.recharts.polar_angle_axis(data_key="subject", stroke=rx.color("slate", 9), font_size=12),
                                rx.recharts.polar_radius_axis(angle=30, domain=[0, 100], stroke="none"),
                                rx.recharts.legend(),
                                data=State.radar_data,
                                width="100%",
                                height=250,
                            ),
                            align_items="center",
                            width="100%",
                        ),
                        variant="surface",
                        bg="#1A1A1A",
                        box_shadow="0 10px 15px -3px rgba(0, 0, 0, 0.4)",
                        border=f"1px solid {rx.color('slate', 4)}",
                        padding="1.5rem",
                        width="100%",
                        border_radius="xl",
                    ),
                    rx.card(
                        rx.vstack(
                            rx.text("Recent Trend (Last 10 Matches)", weight="bold", size="4", color="white"),
                            rx.recharts.line_chart(
                                rx.recharts.line(
                                    data_key="zero_boost",
                                    type_="monotone",
                                    stroke="#8884d8",
                                    name="Time Zero Boost (s)",
                                    stroke_width=2,
                                ),
                                rx.recharts.reference_line(
                                    y=30,
                                    stroke="#ef4444",
                                    stroke_dasharray="3 3",
                                    label="Pro Avg"
                                ),
                                rx.recharts.line(
                                    data_key="behind_ball",
                                    type_="monotone",
                                    stroke="#10b981",
                                    name="Time Behind Ball (s)",
                                    stroke_width=2,
                                ),
                                rx.recharts.x_axis(data_key="date", stroke=rx.color("slate", 9)),
                                rx.recharts.y_axis(stroke=rx.color("slate", 9)),
                                rx.recharts.cartesian_grid(stroke_dasharray="3 3", stroke=rx.color("slate", 5)),
                                rx.recharts.tooltip(),
                                rx.recharts.legend(),
                                data=State.history_data,
                                width="100%",
                                height=250,
                            ),
                            align_items="center",
                            width="100%",
                        ),
                        variant="surface",
                        bg="#1A1A1A",
                        box_shadow="0 10px 15px -3px rgba(0, 0, 0, 0.4)",
                        border=f"1px solid {rx.color('slate', 4)}",
                        padding="1.5rem",
                        width="100%",
                        border_radius="xl",
                    ),
                    columns="2",
                    spacing="4",
                    width="100%",
                    margin_top="2rem",
                ),
                rx.box()
            ),
            width="100%",
            spacing="4"
        ),
        rx.box()
    )

    return rx.box(
        rx.vstack(
            query_input,
            analytic_grid,
            width="100%",
            height="100vh",
            align_items="center",
            justify_content=rx.cond(State.show_results, "start", "center"),
            padding_top=rx.cond(State.show_results, "4rem", "0"),
            transition="all 0.4s ease-in-out",
            padding_x="2rem",
        ),
        margin_left="260px", # Offset for fixed sidebar
        width="calc(100% - 260px)",
        bg=rx.color("slate", 1),
    )

def index() -> rx.Component:
    """The main page layout."""
    return rx.box(
        sidebar(),
        main_content(),
        background_color=rx.color("slate", 1),
        min_height="100vh",
        color=rx.color("slate", 12),
        font_family="system-ui, sans-serif",
        on_mount=State.fetch_replays_from_db,
    )

app = rx.App(
    theme=rx.theme(
        appearance="dark",
        has_background=True,
        radius="large",
        accent_color="blue", # Highlights
        gray_color="slate",  # Dark zinc/slate palette
    ),
)
app.add_page(index, title="RL Coach Agent", on_load=State.on_load)
