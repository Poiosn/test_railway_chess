import os
import psycopg2
import psycopg2.extras
import threading

DATABASE_URL = os.environ.get("DATABASE_URL")
thread_local = threading.local()

def init_db_pool():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    print("✅ PostgreSQL initialized")

def get_db_conn():
    if not hasattr(thread_local, "conn") or thread_local.conn.closed:
        thread_local.conn = psycopg2.connect(
            DATABASE_URL,
            sslmode="require",
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        thread_local.conn.autocommit = False
    return thread_local.conn

def release_db_conn():
    pass

# ---------------- CORE HELPERS ----------------

def _execute(query, params=None, fetchone=False, fetchall=False):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetchone:
                res = cur.fetchone()
            elif fetchall:
                res = cur.fetchall()
            else:
                res = True
        conn.commit()
        return res
    except Exception as e:
        conn.rollback()
        print("❌ DB ERROR:", e)
        return None

# ---------------- GAME SAVE ----------------

def save_game_record(room, g, start_time, end_time, reason):
    if not g.get("white_user_id") and not g.get("black_user_id"):
        print("❌ save_game_record skipped – no users")
        return False

    query = """
    INSERT INTO games
    (room, white_user_id, black_user_id, winner, reason, start_time, end_time)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    RETURNING id
    """

    game_id = _execute(
        query,
        (
            room,
            g.get("white_user_id"),
            g.get("black_user_id"),
            g.get("winner"),
            reason,
            start_time,
            end_time,
        ),
        fetchone=True,
    )

    if not game_id:
        return False

    game_id = game_id["id"]

    moves = g.get("move_history", [])
    for idx, m in enumerate(moves):
        _execute(
            """
            INSERT INTO game_moves
            (game_id, move_no, notation, fen)
            VALUES (%s,%s,%s,%s)
            """,
            (game_id, idx + 1, m["notation"], m["fen"]),
        )

    print(f"✅ Game saved ID={game_id}")
    return True

# ---------------- PROFILE ----------------

def get_user_games(username):
    return _execute(
        """
        SELECT g.*
        FROM games g
        JOIN users u ON u.id IN (g.white_user_id, g.black_user_id)
        WHERE u.username=%s
        ORDER BY g.id DESC
        """,
        (username,),
        fetchall=True,
    )
