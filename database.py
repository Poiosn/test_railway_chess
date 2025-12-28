import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Global DB Pool
db_pool = None

def init_db_pool():
    """Initialize the database connection pool and create tables."""
    global db_pool
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL not set. Database features will be disabled.")
        return

    # Fix Railway Postgres URL
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    try:
        # Reduced pool size (1-4) for Railway stability
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 4, dsn=DATABASE_URL)
        print("✅ Database connection pool created!")
        
        # Initialize Tables - Get connection directly from pool
        if db_pool:
            conn = None
            try:
                conn = db_pool.getconn()
                cur = conn.cursor()
                
                print("🔨 Creating/updating database tables...")
                
                # 1. Create Visitors Table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS visitors (
                        visit_date DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                        visit_count INTEGER DEFAULT 0
                    );
                """)
                print("  ✓ Visitors table ready")
                
                # 2. Create Users Table (NEW)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(50) UNIQUE NOT NULL,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        display_name VARCHAR(100),
                        avatar_url VARCHAR(500),
                        elo_rating INTEGER DEFAULT 1200,
                        games_played INTEGER DEFAULT 0,
                        games_won INTEGER DEFAULT 0,
                        games_drawn INTEGER DEFAULT 0,
                        games_lost INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP
                    );
                """)
                print("  ✓ Users table ready")
                
                # 3. Create or Update Games Table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS games (
                        id SERIAL PRIMARY KEY,
                        room_name VARCHAR(255),
                        white_player VARCHAR(255),
                        black_player VARCHAR(255),
                        winner VARCHAR(50),
                        win_reason VARCHAR(100),
                        start_time TIMESTAMP,
                        end_time TIMESTAMP
                    );
                """)
                
                # Add new columns to existing games table if they don't exist
                # Check and add white_user_id column
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='games' AND column_name='white_user_id';
                """)
                if not cur.fetchone():
                    cur.execute("ALTER TABLE games ADD COLUMN white_user_id INTEGER REFERENCES users(id);")
                    print("    + Added white_user_id column to games")
                
                # Check and add black_user_id column
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='games' AND column_name='black_user_id';
                """)
                if not cur.fetchone():
                    cur.execute("ALTER TABLE games ADD COLUMN black_user_id INTEGER REFERENCES users(id);")
                    print("    + Added black_user_id column to games")
                
                # Check and add time_control column
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='games' AND column_name='time_control';
                """)
                if not cur.fetchone():
                    cur.execute("ALTER TABLE games ADD COLUMN time_control INTEGER;")
                    print("    + Added time_control column to games")
                
                # Check and add game_mode column
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='games' AND column_name='game_mode';
                """)
                if not cur.fetchone():
                    cur.execute("ALTER TABLE games ADD COLUMN game_mode VARCHAR(50);")
                    print("    + Added game_mode column to games")
                
                print("  ✓ Games table ready")
                
                # 4. Create Game Moves Table (NEW - for replays)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS game_moves (
                        id SERIAL PRIMARY KEY,
                        game_id INTEGER REFERENCES games(id) ON DELETE CASCADE,
                        move_number INTEGER NOT NULL,
                        move_notation VARCHAR(20) NOT NULL,
                        from_square VARCHAR(2),
                        to_square VARCHAR(2),
                        piece VARCHAR(10),
                        captured_piece VARCHAR(10),
                        time_remaining_white REAL,
                        time_remaining_black REAL,
                        position_fen TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                print("  ✓ Game_moves table ready")
                
                # 5. Create Indexes for Performance (only after columns exist!)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_games_white_user ON games(white_user_id);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_games_black_user ON games(black_user_id);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_game_moves_game_id ON game_moves(game_id);
                """)
                print("  ✓ Indexes created")
                
                conn.commit()
                cur.close()
                print("✅ Database tables ready!")
                
            except Exception as e:
                print(f"❌ Table creation error: {e}")
                import traceback
                traceback.print_exc()
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    db_pool.putconn(conn)
            
    except Exception as e:
        print(f"❌ Failed to create DB pool: {e}")

def get_db_conn():
    """Get a fresh, live connection from the pool."""
    global db_pool
    if not db_pool:
        return None
    
    try:
        conn = db_pool.getconn()
        # Liveness Check: Verify connection is active
        if conn:
            if conn.closed:
                db_pool.putconn(conn, close=True)
                return db_pool.getconn()
            
            # Double check with a simple query
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return conn
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                # Connection is dead, get a new one
                try:
                    db_pool.putconn(conn, close=True)
                except:
                    pass
                return db_pool.getconn()
    except Exception as e:
        print(f"⚠️ DB Pool Exhausted or Error: {e}")
        return None
    return None

def release_db_conn(conn):
    """Return connection to pool."""
    global db_pool
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass

# ===== APP HELPER FUNCTIONS =====

def increment_visitor_count():
    """Upsert visitor count for today."""
    conn = get_db_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO visitors (visit_date, visit_count) 
            VALUES (CURRENT_DATE, 1)
            ON CONFLICT (visit_date) 
            DO UPDATE SET visit_count = visitors.visit_count + 1
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Visitor count error: {e}")
        conn.rollback()
    finally:
        release_db_conn(conn)

def get_total_visitor_count():
    """Get sum of all visits."""
    conn = get_db_conn()
    count = 0
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(SUM(visit_count), 0) FROM visitors")
            res = cur.fetchone()
            count = res[0] if res else 0
            cur.close()
        except Exception as e:
            print(f"Visitor API error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)
    return count

def get_leaderboard_data(limit=5):
    """Get top 5 players by wins."""
    conn = get_db_conn()
    data = []
    if conn:
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT player_name, COUNT(*) as total,
                SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END) as wins,
                ROUND((SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*),0))*100, 1) as win_rate
                FROM (
                    SELECT white_player as player_name, 'white' as player_color, winner FROM games WHERE white_player IS NOT NULL AND white_player != 'Bot'
                    UNION ALL
                    SELECT black_player as player_name, 'black' as player_color, winner FROM games WHERE black_player IS NOT NULL AND black_player != 'Bot'
                ) as sub GROUP BY player_name HAVING COUNT(*) > 0 ORDER BY wins DESC LIMIT %s
            """, (limit,))
            data = cur.fetchall()
            cur.close()
        except Exception as e:
            print(f"Leaderboard error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)
    return [dict(row) for row in data]

def save_game_record(room, g, start_time, end_time, win_reason):
    """Save finished game with moves and user stats."""
    conn = get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            # Get user IDs if available
            white_user_id = g.get("white_user_id")
            black_user_id = g.get("black_user_id")
            
            # Insert game
            cur.execute("""
                INSERT INTO games 
                (room_name, white_player, black_player, white_user_id, black_user_id,
                 winner, win_reason, start_time, end_time, time_control, game_mode) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                room, 
                g["white_player"], 
                g["black_player"],
                white_user_id,
                black_user_id,
                g["winner"], 
                win_reason, 
                start_time, 
                end_time,
                int(g.get("whiteTime", 300)),
                g.get("game_mode", "friend")
            ))
            
            game_id = cur.fetchone()[0]
            
            # Save moves if available
            if "move_history" in g and g["move_history"]:
                for idx, move_data in enumerate(g["move_history"]):
                    cur.execute("""
                        INSERT INTO game_moves 
                        (game_id, move_number, move_notation, from_square, to_square,
                         time_remaining_white, time_remaining_black, position_fen)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        game_id,
                        idx + 1,
                        move_data.get("notation", ""),
                        move_data.get("from_square", ""),
                        move_data.get("to_square", ""),
                        move_data.get("white_time", 0),
                        move_data.get("black_time", 0),
                        move_data.get("fen", "")
                    ))
            
            # Update user stats if users are linked
            if white_user_id:
                update_user_stats(cur, white_user_id, g["winner"], "white")
            if black_user_id:
                update_user_stats(cur, black_user_id, g["winner"], "black")
            
            conn.commit()
            cur.close()
            print(f"✅ Game {room} saved to DB with ID {game_id}.")
            return True
        except Exception as e:
            print(f"❌ Save game error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)
    return False

def update_user_stats(cur, user_id, winner, player_color):
    """Update user statistics after game."""
    try:
        # Determine result
        if winner == "draw":
            cur.execute("""
                UPDATE users 
                SET games_played = games_played + 1,
                    games_drawn = games_drawn + 1
                WHERE id = %s
            """, (user_id,))
        elif winner == player_color:
            # Won
            cur.execute("""
                UPDATE users 
                SET games_played = games_played + 1,
                    games_won = games_won + 1,
                    elo_rating = elo_rating + 25
                WHERE id = %s
            """, (user_id,))
        else:
            # Lost
            cur.execute("""
                UPDATE users 
                SET games_played = games_played + 1,
                    games_lost = games_lost + 1,
                    elo_rating = GREATEST(elo_rating - 20, 100)
                WHERE id = %s
            """, (user_id,))
    except Exception as e:
        print(f"Error updating user stats: {e}")

# ===== AUTHENTICATION FUNCTIONS =====

def get_user_by_id(user_id):
    """Get user by ID."""
    conn = get_db_conn()
    if not conn:
        return None
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, username, email, display_name, elo_rating, 
                   games_played, games_won, games_drawn, games_lost 
            FROM users WHERE id = %s
        """, (user_id,))
        user = cur.fetchone()
        cur.close()
        return dict(user) if user else None
    except Exception as e:
        print(f"Error fetching user: {e}")
        return None
    finally:
        release_db_conn(conn)

def get_user_by_username(username):
    """Get user by username (for login)."""
    conn = get_db_conn()
    if not conn:
        return None
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, username, email, password_hash, display_name, elo_rating, 
                   games_played, games_won, games_drawn, games_lost
            FROM users 
            WHERE username = %s OR email = %s
        """, (username, username))
        user = cur.fetchone()
        cur.close()
        return dict(user) if user else None
    except Exception as e:
        print(f"Error fetching user: {e}")
        return None
    finally:
        release_db_conn(conn)

def create_user(username, email, password_hash, display_name):
    """Create new user."""
    conn = get_db_conn()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (username, email, password_hash, display_name)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (username, email, password_hash, display_name))
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return user_id
    except Exception as e:
        print(f"Error creating user: {e}")
        conn.rollback()
        return None
    finally:
        release_db_conn(conn)

def update_last_login(user_id):
    """Update user's last login timestamp."""
    conn = get_db_conn()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = %s", (user_id,))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error updating last login: {e}")
        conn.rollback()
    finally:
        release_db_conn(conn)

def get_user_profile(username):
    """Get public user profile."""
    conn = get_db_conn()
    if not conn:
        return None
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT username, display_name, elo_rating, games_played, 
                   games_won, games_drawn, games_lost, created_at
            FROM users 
            WHERE username = %s
        """, (username,))
        user = cur.fetchone()
        cur.close()
        return dict(user) if user else None
    except Exception as e:
        print(f"Error fetching profile: {e}")
        return None
    finally:
        release_db_conn(conn)

def get_user_games(username):
    """Get user's game history."""
    conn = get_db_conn()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get user ID
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        user_result = cur.fetchone()
        if not user_result:
            cur.close()
            return []
        
        user_id = user_result['id']
        
        # Get games
        cur.execute("""
            SELECT g.id, g.room_name, g.white_player, g.black_player, 
                   g.winner, g.win_reason, g.start_time, g.end_time,
                   g.time_control, g.game_mode,
                   w.username as white_username, b.username as black_username
            FROM games g
            LEFT JOIN users w ON g.white_user_id = w.id
            LEFT JOIN users b ON g.black_user_id = b.id
            WHERE g.white_user_id = %s OR g.black_user_id = %s
            ORDER BY g.end_time DESC
            LIMIT 50
        """, (user_id, user_id))
        
        games = cur.fetchall()
        cur.close()
        return [dict(g) for g in games]
    except Exception as e:
        print(f"Error fetching games: {e}")
        return []
    finally:
        release_db_conn(conn)

def get_game_replay(game_id):
    """Get game info and moves for replay."""
    conn = get_db_conn()
    if not conn:
        return None
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get game info
        cur.execute("""
            SELECT g.*, w.username as white_username, b.username as black_username
            FROM games g
            LEFT JOIN users w ON g.white_user_id = w.id
            LEFT JOIN users b ON g.black_user_id = b.id
            WHERE g.id = %s
        """, (game_id,))
        
        game = cur.fetchone()
        if not game:
            cur.close()
            return None
        
        # Get moves
        cur.execute("""
            SELECT move_number, move_notation, from_square, to_square, 
                   piece, captured_piece, time_remaining_white, 
                   time_remaining_black, position_fen, timestamp
            FROM game_moves
            WHERE game_id = %s
            ORDER BY move_number
        """, (game_id,))
        
        moves = cur.fetchall()
        cur.close()
        
        return {
            'game': dict(game),
            'moves': [dict(m) for m in moves]
        }
    except Exception as e:
        print(f"Error fetching replay: {e}")
        return None
    finally:
        release_db_conn(conn)
