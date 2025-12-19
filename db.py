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
        
        # Initialize Tables
        conn = get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                
                # 1. Create Visitors Table (New Structure: Date-based)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS visitors (
                        visit_date DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                        visit_count INTEGER DEFAULT 0
                    );
                """)
                
                # 2. Create Games Table
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
                
                conn.commit()
                cur.close()
                print("✅ Database tables checked/created.")
            except Exception as e:
                print(f"❌ Table creation error: {e}")
                conn.rollback()
            finally:
                release_db_conn(conn)
            
    except Exception as e:
        print(f"❌ Failed to create DB pool: {e}")

def get_db_conn():
    """Get a fresh, live connection from the pool."""
    global db_pool
    if not db_pool:
        init_db_pool()
    
    if db_pool:
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
        # Try to insert today's date. If exists, add +1 to count.
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
    """Save finished game."""
    conn = get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO games 
                (room_name, white_player, black_player, winner, win_reason, start_time, end_time) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                room, 
                g["white_player"], 
                g["black_player"], 
                g["winner"], 
                win_reason, 
                start_time, 
                end_time
            ))
            conn.commit()
            cur.close()
            print(f"✅ Game {room} saved to DB.")
            return True
        except Exception as e:
            print(f"❌ Save game error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)
    return False
