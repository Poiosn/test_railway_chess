"""
Database module for Chess Master application
Supports PostgreSQL (Railway/Production) and SQLite (Local Development)
"""

import os
from datetime import datetime
import threading
import traceback

# Check if PostgreSQL is available (Railway sets DATABASE_URL)
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    # Use PostgreSQL with connection pool
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool
    USE_POSTGRES = True
    print("🐘 Using PostgreSQL database")

    # Create a connection pool for PostgreSQL
    try:
        # Use ThreadedConnectionPool for thread-safe connections
        db_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL,
            sslmode="require"
        )
        print("✅ PostgreSQL connection pool created")
    except Exception as e:
        print(f"❌ Failed to create PostgreSQL connection pool: {e}")
        traceback.print_exc()
        db_pool = None
else:
    # Use SQLite for local development
    import sqlite3
    USE_POSTGRES = False
    db_pool = None
    print("📦 Using SQLite database for local development")

# Thread-local storage for SQLite connections
thread_local = threading.local()

# SQLite database path (only used when PostgreSQL is not available)
DB_PATH = os.path.join(os.path.dirname(__file__), 'chess_master.db')

def get_db_conn():
    """Get a database connection from pool (PostgreSQL) or thread-local (SQLite)"""
    if USE_POSTGRES:
        if db_pool is None:
            raise Exception("PostgreSQL connection pool not initialized")
        try:
            conn = db_pool.getconn()
            if conn.closed:
                print("⚠️ Got closed connection from pool, reconnecting...")
                db_pool.putconn(conn, close=True)
                conn = db_pool.getconn()
            return conn
        except Exception as e:
            print(f"❌ Error getting connection from pool: {e}")
            traceback.print_exc()
            raise
    else:
        # SQLite - use thread-local connection
        if not hasattr(thread_local, 'connection') or thread_local.connection is None:
            thread_local.connection = sqlite3.connect(DB_PATH, check_same_thread=False)
            thread_local.connection.row_factory = sqlite3.Row
        return thread_local.connection

def release_db_conn(conn):
    """Release database connection back to pool"""
    if USE_POSTGRES and db_pool is not None and conn is not None:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            print(f"⚠️ Error returning connection to pool: {e}")

def init_db_pool():
    """Initialize database and create tables"""
    if USE_POSTGRES:
        print(f"🐘 Connected to PostgreSQL via connection pool")
    else:
        print(f"📂 Database file: {DB_PATH}")

    create_tables()

    # Auto-migrate tables if needed (for existing databases)
    if USE_POSTGRES:
        migrate_games_table()
        migrate_game_moves_table()

    print("✅ Database initialized successfully")

def migrate_games_table():
    """Auto-migrate games table to add any missing columns"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()

        # Check if games table exists and has room_code column
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'games'
        """)
        existing_columns = {row[0] for row in cur.fetchall()}

        if not existing_columns:
            print("📝 Games table doesn't exist yet, will be created")
            return

        # Columns that should exist
        columns_to_add = [
            ("room_code", "VARCHAR(100)"),
            ("white_player", "VARCHAR(100)"),
            ("black_player", "VARCHAR(100)"),
            ("white_user_id", "INTEGER"),
            ("black_user_id", "INTEGER"),
            ("winner", "VARCHAR(20)"),
            ("win_reason", "VARCHAR(50)"),
            ("game_mode", "VARCHAR(20)"),
            ("time_control", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("end_time", "TIMESTAMP"),
            ("move_count", "INTEGER DEFAULT 0"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]

        added_columns = []
        for col_name, col_type in columns_to_add:
            if col_name not in existing_columns:
                try:
                    cur.execute(f"ALTER TABLE games ADD COLUMN {col_name} {col_type}")
                    added_columns.append(col_name)
                except Exception as e:
                    print(f"⚠️ Could not add column {col_name}: {e}")

        if added_columns:
            conn.commit()
            print(f"✅ Auto-migrated games table, added columns: {', '.join(added_columns)}")
        else:
            print("✅ Games table schema is up to date")

    except Exception as e:
        print(f"⚠️ Auto-migration check failed: {e}")
        conn.rollback()
    finally:
        release_db_conn(conn)

def migrate_game_moves_table():
    """Auto-migrate game_moves table to add any missing columns"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()

        # Check if game_moves table exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'game_moves'
        """)
        existing_columns = {row[0] for row in cur.fetchall()}

        if not existing_columns:
            print("📝 game_moves table doesn't exist yet, will be created")
            return

        # Columns that should exist
        columns_to_add = [
            ("game_id", "INTEGER"),
            ("move_number", "INTEGER"),
            ("move_notation", "VARCHAR(20)"),
            ("from_square", "VARCHAR(10)"),
            ("to_square", "VARCHAR(10)"),
            ("position_fen", "TEXT"),
            ("white_time_remaining", "REAL"),
            ("black_time_remaining", "REAL"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]

        added_columns = []
        for col_name, col_type in columns_to_add:
            if col_name not in existing_columns:
                try:
                    cur.execute(f"ALTER TABLE game_moves ADD COLUMN {col_name} {col_type}")
                    added_columns.append(col_name)
                except Exception as e:
                    print(f"⚠️ Could not add column {col_name}: {e}")

        if added_columns:
            conn.commit()
            print(f"✅ Auto-migrated game_moves table, added columns: {', '.join(added_columns)}")
        else:
            print("✅ game_moves table schema is up to date")

    except Exception as e:
        print(f"⚠️ game_moves auto-migration check failed: {e}")
        conn.rollback()
    finally:
        release_db_conn(conn)

def create_tables():
    """Create necessary database tables if they don't exist"""
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        if USE_POSTGRES:
            # PostgreSQL table definitions
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    display_name VARCHAR(100),
                    elo_rating INTEGER DEFAULT 1200,
                    games_played INTEGER DEFAULT 0,
                    games_won INTEGER DEFAULT 0,
                    games_drawn INTEGER DEFAULT 0,
                    games_lost INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id SERIAL PRIMARY KEY,
                    room_code VARCHAR(100) NOT NULL,
                    white_player VARCHAR(100),
                    black_player VARCHAR(100),
                    white_user_id INTEGER REFERENCES users(id),
                    black_user_id INTEGER REFERENCES users(id),
                    winner VARCHAR(20),
                    win_reason VARCHAR(50),
                    game_mode VARCHAR(20),
                    time_control INTEGER,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    move_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS game_moves (
                    id SERIAL PRIMARY KEY,
                    game_id INTEGER NOT NULL REFERENCES games(id) ON DELETE CASCADE,
                    move_number INTEGER NOT NULL,
                    move_notation VARCHAR(20) NOT NULL,
                    from_square VARCHAR(10),
                    to_square VARCHAR(10),
                    position_fen TEXT,
                    white_time_remaining REAL,
                    black_time_remaining REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS visitor_count (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    count INTEGER DEFAULT 0
                )
            """)

            # Insert initial visitor count if not exists
            cur.execute("""
                INSERT INTO visitor_count (id, count) VALUES (1, 0)
                ON CONFLICT (id) DO NOTHING
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS password_reset_codes (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    email VARCHAR(255) NOT NULL,
                    code VARCHAR(10) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    used INTEGER DEFAULT 0
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS email_verification_codes (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) NOT NULL,
                    username VARCHAR(50) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    display_name VARCHAR(100),
                    code VARCHAR(10) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    verified INTEGER DEFAULT 0
                )
            """)

        else:
            # SQLite table definitions
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    display_name TEXT,
                    elo_rating INTEGER DEFAULT 1200,
                    games_played INTEGER DEFAULT 0,
                    games_won INTEGER DEFAULT 0,
                    games_drawn INTEGER DEFAULT 0,
                    games_lost INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_code TEXT NOT NULL,
                    white_player TEXT,
                    black_player TEXT,
                    white_user_id INTEGER,
                    black_user_id INTEGER,
                    winner TEXT,
                    win_reason TEXT,
                    game_mode TEXT,
                    time_control INTEGER,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    move_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (white_user_id) REFERENCES users(id),
                    FOREIGN KEY (black_user_id) REFERENCES users(id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS game_moves (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id INTEGER NOT NULL,
                    move_number INTEGER NOT NULL,
                    move_notation TEXT NOT NULL,
                    from_square TEXT,
                    to_square TEXT,
                    position_fen TEXT,
                    white_time_remaining REAL,
                    black_time_remaining REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS visitor_count (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    count INTEGER DEFAULT 0
                )
            """)

            cur.execute("INSERT OR IGNORE INTO visitor_count (id, count) VALUES (1, 0)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS password_reset_codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    email TEXT NOT NULL,
                    code TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    used INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS email_verification_codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    display_name TEXT,
                    code TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    verified INTEGER DEFAULT 0
                )
            """)

        conn.commit()
        print("✅ Database tables created/verified")

    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        traceback.print_exc()
        conn.rollback()
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

# ===== VISITOR COUNT FUNCTIONS =====

def increment_visitor_count():
    """Increment the visitor counter"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE visitor_count SET count = count + 1 WHERE id = 1")
        conn.commit()
        print("👁️ Visitor count incremented")
    except Exception as e:
        print(f"❌ Error incrementing visitor count: {e}")
        traceback.print_exc()
        conn.rollback()
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def get_total_visitor_count():
    """Get total visitor count"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT count FROM visitor_count WHERE id = 1")
        result = cur.fetchone()
        if result:
            # For both PostgreSQL and SQLite, result is a tuple when using regular cursor
            count = result[0] if isinstance(result, tuple) else result['count']
            return count
        return 0
    except Exception as e:
        print(f"❌ Error getting visitor count: {e}")
        traceback.print_exc()
        return 0
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

# ===== USER FUNCTIONS =====

def get_user_by_id(user_id):
    """Get user by ID"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            SELECT id, username, email, password_hash, display_name,
                   elo_rating, games_played, games_won, games_drawn, games_lost,
                   created_at, last_login
            FROM users WHERE id = {placeholder}
        """, (user_id,))

        row = cur.fetchone()
        if row:
            return dict(row)
        return None
    except Exception as e:
        print(f"❌ Error getting user by id: {e}")
        traceback.print_exc()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def get_user_by_username(username):
    """Get user by username"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            SELECT id, username, email, password_hash, display_name,
                   elo_rating, games_played, games_won, games_drawn, games_lost,
                   created_at, last_login
            FROM users WHERE username = {placeholder}
        """, (username,))

        row = cur.fetchone()
        if row:
            return dict(row)
        return None
    except Exception as e:
        print(f"❌ Error getting user by username: {e}")
        traceback.print_exc()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def create_user(username, email, password_hash, display_name=None):
    """Create a new user"""
    conn = get_db_conn()
    try:
        print(f"📝 Creating user: {username}, email: {email}")
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO users (username, email, password_hash, display_name)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (username, email, password_hash, display_name or username))
            result = cur.fetchone()
            user_id = result[0] if result else None
            print(f"✅ PostgreSQL INSERT returned user_id: {user_id}")
        else:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO users (username, email, password_hash, display_name)
                VALUES (?, ?, ?, ?)
            """, (username, email, password_hash, display_name or username))
            user_id = cur.lastrowid

        conn.commit()
        print(f"✅ User created successfully with ID: {user_id}")
        return user_id
    except Exception as e:
        print(f"❌ Error creating user: {e}")
        traceback.print_exc()
        conn.rollback()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def update_last_login(user_id):
    """Update user's last login time"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            UPDATE users SET last_login = CURRENT_TIMESTAMP
            WHERE id = {placeholder}
        """, (user_id,))
        conn.commit()
    except Exception as e:
        print(f"❌ Error updating last login: {e}")
        traceback.print_exc()
        conn.rollback()
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def get_user_profile(username):
    """Get user profile information"""
    return get_user_by_username(username)

def get_user_by_email(email):
    """Get user by email address"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            SELECT id, username, email, password_hash, display_name,
                   elo_rating, games_played, games_won, games_drawn, games_lost,
                   created_at, last_login
            FROM users WHERE email = {placeholder}
        """, (email.lower(),))

        row = cur.fetchone()
        if row:
            return dict(row)
        return None
    except Exception as e:
        print(f"❌ Error getting user by email: {e}")
        traceback.print_exc()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def update_user_password(user_id, new_password_hash):
    """Update user's password"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            UPDATE users SET password_hash = {placeholder}
            WHERE id = {placeholder}
        """, (new_password_hash, user_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error updating password: {e}")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def create_reset_code(user_id, email, code, expires_at):
    """Create a password reset code"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'

        # Invalidate any existing unused codes for this user
        cur.execute(f"""
            UPDATE password_reset_codes SET used = 1
            WHERE user_id = {placeholder} AND used = 0
        """, (user_id,))

        # Create new code
        cur.execute(f"""
            INSERT INTO password_reset_codes (user_id, email, code, expires_at)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})
        """, (user_id, email.lower(), code, expires_at))
        conn.commit()
        print(f"✅ Reset code created for user {user_id}")
        return True
    except Exception as e:
        print(f"❌ Error creating reset code: {e}")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def verify_reset_code(email, code):
    """Verify a password reset code and return user_id if valid"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            SELECT user_id, expires_at FROM password_reset_codes
            WHERE email = {placeholder} AND code = {placeholder} AND used = 0
            ORDER BY created_at DESC
            LIMIT 1
        """, (email.lower(), code))

        row = cur.fetchone()
        if not row:
            return None

        # Convert to dict properly
        row_dict = dict(row)

        # Check if code has expired
        expires_at = row_dict['expires_at']
        if isinstance(expires_at, str):
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')

        if datetime.utcnow() > expires_at:
            print(f"⚠️ Reset code expired for {email}")
            return None

        return row_dict['user_id']
    except Exception as e:
        print(f"❌ Error verifying reset code: {e}")
        traceback.print_exc()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def mark_reset_code_used(email, code):
    """Mark a reset code as used"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            UPDATE password_reset_codes SET used = 1
            WHERE email = {placeholder} AND code = {placeholder}
        """, (email.lower(), code))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error marking reset code as used: {e}")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

# ===== EMAIL VERIFICATION FUNCTIONS =====

def create_verification_code(email, username, password_hash, display_name, code, expires_at):
    """Create an email verification code for registration"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'

        # Remove any existing unverified codes for this email
        cur.execute(f"""
            DELETE FROM email_verification_codes
            WHERE email = {placeholder} AND verified = 0
        """, (email.lower(),))

        # Create new verification code
        cur.execute(f"""
            INSERT INTO email_verification_codes (email, username, password_hash, display_name, code, expires_at)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
        """, (email.lower(), username, password_hash, display_name, code, expires_at))
        conn.commit()
        print(f"✅ Verification code created for {email}")
        return True
    except Exception as e:
        print(f"❌ Error creating verification code: {e}")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def verify_email_code(email, code):
    """Verify email code and return registration data if valid"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            SELECT username, password_hash, display_name, expires_at
            FROM email_verification_codes
            WHERE email = {placeholder} AND code = {placeholder} AND verified = 0
            ORDER BY created_at DESC
            LIMIT 1
        """, (email.lower(), code))

        row = cur.fetchone()
        if not row:
            print(f"⚠️ No verification code found for {email} with code {code}")
            return None

        # Convert to dict properly
        row_dict = dict(row)

        # Check if code has expired
        expires_at = row_dict['expires_at']
        if isinstance(expires_at, str):
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')

        if datetime.utcnow() > expires_at:
            print(f"⚠️ Verification code expired for {email}")
            return None

        return {
            'username': row_dict['username'],
            'password_hash': row_dict['password_hash'],
            'display_name': row_dict['display_name']
        }
    except Exception as e:
        print(f"❌ Error verifying email code: {e}")
        traceback.print_exc()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def mark_email_verified(email, code):
    """Mark email verification code as verified"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            UPDATE email_verification_codes SET verified = 1
            WHERE email = {placeholder} AND code = {placeholder}
        """, (email.lower(), code))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error marking email as verified: {e}")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def check_username_exists(username):
    """Check if username already exists"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"SELECT id FROM users WHERE username = {placeholder}", (username,))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"❌ Error checking username: {e}")
        traceback.print_exc()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def check_email_exists(email):
    """Check if email already exists"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"SELECT id FROM users WHERE email = {placeholder}", (email.lower(),))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"❌ Error checking email: {e}")
        traceback.print_exc()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

# ===== GAME FUNCTIONS =====

def save_game_record(room, game_data, start_time, end_time, win_reason):
    """Save a completed game to database"""
    print(f"🎮 Saving game record for room: {room}")
    print(f"   Game data keys: {list(game_data.keys())}")
    conn = get_db_conn()
    try:
        cur = conn.cursor()

        # Extract game data
        white_player = game_data.get('white_player', 'Unknown')
        black_player = game_data.get('black_player', 'Unknown')
        white_user_id = game_data.get('white_user_id')
        black_user_id = game_data.get('black_user_id')
        winner = game_data.get('winner')
        game_mode = game_data.get('game_mode', 'friend')
        time_control = int(game_data.get('whiteTime', 300))
        move_history = game_data.get('move_history', [])

        print(f"   White: {white_player} (user_id: {white_user_id})")
        print(f"   Black: {black_player} (user_id: {black_user_id})")
        print(f"   Winner: {winner}, Reason: {win_reason}")
        print(f"   Move count: {len(move_history)}")

        # Insert game record
        if USE_POSTGRES:
            cur.execute("""
                INSERT INTO games (
                    room_code, white_player, black_player,
                    white_user_id, black_user_id, winner, win_reason,
                    game_mode, time_control, start_time, end_time, move_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                room, white_player, black_player,
                white_user_id, black_user_id, winner, win_reason,
                game_mode, time_control, start_time, end_time, len(move_history)
            ))
            result = cur.fetchone()
            game_id = result[0] if result else None
        else:
            cur.execute("""
                INSERT INTO games (
                    room_code, white_player, black_player,
                    white_user_id, black_user_id, winner, win_reason,
                    game_mode, time_control, start_time, end_time, move_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                room, white_player, black_player,
                white_user_id, black_user_id, winner, win_reason,
                game_mode, time_control, start_time, end_time, len(move_history)
            ))
            game_id = cur.lastrowid

        print(f"   Game ID: {game_id}")

        # Save move history for replay
        for i, move in enumerate(move_history):
            if USE_POSTGRES:
                cur.execute("""
                    INSERT INTO game_moves (
                        game_id, move_number, move_notation, from_square, to_square,
                        position_fen, white_time_remaining, black_time_remaining
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    game_id, i + 1, move.get('notation', ''),
                    move.get('from_square', ''), move.get('to_square', ''),
                    move.get('fen', ''),
                    move.get('white_time', 0), move.get('black_time', 0)
                ))
            else:
                cur.execute("""
                    INSERT INTO game_moves (
                        game_id, move_number, move_notation, from_square, to_square,
                        position_fen, white_time_remaining, black_time_remaining
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    game_id, i + 1, move.get('notation', ''),
                    move.get('from_square', ''), move.get('to_square', ''),
                    move.get('fen', ''),
                    move.get('white_time', 0), move.get('black_time', 0)
                ))

        # Update user statistics
        print(f"📊 Updating stats - White user: {white_user_id}, Black user: {black_user_id}, Winner: {winner}")
        if white_user_id:
            update_user_stats(cur, white_user_id, winner, 'white')
            print(f"   ✓ Updated stats for white player (user_id: {white_user_id})")
        else:
            print(f"   ⚠️ No white_user_id - stats not updated")
        if black_user_id:
            update_user_stats(cur, black_user_id, winner, 'black')
            print(f"   ✓ Updated stats for black player (user_id: {black_user_id})")
        else:
            print(f"   ⚠️ No black_user_id - stats not updated (bot game or guest)")

        conn.commit()
        print(f"✅ Game {room} saved to database (ID: {game_id})")
        return True

    except Exception as e:
        print(f"❌ Error saving game: {e}")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def update_user_stats(cur, user_id, winner, player_color):
    """Update user statistics after game"""
    placeholder = '%s' if USE_POSTGRES else '?'

    try:
        if winner == 'draw':
            cur.execute(f"""
                UPDATE users
                SET games_played = games_played + 1,
                    games_drawn = games_drawn + 1
                WHERE id = {placeholder}
            """, (user_id,))
            print(f"      → Draw recorded for user {user_id}")
        elif winner == player_color:
            # Player won
            cur.execute(f"""
                UPDATE users
                SET games_played = games_played + 1,
                    games_won = games_won + 1,
                    elo_rating = elo_rating + 20
                WHERE id = {placeholder}
            """, (user_id,))
            print(f"      → Win recorded for user {user_id} (+20 ELO)")
        else:
            # Player lost
            if USE_POSTGRES:
                cur.execute(f"""
                    UPDATE users
                    SET games_played = games_played + 1,
                        games_lost = games_lost + 1,
                        elo_rating = GREATEST(elo_rating - 15, 800)
                    WHERE id = {placeholder}
                """, (user_id,))
            else:
                # SQLite doesn't have GREATEST, use MAX
                cur.execute(f"""
                    UPDATE users
                    SET games_played = games_played + 1,
                        games_lost = games_lost + 1,
                        elo_rating = MAX(elo_rating - 15, 800)
                    WHERE id = {placeholder}
                """, (user_id,))
            print(f"      → Loss recorded for user {user_id} (-15 ELO)")
    except Exception as e:
        print(f"❌ Error in update_user_stats: {e}")
        traceback.print_exc()
        raise

def get_user_games(username):
    """Get game history for a user"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        # Get user ID
        user = get_user_by_username(username)
        if not user:
            return None

        user_id = user['id']
        placeholder = '%s' if USE_POSTGRES else '?'

        # Get games
        cur.execute(f"""
            SELECT id, room_code, white_player, black_player,
                   winner, win_reason, game_mode, time_control,
                   start_time, end_time, move_count,
                   white_user_id, black_user_id
            FROM games
            WHERE white_user_id = {placeholder} OR black_user_id = {placeholder}
            ORDER BY end_time DESC
            LIMIT 50
        """, (user_id, user_id))

        games_list = []
        for row in cur.fetchall():
            row_dict = dict(row)
            games_list.append({
                'id': row_dict['id'],
                'room_code': row_dict['room_code'],
                'white_player': row_dict['white_player'],
                'black_player': row_dict['black_player'],
                'winner': row_dict['winner'],
                'win_reason': row_dict['win_reason'],
                'game_mode': row_dict['game_mode'],
                'time_control': row_dict['time_control'],
                'start_time': row_dict['start_time'],
                'end_time': row_dict['end_time'],
                'move_count': row_dict['move_count'],
                'white_username': username if row_dict['white_user_id'] == user_id else 'Opponent',
                'black_username': username if row_dict['black_user_id'] == user_id else 'Opponent'
            })

        return games_list

    except Exception as e:
        print(f"❌ Error getting user games: {e}")
        traceback.print_exc()
        return []
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def get_game_replay(game_id):
    """Get game replay data including all moves"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'

        # Get game info
        cur.execute(f"""
            SELECT room_code, white_player, black_player, winner, win_reason,
                   game_mode, time_control, start_time, end_time
            FROM games WHERE id = {placeholder}
        """, (game_id,))

        game_row = cur.fetchone()
        if not game_row:
            return None

        game_dict = dict(game_row)

        # Get moves
        cur.execute(f"""
            SELECT move_number, move_notation, from_square, to_square,
                   position_fen, white_time_remaining, black_time_remaining
            FROM game_moves
            WHERE game_id = {placeholder}
            ORDER BY move_number
        """, (game_id,))

        moves = []
        for row in cur.fetchall():
            row_dict = dict(row)
            moves.append({
                'move_number': row_dict['move_number'],
                'move_notation': row_dict['move_notation'],
                'from_square': row_dict['from_square'],
                'to_square': row_dict['to_square'],
                'position_fen': row_dict['position_fen'],
                'white_time': row_dict['white_time_remaining'],
                'black_time': row_dict['black_time_remaining']
            })

        return {
            'game': {
                'id': game_id,
                'room_code': game_dict['room_code'],
                'white_player': game_dict['white_player'],
                'black_player': game_dict['black_player'],
                'winner': game_dict['winner'],
                'win_reason': game_dict['win_reason'],
                'game_mode': game_dict['game_mode'],
                'time_control': game_dict['time_control'],
                'start_time': game_dict['start_time'],
                'end_time': game_dict['end_time']
            },
            'moves': moves
        }

    except Exception as e:
        print(f"❌ Error getting game replay: {e}")
        traceback.print_exc()
        return None
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)

def get_leaderboard_data(limit=10):
    """Get top players by ELO rating"""
    conn = get_db_conn()
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        placeholder = '%s' if USE_POSTGRES else '?'
        cur.execute(f"""
            SELECT username, display_name, elo_rating,
                   games_played, games_won, games_drawn, games_lost
            FROM users
            WHERE games_played > 0
            ORDER BY elo_rating DESC
            LIMIT {placeholder}
        """, (limit,))

        leaderboard = []
        for row in cur.fetchall():
            row_dict = dict(row)
            leaderboard.append({
                'username': row_dict['username'],
                'display_name': row_dict['display_name'],
                'elo_rating': row_dict['elo_rating'],
                'games_played': row_dict['games_played'],
                'games_won': row_dict['games_won'],
                'games_drawn': row_dict['games_drawn'],
                'games_lost': row_dict['games_lost']
            })

        return leaderboard

    except Exception as e:
        print(f"❌ Error getting leaderboard: {e}")
        traceback.print_exc()
        return []
    finally:
        if USE_POSTGRES:
            release_db_conn(conn)
