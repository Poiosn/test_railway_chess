from flask import Flask, render_template, request, jsonify, session
from flask_socketio import SocketIO, emit, join_room, leave_room
import chess
import chess.engine
import time
import random
import secrets
import threading
import os
from datetime import datetime, timedelta
import shutil
import hashlib
import hmac
import json

# Import database functions
from database import (
    init_db_pool, get_db_conn, release_db_conn,
    increment_visitor_count, get_total_visitor_count,
    get_leaderboard_data, save_game_record,
    get_user_by_id, get_user_by_username, get_user_by_email, create_user,
    update_last_login, update_user_password, get_user_profile, get_user_games,
    get_game_replay, create_reset_code, verify_reset_code, mark_reset_code_used,
    create_verification_code, verify_email_code, mark_email_verified,
    check_username_exists, check_email_exists
)

# Email imports - using HTTP API (Resend) for Railway compatibility
import urllib.request
import urllib.error

# ===== FLASK APP =====
app = Flask(__name__)
app.config["SECRET_KEY"] = secrets.token_hex(16)
app.config["SESSION_COOKIE_SECURE"] = False  # Set to True in production with HTTPS
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ===== EMAIL CONFIGURATION =====
# Using Brevo (Sendinblue) API - works on Railway, no domain verification required
# Get your API key from https://app.brevo.com/settings/keys/api (free tier: 300 emails/day)
EMAIL_CONFIG = {
    'brevo_api_key': os.environ.get('BREVO_API_KEY', ''),  # Required: your Brevo API key
    'sender_email': os.environ.get('SENDER_EMAIL', 'ssswapnil250@gmail.com'),  # Your email (must match Brevo account)
    'sender_name': os.environ.get('SENDER_NAME', 'Chess Master'),
    'enabled': os.environ.get('EMAIL_ENABLED', 'true').lower() == 'true'
}

def _send_email_worker(to_email, subject, text_content, html_content):
    """Background worker to send email via Brevo API"""
    print(f"📧 Starting email send to {to_email}...")

    api_key = EMAIL_CONFIG['brevo_api_key']

    if not api_key:
        print(f"⚠️ BREVO_API_KEY not set. Email not sent.")
        print(f"📧 [DEV MODE] Would send to {to_email}: {subject}")
        return False

    try:
        # Prepare request data for Brevo API
        data = json.dumps({
            "sender": {
                "name": EMAIL_CONFIG['sender_name'],
                "email": EMAIL_CONFIG['sender_email']
            },
            "to": [{"email": to_email}],
            "subject": subject,
            "htmlContent": html_content,
            "textContent": text_content
        }).encode('utf-8')

        # Create request
        req = urllib.request.Request(
            'https://api.brevo.com/v3/smtp/email',
            data=data,
            headers={
                'api-key': api_key,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            method='POST'
        )

        print(f"📧 Sending via Brevo API...")

        # Send request
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            print(f"✅ Email sent to {to_email} via Brevo (ID: {result.get('messageId', 'unknown')})")
            return True

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else 'No details'
        print(f"❌ Brevo API error ({e.code}): {error_body}")
        return False
    except urllib.error.URLError as e:
        print(f"❌ Network error: {e.reason}")
        return False
    except Exception as e:
        print(f"❌ Failed to send email to {to_email}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

def send_email_async(to_email, subject, text_content, html_content):
    """Send email in background thread to avoid blocking API response"""
    email_thread = threading.Thread(
        target=_send_email_worker,
        args=(to_email, subject, text_content, html_content),
        daemon=True
    )
    email_thread.start()
    print(f"📧 Email queued for {to_email}")
    return True

def send_reset_code_email(to_email, username, code):
    """Send password reset code via email"""
    if not EMAIL_CONFIG['enabled']:
        print(f"📧 [DEV MODE] Reset code for {username}: {code}")
        return True

    subject = 'Chess Master - Password Reset Code'

    text = f"""
Chess Master - Password Reset

Hi {username},

You requested a password reset for your Chess Master account.

Your reset code is: {code}

This code will expire in 15 minutes.

If you didn't request this, please ignore this email.

- Chess Master Team
    """

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px;">
        <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 20px; padding: 30px; box-shadow: 0 10px 40px rgba(0,0,0,0.2);">
            <h1 style="text-align: center; color: #667eea;">Chess Master</h1>
            <h2 style="text-align: center; color: #333;">Password Reset</h2>
            <p style="color: #666;">Hi <strong>{username}</strong>,</p>
            <p style="color: #666;">You requested a password reset for your Chess Master account.</p>
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 12px; padding: 20px; text-align: center; margin: 20px 0;">
                <p style="color: white; margin: 0; font-size: 14px;">Your reset code is:</p>
                <h1 style="color: white; margin: 10px 0; letter-spacing: 8px; font-size: 32px;">{code}</h1>
            </div>
            <p style="color: #999; font-size: 12px; text-align: center;">This code will expire in 15 minutes.</p>
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
            <p style="color: #999; font-size: 11px; text-align: center;">If you didn't request this, please ignore this email.</p>
        </div>
    </body>
    </html>
    """

    # Send asynchronously to avoid blocking
    return send_email_async(to_email, subject, text, html)

def generate_reset_code():
    """Generate a 6-digit reset code"""
    return ''.join([str(random.randint(0, 9)) for _ in range(6)])

def send_verification_code_email(to_email, username, code):
    """Send email verification code for registration"""
    if not EMAIL_CONFIG['enabled']:
        print(f"📧 [DEV MODE] Verification code for {username}: {code}")
        return True

    subject = 'Chess Master - Verify Your Email'

    text = f"""
Chess Master - Email Verification

Hi {username},

Welcome to Chess Master! Please verify your email to complete registration.

Your verification code is: {code}

This code will expire in 15 minutes.

If you didn't create this account, please ignore this email.

- Chess Master Team
    """

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px;">
        <div style="max-width: 500px; margin: 0 auto; background: white; border-radius: 20px; padding: 30px; box-shadow: 0 10px 40px rgba(0,0,0,0.2);">
            <h1 style="text-align: center; color: #667eea;">Chess Master</h1>
            <h2 style="text-align: center; color: #333;">Welcome, {username}!</h2>
            <p style="color: #666;">Please verify your email to complete your registration.</p>
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 12px; padding: 20px; text-align: center; margin: 20px 0;">
                <p style="color: white; margin: 0; font-size: 14px;">Your verification code is:</p>
                <h1 style="color: white; margin: 10px 0; letter-spacing: 8px; font-size: 32px;">{code}</h1>
            </div>
            <p style="color: #999; font-size: 12px; text-align: center;">This code will expire in 15 minutes.</p>
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
            <p style="color: #999; font-size: 11px; text-align: center;">If you didn't create this account, please ignore this email.</p>
        </div>
    </body>
    </html>
    """

    # Send asynchronously to avoid blocking
    return send_email_async(to_email, subject, text, html)

# ===== AUTHENTICATION HELPERS =====
def hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, password_hash):
    """Verify password against hash"""
    return hmac.compare_digest(hash_password(password), password_hash)

def get_current_user():
    """Get current logged in user from session (for HTTP routes only)"""
    try:
        user_id = session.get('user_id')
        print(f"🔍 get_current_user() - session user_id: {user_id}")
        if not user_id:
            return None
        user = get_user_by_id(user_id)
        print(f"🔍 get_current_user() - found user: {user['username'] if user else None}")
        return user
    except Exception as e:
        print(f"⚠️ get_current_user() error: {e}")
        return None

def get_socketio_user(sid=None):
    """Get user for a SocketIO session - use this in SocketIO event handlers"""
    if sid is None:
        sid = request.sid

    # First try from our sid_to_user cache
    if sid in sid_to_user:
        user_info = sid_to_user[sid]
        print(f"🔍 get_socketio_user({sid}) - from cache: {user_info}")
        return user_info

    # Fall back to Flask session (may work in some cases)
    try:
        user_id = session.get('user_id')
        if user_id:
            user = get_user_by_id(user_id)
            if user:
                # Cache it for future use
                sid_to_user[sid] = {'id': user['id'], 'username': user['username']}
                print(f"🔍 get_socketio_user({sid}) - from session: {user['username']}")
                return sid_to_user[sid]
    except Exception as e:
        print(f"⚠️ get_socketio_user() session fallback error: {e}")

    print(f"🔍 get_socketio_user({sid}) - no user found")
    return None

# Initialize database on startup
init_db_pool()

# ===== STOCKFISH SETUP =====
# Try to find Stockfish in multiple locations
STOCKFISH_PATH = None
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
stockfish_paths = [
    os.path.join(BASE_DIR, "stockfish"),  # Project directory (first priority)
    os.path.expanduser("~/.local/bin/stockfish"),  # User local bin
    shutil.which("stockfish"),
    "/opt/homebrew/bin/stockfish",  # Homebrew on Apple Silicon
    "/usr/local/bin/stockfish",      # Homebrew on Intel Mac
    "/usr/games/stockfish",          # Linux
    "/usr/bin/stockfish",            # Linux
]

for path in stockfish_paths:
    if path and os.path.exists(path):
        STOCKFISH_PATH = path
        break

if STOCKFISH_PATH:
    print(f"✅ Stockfish Engine Found: {STOCKFISH_PATH}")
else:
    print("⚠️ Stockfish not found! Bot will use random moves.")
    print("📥 Install Stockfish:")
    print("   macOS: brew install stockfish")
    print("   Linux: sudo apt-get install stockfish")
    print("   Or download from: https://stockfishchess.org/download/")

games = {}
sid_to_room = {}
sid_to_user = {}  # Maps socket ID to user info for SocketIO contexts
DISCONNECT_TIMEOUT = 15.0

# ===== GLOBAL MATCHMAKING QUEUE =====
matchmaking_queue = []
matchmaking_lock = threading.Lock()

# --- ROUTES ---
@app.route("/")
def index():
    increment_visitor_count()
    return render_template("landing.html")

@app.route("/game")
def game_page():
    return render_template("game.html")

@app.route("/profile")
def profile_page():
    return render_template("profile.html")

# --- API ENDPOINTS ---
@app.route('/api/visitor-count')
def visitor_count_api():
    count = get_total_visitor_count()
    return jsonify({'visitor_count': count})

@app.route('/api/leaderboard')
def leaderboard_api():
    data = get_leaderboard_data(limit=5)
    return jsonify(data)

@app.route('/api/active-games')
def active_games_api():
    """Returns list of active games that can be spectated"""
    active = []
    for room, g in games.items():
        if g.get("isActive") and not g["winner"] and not g.get("bot"):
            active.append({
                "room": room,
                "whiteName": g.get("white_player", "White"),
                "blackName": g.get("black_player", "Black"),
                "spectators": len(g.get("spectators", set())),
                "gameMode": g.get("game_mode", "friend")
            })
    return jsonify(active)

# ===== AUTHENTICATION ENDPOINTS =====

@app.route('/api/auth/register', methods=['POST'])
def register():
    """Step 1: Send verification code to email"""
    data = request.get_json()
    username = data.get('username', '').strip()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    display_name = data.get('displayName', username).strip()

    if not username or not email or not password:
        return jsonify({'error': 'Missing required fields'}), 400

    if len(username) < 3 or len(username) > 50:
        return jsonify({'error': 'Username must be 3-50 characters'}), 400

    if len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400

    # Check if username already exists
    if check_username_exists(username):
        return jsonify({'error': 'Username already exists'}), 409

    # Check if email already exists
    if check_email_exists(email):
        return jsonify({'error': 'Email already registered'}), 409

    # Generate verification code
    code = generate_reset_code()
    expires_at = (datetime.utcnow() + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')

    # Store registration data with verification code
    password_hash = hash_password(password)
    if not create_verification_code(email, username, password_hash, display_name, code, expires_at):
        return jsonify({'error': 'Failed to create verification code'}), 500

    # Send verification email
    send_verification_code_email(email, username, code)

    return jsonify({
        'message': 'Verification code sent to your email',
        'email': email
    }), 200

@app.route('/api/auth/verify-registration', methods=['POST'])
def verify_registration():
    """Step 2: Verify code and create account"""
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    code = data.get('code', '').strip()

    if not email or not code:
        return jsonify({'error': 'Email and code are required'}), 400

    # Verify code and get registration data
    reg_data = verify_email_code(email, code)

    if not reg_data:
        return jsonify({'error': 'Invalid or expired code'}), 400

    # Check again if username/email was taken in the meantime
    if check_username_exists(reg_data['username']):
        return jsonify({'error': 'Username already taken'}), 409

    if check_email_exists(email):
        return jsonify({'error': 'Email already registered'}), 409

    # Create user
    user_id = create_user(
        reg_data['username'],
        email,
        reg_data['password_hash'],
        reg_data['display_name']
    )

    if not user_id:
        return jsonify({'error': 'Registration failed'}), 500

    # Mark verification code as used
    mark_email_verified(email, code)

    # Log user in
    session['user_id'] = user_id
    session['username'] = reg_data['username']

    return jsonify({
        'message': 'Registration successful',
        'user': {
            'id': user_id,
            'username': reg_data['username'],
            'displayName': reg_data['display_name']
        }
    }), 201

@app.route('/api/auth/resend-verification', methods=['POST'])
def resend_verification():
    """Resend verification code"""
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    username = data.get('username', '').strip()
    password = data.get('password', '')
    display_name = data.get('displayName', username).strip()

    if not email or not username or not password:
        return jsonify({'error': 'Missing required fields'}), 400

    # Generate new code
    code = generate_reset_code()
    expires_at = (datetime.utcnow() + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')

    # Store new verification code
    password_hash = hash_password(password)
    if not create_verification_code(email, username, password_hash, display_name, code, expires_at):
        return jsonify({'error': 'Failed to create verification code'}), 500

    # Send verification email
    send_verification_code_email(email, username, code)

    return jsonify({
        'message': 'Verification code resent'
    }), 200

@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login user"""
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')
    
    if not username or not password:
        return jsonify({'error': 'Missing username or password'}), 400
    
    user = get_user_by_username(username)
    
    if not user or not verify_password(password, user['password_hash']):
        return jsonify({'error': 'Invalid username or password'}), 401
    
    # Update last login
    update_last_login(user['id'])
    
    # Set session
    session['user_id'] = user['id']
    session['username'] = user['username']
    
    return jsonify({
        'message': 'Login successful',
        'user': {
            'id': user['id'],
            'username': user['username'],
            'email': user['email'],
            'displayName': user['display_name'],
            'eloRating': user['elo_rating'],
            'gamesPlayed': user['games_played'],
            'gamesWon': user['games_won'],
            'gamesDrawn': user['games_drawn'],
            'gamesLost': user['games_lost']
        }
    }), 200

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout user"""
    session.clear()
    return jsonify({'message': 'Logged out successfully'}), 200

@app.route('/api/auth/me')
def get_me():
    """Get current user info"""
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Not authenticated'}), 401
    return jsonify({'user': user}), 200

@app.route('/api/auth/forgot-username', methods=['POST'])
def forgot_username():
    """Find username by email and send reset code"""
    data = request.get_json()
    email = data.get('email', '').strip().lower()

    if not email:
        return jsonify({'error': 'Email is required'}), 400

    user = get_user_by_email(email)

    if not user:
        return jsonify({'error': 'No account found with this email'}), 404

    # Generate and store reset code
    code = generate_reset_code()
    expires_at = (datetime.utcnow() + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')

    if not create_reset_code(user['id'], email, code, expires_at):
        return jsonify({'error': 'Failed to generate reset code'}), 500

    # Send email with code
    send_reset_code_email(email, user['username'], code)

    return jsonify({
        'message': 'Reset code sent to your email',
        'username': user['username']
    }), 200

@app.route('/api/auth/verify-reset-code', methods=['POST'])
def verify_code():
    """Verify the reset code entered by user"""
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    code = data.get('code', '').strip()

    if not email or not code:
        return jsonify({'error': 'Email and code are required'}), 400

    user_id = verify_reset_code(email, code)

    if not user_id:
        return jsonify({'error': 'Invalid or expired code'}), 400

    return jsonify({'message': 'Code verified successfully'}), 200

@app.route('/api/auth/reset-password', methods=['POST'])
def reset_password():
    """Reset password after code verification"""
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    code = data.get('code', '').strip()
    new_password = data.get('newPassword', '')

    if not email or not code or not new_password:
        return jsonify({'error': 'Email, code, and new password are required'}), 400

    if len(new_password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400

    # Verify code again
    user_id = verify_reset_code(email, code)

    if not user_id:
        return jsonify({'error': 'Invalid or expired code'}), 400

    # Update password
    new_password_hash = hash_password(new_password)
    success = update_user_password(user_id, new_password_hash)

    if success:
        # Mark code as used
        mark_reset_code_used(email, code)
        return jsonify({'message': 'Password reset successful'}), 200
    else:
        return jsonify({'error': 'Failed to reset password'}), 500

@app.route('/api/debug/recent-games')
def debug_recent_games():
    """Debug endpoint to see recent games and user_ids"""
    conn = get_db_conn()
    try:
        if os.environ.get('DATABASE_URL'):
            from psycopg2.extras import RealDictCursor
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        cur.execute("""
            SELECT id, room_code, white_player, black_player,
                   white_user_id, black_user_id, winner, win_reason,
                   game_mode, created_at
            FROM games
            ORDER BY created_at DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        recent_games = [dict(row) for row in rows]

        # Convert datetime to string for JSON serialization
        for game in recent_games:
            if game.get('created_at'):
                game['created_at'] = str(game['created_at'])

        return jsonify({'recent_games': recent_games, 'count': len(recent_games)}), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/test-db-write')
def debug_test_db_write():
    """Test if we can write to the database"""
    conn = get_db_conn()
    try:
        cur = conn.cursor()

        # Try to insert a test game record
        if os.environ.get('DATABASE_URL'):
            cur.execute("""
                INSERT INTO games (
                    room_code, white_player, black_player,
                    winner, win_reason, game_mode, time_control, move_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, ('test-debug-room', 'TestWhite', 'TestBlack', 'white', 'test', 'debug', 300, 0))
            result = cur.fetchone()
            game_id = result[0] if result else None
        else:
            cur.execute("""
                INSERT INTO games (
                    room_code, white_player, black_player,
                    winner, win_reason, game_mode, time_control, move_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, ('test-debug-room', 'TestWhite', 'TestBlack', 'white', 'test', 'debug', 300, 0))
            game_id = cur.lastrowid

        conn.commit()
        print(f"✅ Debug test: Successfully inserted game with ID: {game_id}")

        return jsonify({
            'success': True,
            'message': f'Test game inserted with ID: {game_id}',
            'game_id': game_id
        }), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/debug/active-rooms')
def debug_active_rooms():
    """Show all active game rooms and their state"""
    active_rooms = []
    for room, g in games.items():
        active_rooms.append({
            'room': room,
            'white_player': g.get('white_player'),
            'black_player': g.get('black_player'),
            'white_user_id': g.get('white_user_id'),
            'black_user_id': g.get('black_user_id'),
            'winner': g.get('winner'),
            'saved': g.get('saved', False),
            'isActive': g.get('isActive'),
            'game_mode': g.get('game_mode'),
            'move_count': len(g.get('move_history', []))
        })
    return jsonify({'active_rooms': active_rooms, 'count': len(active_rooms)}), 200

@app.route('/api/debug/migrate-games-table')
def debug_migrate_games_table():
    """Migrate games table to add missing columns - run this once on Railway"""
    if not os.environ.get('DATABASE_URL'):
        return jsonify({'error': 'This endpoint is only for PostgreSQL'}), 400

    conn = get_db_conn()
    try:
        cur = conn.cursor()
        migrations = []

        # Check and add missing columns to games table
        columns_to_add = [
            ("room_code", "VARCHAR(100)"),
            ("white_player", "VARCHAR(100)"),
            ("black_player", "VARCHAR(100)"),
            ("white_user_id", "INTEGER REFERENCES users(id)"),
            ("black_user_id", "INTEGER REFERENCES users(id)"),
            ("winner", "VARCHAR(20)"),
            ("win_reason", "VARCHAR(50)"),
            ("game_mode", "VARCHAR(20)"),
            ("time_control", "INTEGER"),
            ("start_time", "TIMESTAMP"),
            ("end_time", "TIMESTAMP"),
            ("move_count", "INTEGER DEFAULT 0"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]

        for col_name, col_type in columns_to_add:
            try:
                cur.execute(f"""
                    ALTER TABLE games ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """)
                migrations.append(f"Added/verified column: {col_name}")
            except Exception as e:
                migrations.append(f"Column {col_name}: {str(e)}")

        conn.commit()

        # Also verify the table structure
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'games'
            ORDER BY ordinal_position
        """)
        columns = [{'name': row[0], 'type': row[1]} for row in cur.fetchall()]

        return jsonify({
            'success': True,
            'migrations': migrations,
            'current_columns': columns
        }), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        if os.environ.get('DATABASE_URL'):
            from database import release_db_conn
            release_db_conn(conn)

@app.route('/api/user/<username>')
def get_user_profile_api(username):
    """Get user profile by username"""
    user = get_user_profile(username)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    return jsonify({'user': user}), 200

@app.route('/api/user/<username>/games')
def get_user_games_api(username):
    """Get user's game history"""
    games = get_user_games(username)
    if games is None:
        return jsonify({'error': 'User not found'}), 404
    return jsonify({'games': games}), 200

@app.route('/api/game/<int:game_id>/replay')
def get_game_replay_api(game_id):
    """Get game moves for replay"""
    replay_data = get_game_replay(game_id)
    if not replay_data:
        return jsonify({'error': 'Game not found'}), 404
    return jsonify(replay_data), 200

# --- SOCKETIO CONNECTION HANDLERS ---
@socketio.on("connect")
def on_connect():
    """Store user info when SocketIO connection is established"""
    sid = request.sid
    try:
        user_id = session.get('user_id')
        if user_id:
            user = get_user_by_id(user_id)
            if user:
                sid_to_user[sid] = {'id': user['id'], 'username': user['username']}
                print(f"🔗 SocketIO connected: {sid} -> user: {user['username']} (id: {user['id']})")
            else:
                print(f"🔗 SocketIO connected: {sid} -> user_id {user_id} not found in DB")
        else:
            print(f"🔗 SocketIO connected: {sid} -> guest (no session)")
    except Exception as e:
        print(f"⚠️ on_connect error: {e}")

@socketio.on("authenticate")
def on_authenticate(data):
    """Explicit authentication event - client sends user_id after connecting"""
    sid = request.sid
    user_id = data.get('user_id')
    if user_id:
        # Convert to int if string
        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            print(f"❌ Invalid user_id format: {user_id}")
            emit("authenticated", {"success": False})
            return

        user = get_user_by_id(user_id)
        if user:
            sid_to_user[sid] = {'id': user['id'], 'username': user['username']}
            print(f"✅ SocketIO authenticated: {sid} -> user: {user['username']} (id: {user['id']})")
            emit("authenticated", {"success": True, "username": user['username']})

            # If already in a game, update the user_id linkage
            room = sid_to_room.get(sid)
            if room and room in games:
                g = games[room]
                if sid == g.get("white_sid") and not g.get("white_user_id"):
                    g["white_user_id"] = user['id']
                    print(f"🔗 Late-linked white player to user_id: {user['id']}")
                elif sid == g.get("black_sid") and not g.get("black_user_id"):
                    g["black_user_id"] = user['id']
                    print(f"🔗 Late-linked black player to user_id: {user['id']}")
            return
    print(f"❌ SocketIO authentication failed for {sid}")
    emit("authenticated", {"success": False})

# --- CHESS LOGIC ---
def format_seconds(sec):
    total = int(max(0, round(sec)))
    return f"{total//60}:{total%60:02d}"

def board_to_matrix(board):
    grid = [["." for _ in range(8)] for _ in range(8)]
    for sq, piece in board.piece_map().items():
        grid[7 - chess.square_rank(sq)][chess.square_file(sq)] = piece.symbol()
    return grid

def get_legal_moves_map(board):
    """Pre-calculates all legal moves mapped by starting square (row,col)"""
    moves = {}
    for move in board.legal_moves:
        r_from = 7 - chess.square_rank(move.from_square)
        c_from = chess.square_file(move.from_square)
        r_to = 7 - chess.square_rank(move.to_square)
        c_to = chess.square_file(move.to_square)
        
        key = f"{r_from},{c_from}"
        if key not in moves:
            moves[key] = []
        moves[key].append({"row": r_to, "col": c_to})
    return moves

def check_game_over(board):
    """
    Check if the game is over and return (winner, reason) tuple.
    Returns (None, None) if game is not over.
    """
    if board.is_checkmate():
        winner = "white" if not board.turn else "black"
        return (winner, "checkmate")

    if board.is_stalemate():
        return ("draw", "stalemate")

    if board.is_insufficient_material():
        return ("draw", "insufficient")

    # Threefold repetition - automatic draw
    if board.is_repetition(3):
        return ("draw", "repetition")

    # Fifty-move rule
    if board.is_fifty_moves():
        return ("draw", "fifty_moves")

    return (None, None)

def export_state(room, current_sid=None):
    g = games[room]
    state = {
        "board": board_to_matrix(g["board"]),
        "turn": "white" if g["board"].turn else "black",
        "check": g["board"].is_check(),
        "winner": g["winner"],
        "reason": g.get("reason"),
        "isActive": g.get("isActive", False),
        "whiteTime": g["whiteTime"], 
        "blackTime": g["blackTime"],
        "whiteTimeFormatted": format_seconds(g["whiteTime"]),
        "blackTimeFormatted": format_seconds(g["blackTime"]),
        "moves": get_legal_moves_map(g["board"]),
        "whiteName": g["white_player"],
        "blackName": g["black_player"],
        "gameMode": g.get("game_mode", "friend")
    }
    
    if current_sid:
        if current_sid == g.get("white_sid"):
            state["opponentName"] = g["black_player"]
        elif current_sid == g.get("black_sid"):
            state["opponentName"] = g["white_player"]
    
    return state

def update_time(g):
    if not g.get("isActive") or g["winner"]: return
    now = time.time()
    elapsed = now - g["lastUpdate"]
    g["lastUpdate"] = now
    
    if g["board"].turn:
        g["whiteTime"] = max(0, g["whiteTime"] - elapsed)
        if g["whiteTime"] <= 0: 
            g["winner"] = "black"
            g["reason"] = "timeout"
    else:
        g["blackTime"] = max(0, g["blackTime"] - elapsed)
        if g["blackTime"] <= 0: 
            g["winner"] = "white"
            g["reason"] = "timeout"

def save_game(room, g):
    """Save game using database.py function"""
    if g.get("saved"):
        print(f"⏭️ Game {room} already saved, skipping")
        return

    end_time = datetime.utcnow()
    start_time = g.get("start_timestamp", end_time)
    win_reason = g.get("reason", "unknown")

    print(f"💾 save_game() called for room: {room}")
    print(f"   white_user_id: {g.get('white_user_id')}, black_user_id: {g.get('black_user_id')}")
    print(f"   winner: {g.get('winner')}, reason: {win_reason}")
    print(f"   game_mode: {g.get('game_mode')}, move_count: {len(g.get('move_history', []))}")

    try:
        success = save_game_record(room, g, start_time, end_time, win_reason)
        if success:
            g["saved"] = True
            print(f"✅ Game {room} saved successfully")
        else:
            print(f"❌ save_game_record returned False for room {room}")
    except Exception as e:
        print(f"❌ Exception in save_game for room {room}: {e}")
        import traceback
        traceback.print_exc()

def timeout_watcher():
    while True:
        time.sleep(1)
        for r, g in list(games.items()):
            if g.get("isActive") and not g["winner"]:
                with g["lock"]:
                    update_time(g)
                    if g["winner"]:
                        save_game(r, g)
                        for sid in g.get("clients", set()):
                            socketio.emit("game_update", {"state": export_state(r, sid)}, room=sid)

timeout_thread = threading.Thread(target=timeout_watcher, daemon=True)
timeout_thread.start()

def handle_disconnect_timeout(room, color):
    if room not in games: return
    g = games[room]
    
    with g["lock"]:
        if g["winner"]: return
        if color == "white" and g.get("white_disconnect_timer"):
            g["winner"] = "black"
            g["reason"] = "abandonment"
            g["white_disconnect_timer"] = None
        elif color == "black" and g.get("black_disconnect_timer"):
            g["winner"] = "white"
            g["reason"] = "abandonment"
            g["black_disconnect_timer"] = None
        else:
            return 
        save_game(room, g)
        for sid in g.get("clients", set()):
            socketio.emit("game_update", {"state": export_state(room, sid)}, room=sid)

def cancel_timer(g, color):
    if color == "white" and g.get("white_disconnect_timer"):
        g["white_disconnect_timer"].cancel()
        g["white_disconnect_timer"] = None
    elif color == "black" and g.get("black_disconnect_timer"):
        g["black_disconnect_timer"].cancel()
        g["black_disconnect_timer"] = None

@socketio.on("create_room")
def create(data):
    room = data["room"]
    is_bot = data.get("bot", False)
    player_name = data.get("playerName", "White")
    bot_difficulty = data.get("difficulty", "medium")  # Get difficulty from client
    client_user_id = data.get("user_id")  # User ID passed directly from client

    creator_color = random.choice(["white", "black"]) if not is_bot else "white"

    if room in games:
        g = games[room]
        if g.get("winner") is None:
            emit("error", {"message": f"Room '{room}' is already taken!"})
            return

    white_player = player_name if creator_color == "white" else None
    black_player = player_name if creator_color == "black" else None

    if is_bot:
        black_player = f"Bot ({bot_difficulty.capitalize()})"

    # Get user ID - try client-provided user_id first, then cache, then session
    user = get_socketio_user()
    if not user and client_user_id:
        # Client provided user_id directly, verify and cache it
        db_user = get_user_by_id(client_user_id)
        if db_user:
            sid_to_user[request.sid] = {'id': db_user['id'], 'username': db_user['username']}
            user = sid_to_user[request.sid]
            print(f"✅ Cached user from client-provided user_id in create_room: {user}")

    white_user_id = user['id'] if user and creator_color == "white" else None
    black_user_id = user['id'] if user and creator_color == "black" else None
    print(f"🎮 Creating room {room} - user: {user['username'] if user else 'guest'}, white_user_id: {white_user_id}, black_user_id: {black_user_id}")

    games[room] = {
        "board": chess.Board(),
        "whiteTime": float(data.get("timeControl", 300)),
        "blackTime": float(data.get("timeControl", 300)),
        "lastUpdate": time.time(),
        "start_timestamp": datetime.utcnow(),
        "isActive": True if is_bot else False,
        "winner": None,
        "bot": is_bot,
        "bot_difficulty": bot_difficulty,  # Store difficulty level
        "lock": threading.Lock(),
        "white_player": white_player,
        "black_player": black_player,
        "white_sid": request.sid if creator_color == "white" else None,
        "black_sid": request.sid if creator_color == "black" else None,
        "white_user_id": white_user_id,
        "black_user_id": black_user_id,
        "white_disconnect_timer": None,
        "black_disconnect_timer": None,
        "clients": {request.sid},
        "game_mode": "bot" if is_bot else "friend",
        "move_history": []
    }
    
    sid_to_room[request.sid] = room
    join_room(room)
    emit("room_created", {
        "color": creator_color, 
        "state": export_state(room, request.sid), 
        "room": room
    })

@socketio.on("join_room")
def join(data):
    room = data["room"]
    player_name = data.get("playerName", "Black")
    spectate = data.get("spectate", False)  # NEW: Check if joining as spectator
    client_user_id = data.get("user_id")  # User ID passed directly from client

    if room not in games:
        emit("error", {"message": "Room not found"})
        return

    # Cache user from client-provided user_id if not already cached
    if client_user_id and request.sid not in sid_to_user:
        db_user = get_user_by_id(client_user_id)
        if db_user:
            sid_to_user[request.sid] = {'id': db_user['id'], 'username': db_user['username']}
            print(f"✅ Cached user from client-provided user_id in join_room: {sid_to_user[request.sid]}")

    g = games[room]

    reconnected = False

    # Check for player reconnection
    if request.sid == g.get("white_sid"):
        reconnected = True
        cancel_timer(g, "white")
        socketio.emit("player_reconnected", {"color": "white"}, room=room)

    elif request.sid == g.get("black_sid"):
        reconnected = True
        cancel_timer(g, "black")
        socketio.emit("player_reconnected", {"color": "black"}, room=room)

    elif player_name == g["white_player"] and g.get("white_sid") is None:
        g["white_sid"] = request.sid
        cancel_timer(g, "white")
        reconnected = True
        # Link user if authenticated - use get_socketio_user for SocketIO context
        user = get_socketio_user()
        if user:
            g["white_user_id"] = user['id']
            print(f"🔗 Linked white player to user_id: {user['id']} ({user['username']})")
        # Check if both players are now connected (for global matchmaking)
        if g.get("black_sid") is not None:
            g["isActive"] = True
            g["lastUpdate"] = time.time()
        socketio.emit("player_reconnected", {"color": "white"}, room=room)

    elif g["black_player"] and player_name == g["black_player"] and g.get("black_sid") is None:
        g["black_sid"] = request.sid
        cancel_timer(g, "black")
        reconnected = True
        # Link user if authenticated - use get_socketio_user for SocketIO context
        user = get_socketio_user()
        if user:
            g["black_user_id"] = user['id']
            print(f"🔗 Linked black player to user_id: {user['id']} ({user['username']})")
        # Check if both players are now connected (for global matchmaking)
        if g.get("white_sid") is not None:
            g["isActive"] = True
            g["lastUpdate"] = time.time()
        socketio.emit("player_reconnected", {"color": "black"}, room=room)

    # NEW: Handle spectator mode
    elif spectate or (g["white_player"] and g["black_player"]):
        # Join as spectator
        if "spectators" not in g:
            g["spectators"] = set()
        g["spectators"].add(request.sid)
        
        if "clients" not in g: g["clients"] = set()
        g["clients"].add(request.sid)
        sid_to_room[request.sid] = room
        join_room(room)
        
        # Notify other players about new spectator
        spectator_count = len(g.get("spectators", set()))
        socketio.emit("spectator_joined", {
            "spectatorName": player_name,
            "spectatorCount": spectator_count
        }, room=room, skip_sid=request.sid)
        
        emit("room_joined", {
            "color": "spectator", 
            "state": export_state(room, request.sid), 
            "room": room,
            "spectatorCount": spectator_count
        })
        return

    elif not g["white_player"]:
        g["white_player"] = player_name
        g["white_sid"] = request.sid
        g["isActive"] = True
        g["lastUpdate"] = time.time()
        # Link user if authenticated - use get_socketio_user for SocketIO context
        user = get_socketio_user()
        if user:
            g["white_user_id"] = user['id']
            print(f"🔗 Linked white player to user_id: {user['id']} ({user['username']})")
    elif not g["black_player"]:
        g["black_player"] = player_name
        g["black_sid"] = request.sid
        g["isActive"] = True
        g["lastUpdate"] = time.time()
        # Link user if authenticated - use get_socketio_user for SocketIO context
        user = get_socketio_user()
        if user:
            g["black_user_id"] = user['id']
            print(f"🔗 Linked black player to user_id: {user['id']} ({user['username']})")
    else:
        if not reconnected:
            emit("error", {"message": "Room is full"})
            return

    if "clients" not in g: g["clients"] = set()
    g["clients"].add(request.sid)
    sid_to_room[request.sid] = room
    join_room(room)
    
    my_color = "white" if request.sid == g.get("white_sid") else "black"
    if request.sid != g.get("white_sid") and request.sid != g.get("black_sid"):
        my_color = "spectator"

    spectator_count = len(g.get("spectators", set()))
    emit("room_joined", {
        "color": my_color, 
        "state": export_state(room, request.sid), 
        "room": room,
        "spectatorCount": spectator_count
    })
    
    for sid in g.get("clients", set()):
        socketio.emit("game_start", {"state": export_state(room, sid)}, room=sid)

# ===== GLOBAL MATCHMAKING =====
@socketio.on("join_matchmaking")
def join_matchmaking(data):
    player_name = data.get("playerName", "Player")
    time_control = data.get("timeControl", 300)
    client_user_id = data.get("user_id")  # User ID passed from client
    sid = request.sid

    # Get user from our cache or client-provided ID
    user = get_socketio_user(sid)
    if not user and client_user_id:
        # Client provided user_id directly, verify and cache it
        db_user = get_user_by_id(client_user_id)
        if db_user:
            sid_to_user[sid] = {'id': db_user['id'], 'username': db_user['username']}
            user = sid_to_user[sid]
            print(f"✅ Cached user from client-provided user_id: {user}")

    with matchmaking_lock:
        # Check if already in queue
        if any(p["sid"] == sid for p in matchmaking_queue):
            emit("matchmaking_status", {"status": "already_in_queue"})
            return
        
        # Look for a match with same time control
        match_found = None
        for i, player in enumerate(matchmaking_queue):
            if player["timeControl"] == time_control and player["sid"] != sid:
                match_found = player
                matchmaking_queue.pop(i)
                break
        
        if match_found:
            # Create game room
            room = f"global-{secrets.token_hex(4)}"
            
            # Random color assignment
            if random.choice([True, False]):
                white_player = player_name
                white_sid = sid
                black_player = match_found["playerName"]
                black_sid = match_found["sid"]
            else:
                white_player = match_found["playerName"]
                white_sid = match_found["sid"]
                black_player = player_name
                black_sid = sid
            
            # Get user IDs if authenticated (check both possible players)
            user = get_current_user()
            # We can't determine user_ids yet since players will connect with new sids
            # Set them to None and let join_room handle user linking
            
            games[room] = {
                "board": chess.Board(),
                "whiteTime": float(time_control),
                "blackTime": float(time_control),
                "lastUpdate": time.time(),
                "start_timestamp": datetime.utcnow(),
                "isActive": False,  # Will become True when both players join
                "winner": None,
                "bot": False,
                "lock": threading.Lock(),
                "white_player": white_player,
                "black_player": black_player,
                "white_sid": None,  # Will be set when player joins
                "black_sid": None,  # Will be set when player joins
                "white_user_id": None,  # Will be set when player joins
                "black_user_id": None,  # Will be set when player joins
                "white_disconnect_timer": None,
                "black_disconnect_timer": None,
                "clients": set(),
                "game_mode": "global",
                "move_history": []
            }
            
            # Don't add to sid_to_room yet - will be done in join_room
            
            # Notify both players with their assigned names
            socketio.emit("matchmaking_found", {
                "room": room,
                "playerName": white_player,
                "color": "white"
            }, room=white_sid)
            
            socketio.emit("matchmaking_found", {
                "room": room,
                "playerName": black_player,
                "color": "black"
            }, room=black_sid)
            
            print(f"✅ Match found! Room: {room}, White: {white_player}, Black: {black_player}")
        else:
            # Add to queue
            matchmaking_queue.append({
                "sid": sid,
                "playerName": player_name,
                "timeControl": time_control,
                "timestamp": time.time()
            })
            emit("matchmaking_status", {"status": "searching"})
            print(f"🔍 Player {player_name} joined matchmaking queue (time: {time_control}s)")

@socketio.on("cancel_matchmaking")
def cancel_matchmaking():
    sid = request.sid
    with matchmaking_lock:
        for i, player in enumerate(matchmaking_queue):
            if player["sid"] == sid:
                matchmaking_queue.pop(i)
                emit("matchmaking_cancelled")
                print(f"❌ Player cancelled matchmaking")
                return

# ===== REMATCH FUNCTIONALITY =====
@socketio.on("request_rematch")
def request_rematch(data):
    room = data.get("room")
    if room not in games:
        emit("error", {"message": "Game not found"})
        return
    
    g = games[room]
    requester_sid = request.sid
    
    # Determine who is requesting
    if requester_sid == g.get("white_sid"):
        requester_color = "white"
        opponent_sid = g.get("black_sid")
    elif requester_sid == g.get("black_sid"):
        requester_color = "black"
        opponent_sid = g.get("white_sid")
    else:
        emit("error", {"message": "You are not a player in this game"})
        return
    
    # If bot game, create instant rematch
    if g.get("game_mode") == "bot":
        new_room = f"bot-{secrets.token_hex(4)}"
        time_control = g.get("whiteTime", 300) if g.get("whiteTime", 300) > 0 else 300
        player_name = g.get("white_player") if requester_color == "white" else g.get("black_player")
        bot_difficulty = g.get("bot_difficulty", "medium")  # Preserve difficulty from previous game

        # Get user ID if authenticated - use get_socketio_user for SocketIO context
        user = get_socketio_user()
        white_user_id = user['id'] if user else None
        print(f"🔄 Bot rematch - user: {user['username'] if user else 'guest'}, white_user_id: {white_user_id}")

        # Create new bot game
        games[new_room] = {
            "board": chess.Board(),
            "whiteTime": float(time_control),
            "blackTime": float(time_control),
            "lastUpdate": time.time(),
            "start_timestamp": datetime.utcnow(),
            "isActive": True,
            "winner": None,
            "bot": True,
            "bot_difficulty": bot_difficulty,  # Preserve difficulty
            "lock": threading.Lock(),
            "white_player": player_name,
            "black_player": f"Bot ({bot_difficulty.capitalize()})",
            "white_sid": requester_sid,
            "black_sid": None,
            "white_user_id": white_user_id,
            "black_user_id": None,  # Bot has no user ID
            "white_disconnect_timer": None,
            "black_disconnect_timer": None,
            "clients": {requester_sid},
            "game_mode": "bot",
            "move_history": []
        }
        
        sid_to_room[requester_sid] = new_room
        
        emit("rematch_started", {
            "room": new_room,
            "color": "white",
            "state": export_state(new_room, requester_sid)
        })
        print(f"🔄 Bot rematch created: {new_room}")
    else:
        # For multiplayer, need opponent acceptance
        if "rematch_requests" not in g:
            g["rematch_requests"] = set()
        
        g["rematch_requests"].add(requester_color)
        
        # Check if both players requested rematch
        if len(g["rematch_requests"]) == 2:
            # Create new game
            new_room = f"{g.get('game_mode', 'friend')}-{secrets.token_hex(4)}"
            time_control = g.get("whiteTime", 300) if g.get("whiteTime", 300) > 0 else 300
            
            white_sid = g.get("white_sid")
            black_sid = g.get("black_sid")
            white_player = g.get("white_player")
            black_player = g.get("black_player")
            # Preserve user IDs from previous game
            white_user_id = g.get("white_user_id")
            black_user_id = g.get("black_user_id")

            games[new_room] = {
                "board": chess.Board(),
                "whiteTime": float(time_control),
                "blackTime": float(time_control),
                "lastUpdate": time.time(),
                "start_timestamp": datetime.utcnow(),
                "isActive": True,
                "winner": None,
                "bot": False,
                "lock": threading.Lock(),
                "white_player": white_player,
                "black_player": black_player,
                "white_sid": white_sid,
                "black_sid": black_sid,
                "white_user_id": white_user_id,
                "black_user_id": black_user_id,
                "white_disconnect_timer": None,
                "black_disconnect_timer": None,
                "clients": {white_sid, black_sid},
                "game_mode": g.get("game_mode", "friend"),
                "move_history": []
            }
            
            sid_to_room[white_sid] = new_room
            sid_to_room[black_sid] = new_room
            
            # Notify both players
            socketio.emit("rematch_started", {
                "room": new_room,
                "color": "white",
                "state": export_state(new_room, white_sid)
            }, room=white_sid)
            
            socketio.emit("rematch_started", {
                "room": new_room,
                "color": "black",
                "state": export_state(new_room, black_sid)
            }, room=black_sid)
            
            print(f"🔄 Rematch created: {new_room}")
        else:
            # Notify opponent of rematch request
            if opponent_sid:
                socketio.emit("rematch_requested", {
                    "from": requester_color
                }, room=opponent_sid)

@socketio.on("decline_rematch")
def decline_rematch(data):
    room = data.get("room")
    if room not in games:
        return

    g = games[room]
    decliner_sid = request.sid

    # Determine who is declining
    if decliner_sid == g.get("white_sid"):
        decliner_color = "white"
        opponent_sid = g.get("black_sid")
    elif decliner_sid == g.get("black_sid"):
        decliner_color = "black"
        opponent_sid = g.get("white_sid")
    else:
        return

    # Clear rematch requests
    if "rematch_requests" in g:
        g["rematch_requests"].clear()

    # Notify opponent that rematch was declined
    if opponent_sid:
        socketio.emit("rematch_declined", {
            "from": decliner_color
        }, room=opponent_sid)
        print(f"❌ {decliner_color.upper()} declined rematch in room {room}")

@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    room = sid_to_room.pop(sid, None)

    # Clean up user mapping
    if sid in sid_to_user:
        user_info = sid_to_user.pop(sid)
        print(f"🔌 SocketIO disconnected: {sid} -> user: {user_info.get('username', 'unknown')}")
    else:
        print(f"🔌 SocketIO disconnected: {sid} -> guest")

    # Remove from matchmaking queue
    with matchmaking_lock:
        for i, player in enumerate(matchmaking_queue):
            if player["sid"] == sid:
                matchmaking_queue.pop(i)
                break
    
    if room and room in games:
        g = games[room]
        
        disconnected_color = None
        if sid == g.get("white_sid"):
            disconnected_color = "white"
        elif sid == g.get("black_sid"):
            disconnected_color = "black"
        elif sid in g.get("spectators", set()):
            # Handle spectator disconnect
            g["spectators"].discard(sid)
            spectator_count = len(g.get("spectators", set()))
            socketio.emit("spectator_left", {
                "spectatorCount": spectator_count
            }, room=room)

        if "clients" in g:
            g["clients"].discard(sid)

        if disconnected_color and g.get("isActive") and not g["winner"]:
            print(f"⚠️ {disconnected_color} disconnected from {room}. Starting {DISCONNECT_TIMEOUT}s timer.")
            socketio.emit("player_disconnected", {"color": disconnected_color, "timeout": DISCONNECT_TIMEOUT}, room=room)
            t = threading.Timer(DISCONNECT_TIMEOUT, handle_disconnect_timeout, [room, disconnected_color])
            t.start()
            if disconnected_color == "white":
                g["white_disconnect_timer"] = t
            else:
                g["black_disconnect_timer"] = t

        if len(g["clients"]) == 0 and not g.get("white_disconnect_timer") and not g.get("black_disconnect_timer"):
            print(f"🧹 Room '{room}' is empty and idle. Deleting game.")
            del games[room]

@socketio.on("move")
def move(data):
    room = data["room"]
    if room not in games: return
    g = games[room]
    
    if not g.get("isActive"): 
        emit("error", {"message": "Waiting for opponent..."})
        return
    
    with g["lock"]:
        update_time(g)
        if g["winner"]: return
        
        board = g["board"]
        f = chess.square(data["from"]["col"], 7-data["from"]["row"])
        t = chess.square(data["to"]["col"], 7-data["to"]["row"])
        mv = chess.Move(f, t, chess.QUEEN if data.get("promotion") else None)
        
        if mv not in board.legal_moves and not data.get("promotion"):
            mv = chess.Move(f, t, chess.QUEEN)

        if mv in board.legal_moves:
            san = board.san(mv)
            board.push(mv)
            
            # Record move for replay
            if "move_history" not in g:
                g["move_history"] = []
            
            g["move_history"].append({
                "notation": san,
                "from_square": chess.square_name(mv.from_square),
                "to_square": chess.square_name(mv.to_square),
                "white_time": g["whiteTime"],
                "black_time": g["blackTime"],
                "fen": board.fen()
            })
            
            winner, reason = check_game_over(board)
            if winner:
                g["winner"] = winner
                g["reason"] = reason
                save_game(room, g)
            
            for sid in g.get("clients", set()):
                socketio.emit("game_update", {
                    "state": export_state(room, sid),
                    "lastMove": data,
                    "moveNotation": san
                }, room=sid)

            if g["bot"] and not g["winner"]: 
                socketio.start_background_task(bot_play, room)

def bot_play(room):
    time.sleep(0.5)
    if room not in games: return
    g = games[room]

    with g["lock"]:
        board = g["board"]
        if board.is_game_over(): return

        best_move = None
        bot_difficulty = g.get("bot_difficulty", "medium")  # easy, medium, hard

        if STOCKFISH_PATH:
            try:
                engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)

                # Configure difficulty based on level
                if bot_difficulty == "easy":
                    # Limit depth and time for weaker play
                    result = engine.play(board, chess.engine.Limit(depth=1, time=0.1))
                elif bot_difficulty == "hard":
                    # Strong play with deeper search
                    result = engine.play(board, chess.engine.Limit(depth=15, time=1.0))
                else:  # medium (default)
                    # Balanced play
                    result = engine.play(board, chess.engine.Limit(depth=8, time=0.5))

                best_move = result.move
                engine.quit()
                print(f"🤖 Stockfish move: {best_move} (difficulty: {bot_difficulty})")
            except Exception as e:
                print(f"❌ Stockfish Error: {e}")
                print(f"   Falling back to random moves")
                best_move = random.choice(list(board.legal_moves))
        else:
            # Fallback to random moves if Stockfish not available
            print(f"🎲 Random bot move (Stockfish not available)")
            best_move = random.choice(list(board.legal_moves))

        if best_move:
            san = board.san(best_move)
            board.push(best_move)

            # Record bot move for replay (same as player moves)
            if "move_history" not in g:
                g["move_history"] = []

            g["move_history"].append({
                "notation": san,
                "from_square": chess.square_name(best_move.from_square),
                "to_square": chess.square_name(best_move.to_square),
                "white_time": g["whiteTime"],
                "black_time": g["blackTime"],
                "fen": board.fen()
            })

            winner, reason = check_game_over(board)
            if winner:
                g["winner"] = winner
                g["reason"] = reason
                save_game(room, g)

            for sid in g.get("clients", set()):
                socketio.emit("game_update", {
                    "state": export_state(room, sid),
                    "lastMove": {"from": {"row": 7-chess.square_rank(best_move.from_square), "col": chess.square_file(best_move.from_square)},
                                 "to": {"row": 7-chess.square_rank(best_move.to_square), "col": chess.square_file(best_move.to_square)}},
                    "moveNotation": san
                }, room=sid)

@socketio.on("send_message")
def msg(data):
    room = data.get("room")
    sender = data.get("sender")

    # Block spectators from sending messages
    if sender == "spectator":
        return

    # Check if sender is actually a player in the game
    if room in games:
        g = games[room]
        if request.sid not in [g.get("white_sid"), g.get("black_sid")]:
            return

    socketio.emit("chat_message", data, room=room)

@socketio.on("typing")
def on_typing(data):
    # Block spectators from showing typing indicator
    if data.get("sender") == "spectator":
        return
    socketio.emit("user_typing", data, room=data["room"], skip_sid=request.sid)

@socketio.on("stop_typing")
def on_stop_typing(data):
    # Block spectators from showing typing indicator
    if data.get("sender") == "spectator":
        return
    socketio.emit("user_stop_typing", data, room=data["room"], skip_sid=request.sid)

@socketio.on("offer_draw")
def offer_draw(data):
    socketio.emit("draw_offered", {"fromColor": data["color"]}, room=data["room"], skip_sid=request.sid)

@socketio.on("respond_draw")
def respond_draw(data):
    room = data["room"]
    if room not in games: return
    g = games[room]
    if data["accept"]:
        g["winner"] = "draw"
        g["reason"] = "agreement"
        save_game(room, g)
        for sid in g.get("clients", set()):
            socketio.emit("game_update", {"state": export_state(room, sid)}, room=sid)
    else:
        socketio.emit("draw_declined", {}, room=room)

@socketio.on("resign")
def resign(data):
    room = data["room"]
    if room not in games: return
    g = games[room]
    if g["winner"]: return
    g["winner"] = "black" if data["color"] == "white" else "white"
    g["reason"] = "resign"
    save_game(room, g)
    for sid in g.get("clients", set()):
        socketio.emit("game_update", {"state": export_state(room, sid)}, room=sid)

@socketio.on("leave_room")
def on_leave(data):
    room = data.get("room")
    if room: leave_room(room)

if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', 5001)), allow_unsafe_werkzeug=True)
