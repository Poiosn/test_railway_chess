from flask import Flask, render_template, request, jsonify, session
from flask_socketio import SocketIO, emit, join_room, leave_room
import chess
import chess.engine
import time
import random
import secrets
import threading
import os
from datetime import datetime
import shutil
import hashlib
import hmac
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import database functions
from database import (
    init_db_pool, get_db_conn, release_db_conn,
    increment_visitor_count, get_total_visitor_count,
    get_leaderboard_data, save_game_record,
    get_user_by_id, get_user_by_username, create_user,
    update_last_login, get_user_profile, get_user_games,
    get_game_replay
)

# ===== FLASK APP =====
app = Flask(__name__)
app.config["SECRET_KEY"] = secrets.token_hex(16)
app.config["SESSION_COOKIE_SECURE"] = False  # Set to True in production with HTTPS
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ===== AUTHENTICATION HELPERS =====
def hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, password_hash):
    """Verify password against hash"""
    return hmac.compare_digest(hash_password(password), password_hash)

def get_current_user():
    """Get current logged in user from session"""
    user_id = session.get('user_id')
    if not user_id:
        return None
    return get_user_by_id(user_id)

# Initialize database on startup
init_db_pool()

# ===== IMPROVED STOCKFISH SETUP =====
STOCKFISH_PATH = None
STOCKFISH_ENGINE = None
engine_lock = threading.Lock()

def find_stockfish():
    """Find Stockfish executable"""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    stockfish_paths = [
        "/usr/games/stockfish",          # Railway/Linux (most common)
        "/usr/bin/stockfish",            # Linux alternative
        os.path.join(BASE_DIR, "stockfish"),  # Project directory
        os.path.expanduser("~/.local/bin/stockfish"),  # User local bin
        shutil.which("stockfish"),
        "/opt/homebrew/bin/stockfish",  # Homebrew on Apple Silicon
        "/usr/local/bin/stockfish",      # Homebrew on Intel Mac
    ]
    
    for path in stockfish_paths:
        if path and os.path.exists(path):
            logger.info(f"✅ Stockfish Found: {path}")
            # Test if it's executable
            try:
                test_engine = chess.engine.SimpleEngine.popen_uci(path)
                test_engine.quit()
                logger.info(f"✅ Stockfish verified working at: {path}")
                return path
            except Exception as e:
                logger.warning(f"⚠️ Stockfish found at {path} but failed to initialize: {e}")
                continue
    
    logger.error("❌ Stockfish not found in any location!")
    logger.info("📥 Install Stockfish:")
    logger.info("   Railway: Add to nixpacks.toml: aptPkgs = ['stockfish']")
    logger.info("   macOS: brew install stockfish")
    logger.info("   Linux: sudo apt-get install stockfish")
    return None

STOCKFISH_PATH = find_stockfish()

def get_stockfish_engine():
    """Get or create a Stockfish engine instance (thread-safe)"""
    global STOCKFISH_ENGINE
    
    if not STOCKFISH_PATH:
        return None
    
    with engine_lock:
        # Create new engine for each call (safer for multi-threading)
        try:
            engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
            return engine
        except Exception as e:
            logger.error(f"❌ Failed to create Stockfish engine: {e}")
            return None

games = {}
sid_to_room = {}
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
    """Register a new user"""
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
    
    # Check if user exists
    existing_user = get_user_by_username(username)
    if existing_user:
        return jsonify({'error': 'Username or email already exists'}), 409
    
    # Create user
    password_hash = hash_password(password)
    user_id = create_user(username, email, password_hash, display_name)
    
    if not user_id:
        return jsonify({'error': 'Registration failed'}), 500
    
    # Log user in
    session['user_id'] = user_id
    session['username'] = username
    
    return jsonify({
        'message': 'Registration successful',
        'user': {'id': user_id, 'username': username, 'displayName': display_name}
    }), 201

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
    games_list = get_user_games(username)
    if games_list is None:
        return jsonify({'error': 'User not found'}), 404
    return jsonify({'games': games_list}), 200

@app.route('/api/game/<int:game_id>/replay')
def get_game_replay_api(game_id):
    """Get game replay data"""
    replay = get_game_replay(game_id)
    if not replay:
        return jsonify({'error': 'Game not found'}), 404
    return jsonify(replay), 200

# ===== GAME LOGIC =====
def export_state(room, sid):
    g = games[room]
    is_white = (sid == g.get("white_sid"))
    is_black = (sid == g.get("black_sid"))
    is_spectator = sid in g.get("spectators", set())
    
    # Determine player's color
    color = None
    if is_white:
        color = "white"
    elif is_black:
        color = "black"
    elif is_spectator:
        color = "spectator"
    
    # Convert chess.Board to 8x8 array for frontend
    board = g["board"]
    board_array = []
    for rank in range(8):
        row = []
        for file in range(8):
            square = chess.square(file, 7 - rank)
            piece = board.piece_at(square)
            if piece:
                row.append(str(piece))
            else:
                row.append(".")
        board_array.append(row)
    
    # Calculate legal moves for each piece
    moves_dict = {}
    if not board.is_game_over():
        for move in board.legal_moves:
            from_square = move.from_square
            from_row = 7 - chess.square_rank(from_square)
            from_col = chess.square_file(from_square)
            to_row = 7 - chess.square_rank(move.to_square)
            to_col = chess.square_file(move.to_square)
            
            key = f"{from_row},{from_col}"
            if key not in moves_dict:
                moves_dict[key] = []
            moves_dict[key].append({"row": to_row, "col": to_col})
    
    # Format times as MM:SS
    def format_time(seconds):
        minutes = int(seconds) // 60
        secs = int(seconds) % 60
        return f"{minutes}:{secs:02d}"
    
    return {
        "board": board_array,
        "moves": moves_dict,
        "turn": "white" if board.turn else "black",
        "check": board.is_check(),
        "winner": g["winner"],
        "reason": g.get("reason"),
        "isActive": g.get("isActive", False),
        "whiteName": g.get("white_player", "White"),
        "blackName": g.get("black_player", "Black"),
        "whiteTimeFormatted": format_time(g["whiteTime"]),
        "blackTimeFormatted": format_time(g["blackTime"]),
        "spectatorCount": len(g.get("spectators", set())),
        "gameMode": g.get("game_mode", "friend"),
        "color": color  # Add this line - IMPORTANT!
    }

def update_time(g):
    """Update chess clock"""
    now = time.time()
    elapsed = now - g["lastUpdate"]
    g["lastUpdate"] = now
    
    if not g.get("isActive") or g["winner"]:
        return
    
    if g["board"].turn:  # White's turn
        g["whiteTime"] = max(0, g["whiteTime"] - elapsed)
        if g["whiteTime"] <= 0:
            g["winner"] = "black"
            g["reason"] = "timeout"
    else:  # Black's turn
        g["blackTime"] = max(0, g["blackTime"] - elapsed)
        if g["blackTime"] <= 0:
            g["winner"] = "white"
            g["reason"] = "timeout"

def save_game(room, g):
    """Save completed game to database"""
    if g.get("game_saved"):
        return
    
    white_user_id = g.get("white_user_id")
    black_user_id = g.get("black_user_id")
    
    # Skip saving bot games without user
    if g.get("bot") and not white_user_id:
        return
    
    save_game_record(
        room_name=room,
        white_player=g.get("white_player", "White"),
        black_player=g.get("black_player", "Black"),
        winner=g["winner"],
        win_reason=g.get("reason", "unknown"),
        start_time=g.get("start_time"),
        end_time=datetime.now(),
        white_user_id=white_user_id,
        black_user_id=black_user_id,
        time_control=g.get("time_control"),
        game_mode=g.get("game_mode", "friend"),
        move_history=g.get("move_history", [])
    )
    
    g["game_saved"] = True

def handle_disconnect_timeout(room, color):
    """Handle player disconnect timeout"""
    if room not in games:
        return
    
    g = games[room]
    
    # Check if player reconnected
    if color == "white" and g.get("white_sid"):
        return
    if color == "black" and g.get("black_sid"):
        return
    
    # Player didn't reconnect - they lose
    if not g["winner"]:
        g["winner"] = "black" if color == "white" else "white"
        g["reason"] = "disconnect"
        save_game(room, g)
        
        for sid in g.get("clients", set()):
            socketio.emit("game_update", {"state": export_state(room, sid)}, room=sid)

# ===== SOCKETIO EVENTS =====

@socketio.on("connect")
def connect():
    logger.info(f"🔌 Client connected: {request.sid}")

@socketio.on("create_room")
def create(data):
    room = data["room"]
    time_control = data.get("timeControl", 600)
    bot_mode = data.get("bot", False)
    bot_difficulty = data.get("botDifficulty", "medium")
    game_mode = data.get("gameMode", "friend")
    
    current_user = get_current_user()
    player_name = current_user['display_name'] if current_user else data.get("playerName", "Player")
    user_id = current_user['id'] if current_user else None
    
    sid = request.sid
    join_room(room)
    sid_to_room[sid] = room
    
    games[room] = {
        "board": chess.Board(),
        "white_sid": sid,
        "black_sid": None,
        "white_player": player_name,
        "black_player": "Stockfish Bot" if bot_mode else "Waiting...",
        "white_user_id": user_id,
        "black_user_id": None,
        "whiteTime": time_control,
        "blackTime": time_control,
        "time_control": time_control,
        "lastUpdate": time.time(),
        "winner": None,
        "isActive": bot_mode,
        "clients": {sid},
        "spectators": set(),
        "lock": threading.Lock(),
        "bot": bot_mode,
        "bot_difficulty": bot_difficulty,
        "game_mode": game_mode,
        "start_time": datetime.now() if bot_mode else None,
        "move_history": []
    }
    
    logger.info(f"🎮 Room '{room}' created by {player_name} (bot={bot_mode}, difficulty={bot_difficulty})")
    
    # Send proper state structure
    state = export_state(room, sid)
    emit("room_created", {
        "room": room,
        "color": state["color"],
        "state": state
    })

@socketio.on("join_room")
def join(data):
    room = data["room"]
    sid = request.sid
    
    if room not in games:
        emit("error", {"message": "Room does not exist."})
        return
    
    g = games[room]
    join_room(room)
    sid_to_room[sid] = room
    g["clients"].add(sid)
    
    current_user = get_current_user()
    player_name = current_user['display_name'] if current_user else data.get("playerName", "Player")
    user_id = current_user['id'] if current_user else None
    
    # Check if rejoining as original player
    if sid == g.get("white_sid"):
        # White player reconnected
        if g.get("white_disconnect_timer"):
            g["white_disconnect_timer"].cancel()
            g["white_disconnect_timer"] = None
        socketio.emit("player_reconnected", {"color": "white"}, room=room)
        state = export_state(room, sid)
        emit("game_joined", {
            "room": room,
            "color": state["color"],
            "state": state
        })
        return
    
    if sid == g.get("black_sid"):
        # Black player reconnected
        if g.get("black_disconnect_timer"):
            g["black_disconnect_timer"].cancel()
            g["black_disconnect_timer"] = None
        socketio.emit("player_reconnected", {"color": "black"}, room=room)
        state = export_state(room, sid)
        emit("game_joined", {
            "room": room,
            "color": state["color"],
            "state": state
        })
        return
    
    # New player joining
    if not g["black_sid"] and not g.get("bot"):
        # Join as black player
        g["black_sid"] = sid
        g["black_player"] = player_name
        g["black_user_id"] = user_id
        g["isActive"] = True
        g["lastUpdate"] = time.time()
        g["start_time"] = datetime.now()
        
        for client_sid in g["clients"]:
            state = export_state(room, client_sid)
            socketio.emit("game_update", {
                "state": state
            }, room=client_sid)
        
        logger.info(f"👥 {player_name} joined room '{room}' as Black")
    else:
        # Join as spectator
        g["spectators"].add(sid)
        spectator_count = len(g["spectators"])
        socketio.emit("spectator_joined", {"spectatorCount": spectator_count}, room=room)
        state = export_state(room, sid)
        emit("game_joined", {
            "room": room,
            "color": state["color"],
            "state": state
        })
        logger.info(f"👁️ Spectator joined room '{room}' (total: {spectator_count})")

@socketio.on("find_match")
def find_match(data):
    """Handle matchmaking requests"""
    sid = request.sid
    time_control = data.get("timeControl", 600)
    
    current_user = get_current_user()
    player_name = current_user['display_name'] if current_user else data.get("playerName", "Player")
    user_id = current_user['id'] if current_user else None
    
    with matchmaking_lock:
        # Check if already in queue
        for player in matchmaking_queue:
            if player["sid"] == sid:
                emit("error", {"message": "Already in matchmaking queue"})
                return
        
        # Try to find opponent with same time control
        matched = False
        for i, opponent in enumerate(matchmaking_queue):
            if opponent["timeControl"] == time_control:
                # Match found!
                matchmaking_queue.pop(i)
                
                # Create game room
                room = f"match_{secrets.token_hex(8)}"
                
                # Randomly assign colors
                if random.choice([True, False]):
                    white_sid, black_sid = sid, opponent["sid"]
                    white_name, black_name = player_name, opponent["playerName"]
                    white_id, black_id = user_id, opponent["userId"]
                else:
                    white_sid, black_sid = opponent["sid"], sid
                    white_name, black_name = opponent["playerName"], player_name
                    white_id, black_id = opponent["userId"], user_id
                
                # Add both players to room
                join_room(room, sid=white_sid)
                join_room(room, sid=black_sid)
                sid_to_room[white_sid] = room
                sid_to_room[black_sid] = room
                
                games[room] = {
                    "board": chess.Board(),
                    "white_sid": white_sid,
                    "black_sid": black_sid,
                    "white_player": white_name,
                    "black_player": black_name,
                    "white_user_id": white_id,
                    "black_user_id": black_id,
                    "whiteTime": time_control,
                    "blackTime": time_control,
                    "time_control": time_control,
                    "lastUpdate": time.time(),
                    "winner": None,
                    "isActive": True,
                    "clients": {white_sid, black_sid},
                    "spectators": set(),
                    "lock": threading.Lock(),
                    "bot": False,
                    "game_mode": "matchmaking",
                    "start_time": datetime.now(),
                    "move_history": []
                }
                
                # Notify both players
                socketio.emit("match_found", {
                    "room": room,
                    "state": export_state(room, white_sid)
                }, room=white_sid)
                
                socketio.emit("match_found", {
                    "room": room,
                    "state": export_state(room, black_sid)
                }, room=black_sid)
                
                matched = True
                logger.info(f"🎲 Match created: {white_name} vs {black_name} in room '{room}'")
                break
        
        if not matched:
            # Add to queue
            matchmaking_queue.append({
                "sid": sid,
                "playerName": player_name,
                "userId": user_id,
                "timeControl": time_control,
                "timestamp": time.time()
            })
            emit("matchmaking_waiting", {"queuePosition": len(matchmaking_queue)})
            logger.info(f"⏳ {player_name} added to matchmaking queue (position: {len(matchmaking_queue)})")

@socketio.on("cancel_matchmaking")
def cancel_matchmaking():
    """Cancel matchmaking search"""
    sid = request.sid
    
    with matchmaking_lock:
        for i, player in enumerate(matchmaking_queue):
            if player["sid"] == sid:
                matchmaking_queue.pop(i)
                emit("matchmaking_cancelled")
                logger.info(f"❌ Player cancelled matchmaking")
                return
    
    emit("error", {"message": "Not in matchmaking queue"})

@socketio.on("disconnect")
def disconnect():
    sid = request.sid
    room = sid_to_room.pop(sid, None)
    
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
            logger.info(f"⚠️ {disconnected_color} disconnected from {room}. Starting {DISCONNECT_TIMEOUT}s timer.")
            socketio.emit("player_disconnected", {"color": disconnected_color, "timeout": DISCONNECT_TIMEOUT}, room=room)
            t = threading.Timer(DISCONNECT_TIMEOUT, handle_disconnect_timeout, [room, disconnected_color])
            t.start()
            if disconnected_color == "white":
                g["white_disconnect_timer"] = t
            else:
                g["black_disconnect_timer"] = t

        if len(g["clients"]) == 0 and not g.get("white_disconnect_timer") and not g.get("black_disconnect_timer"):
            logger.info(f"🧹 Room '{room}' is empty and idle. Deleting game.")
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
            
            if board.is_game_over():
                g["winner"] = "white" if not board.turn else "black"
                if board.is_stalemate(): g["winner"] = "draw"
                else: g["reason"] = "checkmate"
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
    """IMPROVED BOT FUNCTION with better error handling and engine management"""
    time.sleep(0.5)
    if room not in games:
        return
    
    g = games[room]
    engine = None
    
    try:
        with g["lock"]:
            board = g["board"]
            if board.is_game_over():
                return

            best_move = None
            bot_difficulty = g.get("bot_difficulty", "medium")
            
            logger.info(f"🤖 Bot thinking (difficulty: {bot_difficulty}, path: {STOCKFISH_PATH})...")
            
            if STOCKFISH_PATH:
                try:
                    # Get a fresh engine instance
                    engine = get_stockfish_engine()
                    
                    if engine:
                        # Configure difficulty based on level
                        if bot_difficulty == "easy":
                            result = engine.play(board, chess.engine.Limit(depth=1, time=0.1))
                        elif bot_difficulty == "hard":
                            result = engine.play(board, chess.engine.Limit(depth=15, time=1.0))
                        else:  # medium
                            result = engine.play(board, chess.engine.Limit(depth=8, time=0.5))
                        
                        best_move = result.move
                        logger.info(f"✅ Stockfish move: {best_move} (difficulty: {bot_difficulty})")
                    else:
                        logger.error("❌ Failed to get Stockfish engine, using random move")
                        best_move = random.choice(list(board.legal_moves))
                        
                except Exception as e:
                    logger.error(f"❌ Stockfish Error: {e}")
                    import traceback
                    traceback.print_exc()
                    logger.info("   Falling back to random moves")
                    best_move = random.choice(list(board.legal_moves))
            else:
                logger.warning("🎲 Stockfish not available, using random move")
                best_move = random.choice(list(board.legal_moves))

            if best_move:
                san = board.san(best_move)
                board.push(best_move)

                # Record bot move
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

                if board.is_game_over():
                    g["winner"] = "black"
                    g["reason"] = "checkmate"
                    save_game(room, g)

                for sid in g.get("clients", set()):
                    socketio.emit("game_update", {
                        "state": export_state(room, sid),
                        "lastMove": {
                            "from": {
                                "row": 7-chess.square_rank(best_move.from_square),
                                "col": chess.square_file(best_move.from_square)
                            },
                            "to": {
                                "row": 7-chess.square_rank(best_move.to_square),
                                "col": chess.square_file(best_move.to_square)
                            }
                        },
                        "moveNotation": san
                    }, room=sid)
    
    finally:
        # CRITICAL: Always close the engine
        if engine:
            try:
                engine.quit()
            except Exception as e:
                logger.error(f"Error closing engine: {e}")

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
    if data.get("sender") == "spectator":
        return
    socketio.emit("user_typing", data, room=data["room"], skip_sid=request.sid)

@socketio.on("stop_typing")
def on_stop_typing(data):
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
