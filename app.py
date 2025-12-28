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

# ===== STOCKFISH SETUP =====
STOCKFISH_PATH = shutil.which("stockfish") or "/usr/games/stockfish" or "/usr/bin/stockfish"
print(f"♟️ Stockfish Engine Path: {STOCKFISH_PATH}")

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
        return
    
    end_time = datetime.utcnow()
    start_time = g.get("start_timestamp", end_time)
    win_reason = g.get("reason", "unknown")
    
    success = save_game_record(room, g, start_time, end_time, win_reason)
    if success:
        g["saved"] = True

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
    
    creator_color = random.choice(["white", "black"]) if not is_bot else "white"
    
    if room in games:
        g = games[room]
        if g.get("winner") is None: 
            emit("error", {"message": f"Room '{room}' is already taken!"})
            return

    white_player = player_name if creator_color == "white" else None
    black_player = player_name if creator_color == "black" else None
    
    if is_bot:
        black_player = "Bot"
    
    # Get user ID if authenticated
    user = get_current_user()
    white_user_id = user['id'] if user and creator_color == "white" else None
    black_user_id = user['id'] if user and creator_color == "black" else None

    games[room] = {
        "board": chess.Board(),
        "whiteTime": float(data.get("timeControl", 300)),
        "blackTime": float(data.get("timeControl", 300)),
        "lastUpdate": time.time(),
        "start_timestamp": datetime.utcnow(),
        "isActive": True if is_bot else False,
        "winner": None,
        "bot": is_bot,
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

    if room not in games: 
        emit("error", {"message": "Room not found"})
        return
        
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
        socketio.emit("player_reconnected", {"color": "white"}, room=room)
        
    elif g["black_player"] and player_name == g["black_player"] and g.get("black_sid") is None:
        g["black_sid"] = request.sid
        cancel_timer(g, "black")
        reconnected = True
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
        # Link user if authenticated
        user = get_current_user()
        if user:
            g["white_user_id"] = user['id']
    elif not g["black_player"]:
        g["black_player"] = player_name
        g["black_sid"] = request.sid
        g["isActive"] = True
        g["lastUpdate"] = time.time()
        # Link user if authenticated
        user = get_current_user()
        if user:
            g["black_user_id"] = user['id']
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
    sid = request.sid
    
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
            "lock": threading.Lock(),
            "white_player": player_name,
            "black_player": "Bot",
            "white_sid": requester_sid,
            "black_sid": None,
            "white_disconnect_timer": None,
            "black_disconnect_timer": None,
            "clients": {requester_sid},
            "game_mode": "bot"
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
                "white_disconnect_timer": None,
                "black_disconnect_timer": None,
                "clients": {white_sid, black_sid},
                "game_mode": g.get("game_mode", "friend")
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

@socketio.on("disconnect")
def on_disconnect():
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
    time.sleep(0.5) 
    if room not in games: return
    g = games[room]
    
    with g["lock"]:
        board = g["board"]
        if board.is_game_over(): return

        best_move = None
        if STOCKFISH_PATH:
            try:
                engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
                result = engine.play(board, chess.engine.Limit(time=0.5))
                best_move = result.move
                engine.quit()
            except Exception as e:
                print(f"❌ Stockfish Error: {e}")
                best_move = random.choice(list(board.legal_moves))
        else:
            best_move = random.choice(list(board.legal_moves))

        if best_move:
            san = board.san(best_move)
            board.push(best_move)
            
            if board.is_game_over():
                g["winner"] = "black"
                g["reason"] = "checkmate"
                save_game(room, g)
            
            for sid in g.get("clients", set()):
                socketio.emit("game_update", {
                    "state": export_state(room, sid),
                    "lastMove": {"from": {"row": 7-chess.square_rank(best_move.from_square), "col": chess.square_file(best_move.from_square)}, 
                                 "to": {"row": 7-chess.square_rank(best_move.to_square), "col": chess.square_file(best_move.to_square)}},
                    "moveNotation": san
                }, room=sid)

@socketio.on("send_message")
def msg(data): socketio.emit("chat_message", data, room=data["room"])
@socketio.on("typing")
def on_typing(data):
    socketio.emit("user_typing", data, room=data["room"], skip_sid=request.sid)

@socketio.on("stop_typing")
def on_stop_typing(data):
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
    socketio.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
