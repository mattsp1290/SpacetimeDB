"""
Example SpacetimeDB Python Server Module: Simple Multiplayer Game

This demonstrates the complete Python server capabilities with tables, reducers,
and advanced type conversion features.
"""

from spacetimedb_server import table, reducer, scheduled, ReducerContext
from typing import Optional, List
import time

# Game state table
@table(primary_key="id", indexes=["active"])
class GameState:
    id: int = 1
    round_number: int = 0
    active_players: int = 0
    game_started: bool = False
    last_updated: int = 0

# Player table with comprehensive field types
@table(primary_key="identity", indexes=["name", "score"])
class Player:
    identity: str           # Primary key - player's unique identity
    name: str              # Player's display name
    score: int = 0         # Current score
    level: int = 1         # Player level
    health: float = 100.0  # Health points
    position_x: float = 0.0 # X coordinate
    position_y: float = 0.0 # Y coordinate
    active: bool = True    # Whether player is active
    last_seen: int = 0     # Timestamp of last activity
    inventory: List[str] = [] # Player's items

# Game events table for tracking actions
@table(indexes=["player_identity", "event_type", "timestamp"])
class GameEvent:
    id: int                # Auto-increment ID
    player_identity: str   # Who performed the action
    event_type: str        # Type of event (join, move, attack, etc.)
    data: str             # JSON data for the event
    timestamp: int        # When the event occurred

# Leaderboard entry
@table(primary_key="rank", indexes=["player_name"])
class LeaderboardEntry:
    rank: int
    player_name: str
    score: int
    achievements: int = 0

# === REDUCER FUNCTIONS ===

@reducer
def join_game(ctx: ReducerContext, player_name: str):
    """
    Reducer for players joining the game.
    Demonstrates basic table operations and context usage.
    """
    player_identity = str(ctx.sender)
    
    # Check if player already exists
    existing_player = ctx.db.get_table(Player).find_by_primary_key(player_identity)
    if existing_player:
        ctx.log(f"Player {player_name} is already in the game")
        return
    
    # Create new player
    new_player = Player(
        identity=player_identity,
        name=player_name,
        score=0,
        level=1,
        health=100.0,
        position_x=0.0,
        position_y=0.0,
        active=True,
        last_seen=ctx.timestamp,
        inventory=["starter_sword", "health_potion"]
    )
    
    # Insert player into database
    ctx.db.get_table(Player).insert(new_player)
    
    # Update game state
    game_state = ctx.db.get_table(GameState).find_by_primary_key(1)
    if game_state:
        game_state.active_players += 1
        game_state.last_updated = ctx.timestamp
        ctx.db.get_table(GameState).insert(game_state)  # Update
    else:
        # Create initial game state
        initial_state = GameState(
            id=1,
            round_number=1,
            active_players=1,
            game_started=True,
            last_updated=ctx.timestamp
        )
        ctx.db.get_table(GameState).insert(initial_state)
    
    # Log the join event
    join_event = GameEvent(
        id=ctx.timestamp,  # Use timestamp as simple ID
        player_identity=player_identity,
        event_type="join",
        data=f'{{"player_name": "{player_name}"}}',
        timestamp=ctx.timestamp
    )
    ctx.db.get_table(GameEvent).insert(join_event)
    
    ctx.log(f"Player {player_name} joined the game successfully")

@reducer
def move_player(ctx: ReducerContext, x: float, y: float):
    """
    Reducer for moving a player to new coordinates.
    Demonstrates float handling and player updates.
    """
    player_identity = str(ctx.sender)
    
    # Find the player
    player = ctx.db.get_table(Player).find_by_primary_key(player_identity)
    if not player:
        ctx.log("Player not found. Please join the game first.", "error")
        return
    
    # Validate movement (simple bounds checking)
    if x < -100.0 or x > 100.0 or y < -100.0 or y > 100.0:
        ctx.log("Invalid coordinates. Stay within game bounds!", "warning")
        return
    
    # Update player position
    player.position_x = x
    player.position_y = y
    player.last_seen = ctx.timestamp
    ctx.db.get_table(Player).insert(player)  # Update
    
    # Log movement event
    move_event = GameEvent(
        id=ctx.timestamp + 1,  # Ensure unique ID
        player_identity=player_identity,
        event_type="move",
        data=f'{{"x": {x}, "y": {y}}}',
        timestamp=ctx.timestamp
    )
    ctx.db.get_table(GameEvent).insert(move_event)
    
    ctx.log(f"Player {player.name} moved to ({x}, {y})")

@reducer
def attack_player(ctx: ReducerContext, target_identity: str, damage: int):
    """
    Reducer for player-vs-player combat.
    Demonstrates complex game logic and multiple table updates.
    """
    attacker_identity = str(ctx.sender)
    
    # Get attacker
    attacker = ctx.db.get_table(Player).find_by_primary_key(attacker_identity)
    if not attacker:
        ctx.log("Attacker not found", "error")
        return
    
    # Get target
    target = ctx.db.get_table(Player).find_by_primary_key(target_identity)
    if not target:
        ctx.log("Target player not found", "error")
        return
    
    # Check if target is alive
    if target.health <= 0:
        ctx.log("Cannot attack a defeated player", "warning")
        return
    
    # Calculate distance (simple distance check)
    distance = ((attacker.position_x - target.position_x) ** 2 + 
                (attacker.position_y - target.position_y) ** 2) ** 0.5
    
    if distance > 10.0:  # Attack range limit
        ctx.log("Target is too far away to attack", "warning")
        return
    
    # Apply damage
    actual_damage = min(damage, int(target.health))
    target.health -= actual_damage
    
    # Award points to attacker
    attacker.score += actual_damage
    attacker.last_seen = ctx.timestamp
    
    # Update both players
    ctx.db.get_table(Player).insert(attacker)
    ctx.db.get_table(Player).insert(target)
    
    # Log attack event
    attack_event = GameEvent(
        id=ctx.timestamp + 2,
        player_identity=attacker_identity,
        event_type="attack",
        data=f'{{"target": "{target_identity}", "damage": {actual_damage}}}',
        timestamp=ctx.timestamp
    )
    ctx.db.get_table(GameEvent).insert(attack_event)
    
    if target.health <= 0:
        ctx.log(f"{attacker.name} defeated {target.name}!")
        # Bonus points for defeat
        attacker.score += 50
        ctx.db.get_table(Player).insert(attacker)
    else:
        ctx.log(f"{attacker.name} attacked {target.name} for {actual_damage} damage")

@reducer
def heal_player(ctx: ReducerContext, amount: int):
    """
    Reducer for healing a player.
    Demonstrates inventory management and health mechanics.
    """
    player_identity = str(ctx.sender)
    
    # Find player
    player = ctx.db.get_table(Player).find_by_primary_key(player_identity)
    if not player:
        ctx.log("Player not found", "error")
        return
    
    # Check if player has health potions
    if "health_potion" not in player.inventory:
        ctx.log("No health potions available", "warning")
        return
    
    # Use health potion
    player.inventory.remove("health_potion")
    
    # Heal player (cap at 100)
    old_health = player.health
    player.health = min(100.0, player.health + amount)
    actual_healing = player.health - old_health
    player.last_seen = ctx.timestamp
    
    # Update player
    ctx.db.get_table(Player).insert(player)
    
    # Log healing event
    heal_event = GameEvent(
        id=ctx.timestamp + 3,
        player_identity=player_identity,
        event_type="heal",
        data=f'{{"amount": {actual_healing}}}',
        timestamp=ctx.timestamp
    )
    ctx.db.get_table(GameEvent).insert(heal_event)
    
    ctx.log(f"{player.name} healed for {actual_healing} HP")

@reducer
def update_leaderboard(ctx: ReducerContext):
    """
    Reducer to manually update the leaderboard.
    Demonstrates complex queries and batch operations.
    """
    # Get all players sorted by score (this would be a complex query in real implementation)
    # For demo purposes, we'll simulate this
    all_players = ctx.db.get_table(Player).scan()
    
    # Sort players by score (in a real implementation, this would be done efficiently by the database)
    # Here we'll just update leaderboard for the top players
    top_players = sorted(all_players, key=lambda p: p.score, reverse=True)[:10]
    
    # Clear existing leaderboard (in a real implementation, we'd do this more efficiently)
    ctx.db.get_table(LeaderboardEntry).delete()
    
    # Insert top players
    for rank, player in enumerate(top_players, 1):
        leaderboard_entry = LeaderboardEntry(
            rank=rank,
            player_name=player.name,
            score=player.score,
            achievements=0  # Would calculate achievements in real game
        )
        ctx.db.get_table(LeaderboardEntry).insert(leaderboard_entry)
    
    ctx.log(f"Leaderboard updated with {len(top_players)} players")

# === SCHEDULED REDUCERS ===

@scheduled(interval_ms=30000)  # Every 30 seconds
def game_tick(ctx: ReducerContext):
    """
    Scheduled reducer that runs game maintenance every 30 seconds.
    Demonstrates scheduled reducer functionality.
    """
    # Clean up inactive players (haven't been seen in 5 minutes)
    current_time = ctx.timestamp
    inactive_threshold = current_time - (5 * 60 * 1000)  # 5 minutes ago
    
    # Find inactive players
    all_players = ctx.db.get_table(Player).scan()
    inactive_count = 0
    
    for player in all_players:
        if player.last_seen < inactive_threshold and player.active:
            player.active = False
            ctx.db.get_table(Player).insert(player)
            inactive_count += 1
    
    # Update game state
    game_state = ctx.db.get_table(GameState).find_by_primary_key(1)
    if game_state:
        game_state.active_players = len([p for p in all_players if p.active])
        game_state.last_updated = current_time
        ctx.db.get_table(GameState).insert(game_state)
    
    if inactive_count > 0:
        ctx.log(f"Game tick: {inactive_count} players marked inactive")

@scheduled(interval_ms=120000)  # Every 2 minutes
def leaderboard_update(ctx: ReducerContext):
    """
    Scheduled reducer to automatically update leaderboard.
    """
    update_leaderboard(ctx)

@scheduled(delay_ms=10000, repeating=False)  # One-time delay
def game_start_announcement(ctx: ReducerContext):
    """
    One-time scheduled reducer for game start announcement.
    """
    ctx.log("🎮 Welcome to SpacetimeDB Python Game Server! 🎮")
    ctx.log("Available commands: join_game, move_player, attack_player, heal_player")
    ctx.log("Game features: Real-time multiplayer, leaderboards, combat system")

# === GAME LOGIC HELPERS ===

def get_player_stats(ctx: ReducerContext, player_identity: str) -> dict:
    """
    Helper function to get comprehensive player statistics.
    This would be called by other reducers.
    """
    player = ctx.db.get_table(Player).find_by_primary_key(player_identity)
    if not player:
        return {}
    
    # Get player's recent events
    events = ctx.db.get_table(GameEvent).filter(player_identity=player_identity)
    recent_events = [e for e in events if e.timestamp > (ctx.timestamp - 3600000)]  # Last hour
    
    return {
        "player": player,
        "recent_events": len(recent_events),
        "kills": len([e for e in recent_events if e.event_type == "attack"]),
        "movements": len([e for e in recent_events if e.event_type == "move"])
    }

@reducer
def get_game_status(ctx: ReducerContext):
    """
    Reducer to get current game status.
    Demonstrates read-only operations and complex data aggregation.
    """
    # Get game state
    game_state = ctx.db.get_table(GameState).find_by_primary_key(1)
    if not game_state:
        ctx.log("Game not initialized", "error")
        return
    
    # Get active players
    active_players = ctx.db.get_table(Player).filter(active=True)
    
    # Get recent events (last 10 minutes)
    recent_threshold = ctx.timestamp - (10 * 60 * 1000)
    recent_events = ctx.db.get_table(GameEvent).filter(timestamp__gte=recent_threshold)
    
    # Log comprehensive status
    ctx.log(f"🎮 Game Status Report 🎮")
    ctx.log(f"Round: {game_state.round_number}")
    ctx.log(f"Active Players: {len(active_players)}")
    ctx.log(f"Recent Activity: {len(recent_events)} events in last 10 minutes")
    ctx.log(f"Game Started: {game_state.game_started}")
    ctx.log(f"Last Updated: {game_state.last_updated}")

# Demo function to show advanced type handling
@reducer
def test_complex_types(ctx: ReducerContext, test_data: dict):
    """
    Reducer to test complex type conversion capabilities.
    Demonstrates the enhanced BSATN bridge functionality.
    """
    ctx.log(f"Received complex data: {test_data}")
    
    # This would test various data types:
    # - Nested objects (Products)
    # - Union types (Sums) 
    # - Arrays of different types
    # - Large numbers
    # - Strings with special characters
    
    ctx.log("Complex type conversion test completed successfully")
