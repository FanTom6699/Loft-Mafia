"""Stateful domain models for the Mafia game.

``GameRoom`` remains the historical runtime class for compatibility while
individual behavior is extracted into focused domain modules.
"""

from ..game_legacy import GameRoom, Player
from .lobby import (
    add_player,
    close_registration,
    extend_registration,
    open_registration,
    remove_player,
)

# Route lobby behavior through the extracted domain module while keeping the
# original GameRoom class identity used by storage and handlers.
GameRoom.add_player = add_player
GameRoom.open_registration = open_registration
GameRoom.extend_registration = extend_registration
GameRoom.close_registration = close_registration
GameRoom.remove_player = remove_player

__all__ = ["Player", "GameRoom"]
