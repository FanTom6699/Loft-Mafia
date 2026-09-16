"""Stateful domain models for the Mafia game.

``GameRoom`` remains the historical runtime class for compatibility while
individual behavior is extracted into focused domain modules.
"""

from ..game_legacy import GameRoom, Player
from .assignment import assign_roles, build_roles
from .lobby import (
    add_player,
    close_registration,
    extend_registration,
    open_registration,
    remove_player,
)
from .night_actions import (
    check_player_role,
    set_advocate_target,
    set_bum_target,
    set_commissar_action_mode,
    set_commissar_shot_target,
    set_doctor_target,
    set_kamikaze_target,
    set_maniac_target,
    set_mistress_target,
    set_night_vote,
)
from .night_resolution import resolve_night

# Route extracted behavior through focused domain modules while keeping the
# original GameRoom class identity used by storage and handlers.
GameRoom.add_player = add_player
GameRoom.open_registration = open_registration
GameRoom.extend_registration = extend_registration
GameRoom.close_registration = close_registration
GameRoom.remove_player = remove_player
GameRoom.assign_roles = assign_roles
GameRoom.build_roles = staticmethod(build_roles)
GameRoom.set_night_vote = set_night_vote
GameRoom.set_doctor_target = set_doctor_target
GameRoom.set_kamikaze_target = set_kamikaze_target
GameRoom.set_maniac_target = set_maniac_target
GameRoom.set_mistress_target = set_mistress_target
GameRoom.set_bum_target = set_bum_target
GameRoom.check_player_role = check_player_role
GameRoom.set_commissar_action_mode = set_commissar_action_mode
GameRoom.set_commissar_shot_target = set_commissar_shot_target
GameRoom.set_advocate_target = set_advocate_target
GameRoom.resolve_night = resolve_night

__all__ = ["Player", "GameRoom"]
