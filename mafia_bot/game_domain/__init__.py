"""Domain-layer building blocks for the Mafia game.

The public compatibility surface remains ``mafia_bot.game`` for now.
"""

from .constants import *
from .roles import *
from .assignment import assign_roles, build_roles
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
from .models import GameRoom, Player
from .storage import GameStorage

__all__ = [name for name in globals() if not name.startswith("_")]
