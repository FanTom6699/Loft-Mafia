"""Domain-layer building blocks for the Mafia game.

The public compatibility surface remains ``mafia_bot.game`` for now.
"""

from .constants import *
from .roles import *
from .assignment import assign_roles, build_roles
from .day_flow import (
    all_alive_day_voted,
    all_alive_trial_voted,
    end_day_no_lynch,
    resolve_day_nomination,
    resolve_day_trial,
    set_trial_vote,
    start_day_discussion,
    start_day_nomination,
    start_day_trial,
    trial_vote_counts,
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
from .night_state import (
    all_required_night_actions_done,
    arm_documents,
    arm_shield,
    can_skip_night_action,
    pop_spent_documents_user_ids,
    pop_spent_shield_user_ids,
    set_night_skip,
)
from .player_state import (
    alive_civilians,
    alive_mafia,
    alive_mafia_ids,
    alive_players,
    all_mafia_voted,
    check_winner,
    current_mafia_target_id,
    forget_dead_commissar_checks,
    get_player,
    mark_night_role_announced,
    pop_pending_sergeant_check,
    remember_commissar_check,
    set_pending_sergeant_check,
    transfer_commissar_if_needed,
    transfer_don_if_needed,
)
from .models import GameRoom, Player
from .storage import GameStorage

__all__ = [name for name in globals() if not name.startswith("_")]
