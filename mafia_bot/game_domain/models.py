"""Stateful domain models for the Mafia game.

``GameRoom`` remains the historical runtime class for compatibility while
individual behavior is extracted into focused domain modules.
"""

from ..game_legacy import GameRoom, Player
from .assignment import assign_roles, build_roles
from .day_flow import (
    _reset_for_night_transition,
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
from .night_state import (
    all_required_night_actions_done,
    arm_documents,
    arm_shield,
    can_skip_night_action,
    pop_spent_documents_user_ids,
    pop_spent_shield_user_ids,
    set_night_skip,
)

# Route extracted behavior through focused domain modules while keeping the
# original GameRoom class identity used by storage and handlers.
GameRoom.add_player = add_player
GameRoom.open_registration = open_registration
GameRoom.extend_registration = extend_registration
GameRoom.close_registration = close_registration
GameRoom.remove_player = remove_player
GameRoom.assign_roles = assign_roles
GameRoom.build_roles = staticmethod(build_roles)

GameRoom.all_alive_day_voted = all_alive_day_voted
GameRoom.all_alive_trial_voted = all_alive_trial_voted
GameRoom.start_day_discussion = start_day_discussion
GameRoom.start_day_nomination = start_day_nomination
GameRoom.start_day_trial = start_day_trial
GameRoom.resolve_day_nomination = resolve_day_nomination
GameRoom.set_trial_vote = set_trial_vote
GameRoom.trial_vote_counts = trial_vote_counts
GameRoom._reset_for_night_transition = _reset_for_night_transition
GameRoom.end_day_no_lynch = end_day_no_lynch
GameRoom.resolve_day_trial = resolve_day_trial

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
GameRoom.all_required_night_actions_done = all_required_night_actions_done
GameRoom.can_skip_night_action = can_skip_night_action
GameRoom.set_night_skip = set_night_skip
GameRoom.arm_shield = arm_shield
GameRoom.arm_documents = arm_documents
GameRoom.pop_spent_documents_user_ids = pop_spent_documents_user_ids
GameRoom.pop_spent_shield_user_ids = pop_spent_shield_user_ids

__all__ = ["Player", "GameRoom"]
