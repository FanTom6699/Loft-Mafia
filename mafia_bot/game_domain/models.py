"""Stateful domain models for the Mafia game.

The data model is copied from the historical ``GameRoom`` while behavior is
attached from focused domain modules below. Keeping the fields unchanged
preserves storage compatibility during the migration.
"""

from dataclasses import dataclass, field
from datetime import datetime

from .constants import (
    PHASE_LOBBY,
)
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
from .player_state import (
    _active_mafia_votes,
    _choose_mafia_target,
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
from .presentation import (
    add_night_report_line,
    alive_players_text,
    alive_role_counts_text,
    alive_role_hints_text,
    anonymous_player_label,
    can_send_last_word,
    commissar_check_result_text,
    consume_last_word,
    day_intro_text,
    day_media_caption,
    end_day_without_votes,
    final_report_text,
    game_duration_text,
    lobby_text,
    night_intro_text,
    night_media_caption,
    pop_night_kill_sources,
    pop_night_reports,
    public_player_mark,
    queue_last_words,
    resolve_day,
    seat_number,
    set_day_vote,
    status_text,
)


@dataclass
class Player:
    user_id: int
    full_name: str
    role: str = ""
    alive: bool = True


@dataclass
class GameRoom:
    chat_id: int
    host_id: int
    settings: dict = field(default_factory=dict)

    chat_title: str = ""
    players: dict[int, Player] = field(default_factory=dict)
    started: bool = False
    phase: str = PHASE_LOBBY
    round_no: int = 0
    registration_open: bool = False
    registration_extensions: int = 0
    registration_message_id: int | None = None
    night_votes: dict[int, int] = field(default_factory=dict)
    night_skipped_user_ids: set[int] = field(default_factory=set)
    mafia_vote_locked: bool = False
    mafia_target_announced: bool = False
    announced_night_roles: set[str] = field(default_factory=set)
    last_don_successor_id: int | None = None
    day_stage: str | None = None
    day_votes: dict[int, int] = field(default_factory=dict)
    trial_candidate_id: int | None = None
    trial_vote_message_id: int | None = None
    trial_votes: dict[int, bool] = field(default_factory=dict)
    night_kill_sources: dict[int, list[str]] = field(default_factory=dict)
    day_silenced_user_id: int | None = None
    doctor_target_id: int | None = None
    doctor_self_heal_used: bool = False
    lucky_save_used: bool = False
    commissar_action_mode: str | None = None
    commissar_target_id: int | None = None
    commissar_shot_target_id: int | None = None
    commissar_known_roles: dict[int, str] = field(default_factory=dict)
    pending_sergeant_check: dict[str, object] | None = None
    advocate_target_id: int | None = None
    maniac_target_id: int | None = None
    mistress_target_id: int | None = None
    mistress_last_target_id: int | None = None
    bum_target_id: int | None = None
    bum_last_target_id: int | None = None
    kamikaze_pending_user_id: int | None = None
    kamikaze_target_id: int | None = None
    documented_user_ids: set[int] = field(default_factory=set)
    spent_documents_user_ids: set[int] = field(default_factory=set)
    shielded_user_ids: set[int] = field(default_factory=set)
    spent_shield_user_ids: set[int] = field(default_factory=set)
    active_role_queued_user_ids: set[int] = field(default_factory=set)
    active_role_triggered_user_ids: set[int] = field(default_factory=set)
    active_role_failed_user_ids: set[int] = field(default_factory=set)
    night_missed_streaks: dict[int, int] = field(default_factory=dict)
    afk_killed_user_ids: set[int] = field(default_factory=set)
    night_reports: dict[int, list[str]] = field(default_factory=dict)
    pending_last_words: set[int] = field(default_factory=set)
    used_last_words: set[int] = field(default_factory=set)
    last_words_log: dict[int, str] = field(default_factory=dict)
    last_doctor_saved_target_id: int | None = None
    phase_started_at: datetime | None = None
    phase_duration_seconds: int | None = None
    stats_recorded: bool = False
    suicide_winners: set[int] = field(default_factory=set)
    winner_team: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


# Route extracted behavior through focused domain modules while keeping the
# public GameRoom API unchanged.
GameRoom.add_player = add_player
GameRoom.open_registration = open_registration
GameRoom.extend_registration = extend_registration
GameRoom.close_registration = close_registration
GameRoom.remove_player = remove_player
GameRoom.assign_roles = assign_roles
GameRoom.build_roles = staticmethod(build_roles)

GameRoom.get_player = get_player
GameRoom.alive_players = alive_players
GameRoom.alive_mafia = alive_mafia
GameRoom.alive_mafia_ids = alive_mafia_ids
GameRoom.all_mafia_voted = all_mafia_voted
GameRoom._active_mafia_votes = _active_mafia_votes
GameRoom._choose_mafia_target = _choose_mafia_target
GameRoom.current_mafia_target_id = current_mafia_target_id
GameRoom.mark_night_role_announced = mark_night_role_announced
GameRoom.transfer_don_if_needed = transfer_don_if_needed
GameRoom.transfer_commissar_if_needed = transfer_commissar_if_needed
GameRoom.remember_commissar_check = remember_commissar_check
GameRoom.set_pending_sergeant_check = set_pending_sergeant_check
GameRoom.pop_pending_sergeant_check = pop_pending_sergeant_check
GameRoom.forget_dead_commissar_checks = forget_dead_commissar_checks
GameRoom.alive_civilians = alive_civilians
GameRoom.check_winner = check_winner

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

GameRoom.seat_number = seat_number
GameRoom.anonymous_player_label = anonymous_player_label
GameRoom.public_player_mark = public_player_mark
GameRoom.commissar_check_result_text = commissar_check_result_text
GameRoom.pop_night_reports = pop_night_reports
GameRoom.add_night_report_line = add_night_report_line
GameRoom.queue_last_words = queue_last_words
GameRoom.can_send_last_word = can_send_last_word
GameRoom.consume_last_word = consume_last_word
GameRoom.set_day_vote = set_day_vote
GameRoom.resolve_day = resolve_day
GameRoom.end_day_without_votes = end_day_without_votes
GameRoom.pop_night_kill_sources = pop_night_kill_sources
GameRoom.alive_role_counts_text = alive_role_counts_text
GameRoom.alive_players_text = alive_players_text
GameRoom.alive_role_hints_text = alive_role_hints_text
GameRoom.game_duration_text = game_duration_text
GameRoom.final_report_text = final_report_text
GameRoom.night_intro_text = night_intro_text
GameRoom.night_media_caption = night_media_caption
GameRoom.day_intro_text = day_intro_text
GameRoom.day_media_caption = day_media_caption
GameRoom.status_text = status_text
GameRoom.lobby_text = lobby_text

__all__ = ["Player", "GameRoom"]
