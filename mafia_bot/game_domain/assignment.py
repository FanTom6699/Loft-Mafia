"""Role assignment and game-start state transition for the Mafia domain."""

from __future__ import annotations

import random
from datetime import datetime

from .constants import MIN_PLAYERS, PHASE_NIGHT
from .roles import adjust_mafia_ratio, apply_role_toggles, ROLE_PLAN_BY_COUNT, MAFIA_ROLES, ROLE_MAFIA, ROLE_DON, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_CITIZEN, MAFIA_RATIO_TARGETS


def build_roles(count: int, settings: dict | None = None) -> list[str]:
    """Build the role list using the historical game rules."""
    role_toggles = dict((settings or {}).get("roles", {}))
    mafia_ratio = str((settings or {}).get("mafia_ratio", "high"))

    if count in ROLE_PLAN_BY_COUNT:
        roles = adjust_mafia_ratio(ROLE_PLAN_BY_COUNT[count].copy(), mafia_ratio)
        return apply_role_toggles(roles, role_toggles)

    if count < MIN_PLAYERS:
        roles = [ROLE_DON, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_CITIZEN][:count]
        return apply_role_toggles(roles, role_toggles)

    roles = ROLE_PLAN_BY_COUNT[20].copy()
    roles = adjust_mafia_ratio(roles, mafia_ratio)
    divisor = MAFIA_RATIO_TARGETS.get(mafia_ratio, 3)
    while len(roles) < count:
        mafia_count = sum(1 for role in roles if role in MAFIA_ROLES)
        target_ratio = mafia_count / len(roles)
        if target_ratio < (1 / divisor):
            roles.append(ROLE_MAFIA)
        else:
            roles.append(ROLE_CITIZEN)
    return apply_role_toggles(roles, role_toggles)


def assign_roles(self) -> None:
    """Assign roles and transition a lobby into the first night."""
    roles = build_roles(len(self.players), self.settings)
    random.shuffle(roles)

    for player, role in zip(self.players.values(), roles):
        player.role = role

    self.started = True
    self.registration_open = False
    self.phase = PHASE_NIGHT
    self.round_no = 1
    self.day_stage = None
    self.night_votes.clear()
    self.night_skipped_user_ids.clear()
    self.mafia_vote_locked = False
    self.mafia_target_announced = False
    self.announced_night_roles.clear()
    self.last_don_successor_id = None
    self.day_votes.clear()
    self.trial_candidate_id = None
    self.trial_vote_message_id = None
    self.trial_votes.clear()
    self.night_kill_sources.clear()
    self.day_silenced_user_id = None
    self.doctor_target_id = None
    self.doctor_self_heal_used = False
    self.lucky_save_used = False
    self.commissar_action_mode = None
    self.commissar_target_id = None
    self.commissar_shot_target_id = None
    self.commissar_known_roles.clear()
    self.pending_sergeant_check = None
    self.advocate_target_id = None
    self.maniac_target_id = None
    self.mistress_target_id = None
    self.mistress_last_target_id = None
    self.bum_target_id = None
    self.bum_last_target_id = None
    self.kamikaze_pending_user_id = None
    self.kamikaze_target_id = None
    self.documented_user_ids.clear()
    self.spent_documents_user_ids.clear()
    self.shielded_user_ids.clear()
    self.spent_shield_user_ids.clear()
    self.night_missed_streaks.clear()
    self.afk_killed_user_ids.clear()
    self.night_reports.clear()
    self.pending_last_words.clear()
    self.used_last_words.clear()
    self.last_words_log.clear()
    self.phase_started_at = None
    self.phase_duration_seconds = None
    self.stats_recorded = False
    self.started_at = datetime.now()
