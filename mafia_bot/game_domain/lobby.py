"""Lobby and registration behavior for the Mafia game domain."""

from __future__ import annotations

from .constants import MAX_PLAYERS, PHASE_LOBBY


def add_player(self, user_id: int, full_name: str) -> tuple[bool, str]:
    if self.started:
        return False, "Игра уже началась."
    if not self.registration_open:
        return False, "Регистрация закрыта."
    if user_id in self.players:
        self.players[user_id].full_name = full_name
        return False, "Ты уже в лобби."
    if len(self.players) >= MAX_PLAYERS:
        return False, "Лобби уже заполнено."

    from .models import Player

    self.players[user_id] = Player(user_id=user_id, full_name=full_name)
    return True, "Игрок добавлен."


def open_registration(self) -> None:
    """Open a fresh lobby and reset per-game runtime state."""
    self.registration_open = True
    self.registration_extensions = 0
    self.phase = PHASE_LOBBY
    self.registration_message_id = None
    self.winner_team = None
    self.finished_at = None
    self.suicide_winners.clear()
    self.last_don_successor_id = None
    self.started = False
    self.round_no = 0
    self.day_stage = None
    self.night_votes.clear()
    self.night_skipped_user_ids.clear()
    self.mafia_vote_locked = False
    self.mafia_target_announced = False
    self.announced_night_roles.clear()
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


def extend_registration(self) -> None:
    self.registration_extensions += 1


def close_registration(self) -> None:
    self.registration_open = False


def remove_player(self, user_id: int) -> tuple[bool, str]:
    if user_id not in self.players:
        return False, "Тебя нет в лобби."
    del self.players[user_id]
    return True, "Игрок удален из лобби."
