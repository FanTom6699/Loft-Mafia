"""Day-phase flow helpers for the Mafia domain."""

from __future__ import annotations

from .constants import (
    DAY_STAGE_DISCUSSION,
    DAY_STAGE_NOMINATION,
    DAY_STAGE_TRIAL,
    PHASE_DAY,
    PHASE_NIGHT,
    ROLE_COMMISSAR,
    ROLE_DON,
    ROLE_KAMIKAZE,
    ROLE_SUICIDE,
)


def all_alive_day_voted(self) -> bool:
    if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_NOMINATION:
        return False
    alive_ids = {p.user_id for p in self.alive_players()}
    if self.day_silenced_user_id is not None:
        alive_ids.discard(self.day_silenced_user_id)
    if not alive_ids:
        return False
    voted_ids = {user_id for user_id in self.day_votes if user_id in alive_ids}
    return voted_ids == alive_ids


def all_alive_trial_voted(self) -> bool:
    if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_TRIAL:
        return False
    alive_ids = {p.user_id for p in self.alive_players()}
    if self.day_silenced_user_id is not None:
        alive_ids.discard(self.day_silenced_user_id)
    if self.trial_candidate_id is not None:
        alive_ids.discard(self.trial_candidate_id)
    if not alive_ids:
        return False
    voted_ids = {user_id for user_id in self.trial_votes if user_id in alive_ids}
    return voted_ids == alive_ids


def start_day_discussion(self) -> None:
    self.day_stage = DAY_STAGE_DISCUSSION
    self.day_votes.clear()
    self.trial_candidate_id = None
    self.trial_votes.clear()


def start_day_nomination(self) -> None:
    self.day_stage = DAY_STAGE_NOMINATION
    self.day_votes.clear()
    self.trial_candidate_id = None
    self.trial_votes.clear()


def start_day_trial(self, candidate_user_id: int) -> tuple[bool, str]:
    candidate = self.get_player(candidate_user_id)
    if candidate is None or not candidate.alive:
        return False, "Кандидат на повешение не найден."
    self.day_stage = DAY_STAGE_TRIAL
    self.trial_candidate_id = candidate_user_id
    self.trial_vote_message_id = None
    self.trial_votes.clear()
    return True, "Этап голосования за/против запущен."


def resolve_day_nomination(self) -> tuple[bool, int | None]:
    if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_NOMINATION:
        return False, None
    eligible_voter_ids = {p.user_id for p in self.alive_players()}
    if self.day_silenced_user_id is not None:
        eligible_voter_ids.discard(self.day_silenced_user_id)
    if not self.day_votes:
        return True, None
    votes_by_target: dict[int, int] = {}
    skipped_count = 0
    for voter_user_id, target_id in self.day_votes.items():
        if voter_user_id not in eligible_voter_ids:
            continue
        if target_id == 0:
            skipped_count += 1
            continue
        target = self.get_player(target_id)
        if target is None or not target.alive:
            continue
        votes_by_target[target_id] = votes_by_target.get(target_id, 0) + 1
    if not votes_by_target:
        return True, None
    max_votes = max(votes_by_target.values())
    if skipped_count > max_votes:
        return True, None
    leaders = [target_id for target_id, count in votes_by_target.items() if count == max_votes]
    if len(leaders) != 1:
        return True, None
    return True, leaders[0]


def set_trial_vote(self, voter_user_id: int, approve: bool) -> tuple[bool, str]:
    if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_TRIAL:
        return False, "Сейчас не идет голосование за/против."
    voter = self.get_player(voter_user_id)
    if voter is None:
        return False, "Игрок не найден."
    if not voter.alive:
        return False, "Ты выбыл из игры."
    if self.day_silenced_user_id is not None and voter.user_id == self.day_silenced_user_id:
        return False, '"Ты со мною забудь обо всём...", - пела 💃🏼 Любовница'
    if self.trial_candidate_id is not None and voter.user_id == self.trial_candidate_id:
        return False, "Кандидат на повешение не может голосовать за/против."
    self.trial_votes[voter_user_id] = approve
    return True, "Твой голос принят."


def trial_vote_counts(self) -> tuple[int, int]:
    yes = sum(1 for value in self.trial_votes.values() if value)
    no = sum(1 for value in self.trial_votes.values() if not value)
    return yes, no


def _reset_for_night_transition(self) -> None:
    self.day_stage = None
    self.day_votes.clear()
    self.trial_candidate_id = None
    self.trial_vote_message_id = None
    self.trial_votes.clear()
    self.night_votes.clear()
    self.night_skipped_user_ids.clear()
    self.mafia_vote_locked = False
    self.mafia_target_announced = False
    self.announced_night_roles.clear()
    self.night_kill_sources.clear()
    self.day_silenced_user_id = None
    self.doctor_target_id = None
    self.commissar_action_mode = None
    self.commissar_target_id = None
    self.commissar_shot_target_id = None
    self.advocate_target_id = None
    self.maniac_target_id = None
    self.mistress_target_id = None
    self.bum_target_id = None
    self.kamikaze_pending_user_id = None
    self.kamikaze_target_id = None
    self.phase_started_at = None
    self.phase_duration_seconds = None


def end_day_no_lynch(self) -> tuple[bool, str]:
    if self.phase != PHASE_DAY:
        return False, "Сейчас не день."
    self._reset_for_night_transition()
    winner = self.check_winner()
    if winner:
        return True, f"Игра окончена. Победила команда: {winner}."
    self.phase = PHASE_NIGHT
    self.round_no += 1
    return True, "Сегодня решили никого не вешать. Наступает ночь."


def resolve_day_trial(self) -> tuple[bool, str, list, str | None, int | None, str | None, int | None]:
    if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_TRIAL:
        return False, "Сейчас не идет этап повешения.", [], None, None, None, None
    eliminated = []
    don_transfer_note: str | None = None
    don_successor_id: int | None = None
    commissar_transfer_note: str | None = None
    commissar_successor_id: int | None = None
    candidate = self.get_player(self.trial_candidate_id) if self.trial_candidate_id is not None else None
    yes_count, no_count = self.trial_vote_counts()
    kamikaze_needs_revenge = False
    kamikaze_user_id: int | None = None
    if candidate is not None and candidate.alive and yes_count > no_count:
        candidate.alive = False
        eliminated.append(candidate)
        if candidate.role == ROLE_SUICIDE:
            self.suicide_winners.add(candidate.user_id)
        if candidate.role == ROLE_KAMIKAZE:
            kamikaze_needs_revenge = True
            kamikaze_user_id = candidate.user_id
        if candidate.role == ROLE_DON:
            don_transfer_result = self.transfer_don_if_needed("казни Дона на голосовании")
            if don_transfer_result is not None:
                don_transfer_note, don_successor_id = don_transfer_result
        if candidate.role == ROLE_COMMISSAR:
            commissar_transfer_result = self.transfer_commissar_if_needed()
            if commissar_transfer_result is not None:
                commissar_transfer_note, commissar_successor_id = commissar_transfer_result
    self._reset_for_night_transition()
    winner = self.check_winner()
    if winner:
        return (True, f"Игра окончена. Победила команда: {winner}.", eliminated, don_transfer_note, don_successor_id, commissar_transfer_note, commissar_successor_id)
    self.phase = PHASE_NIGHT
    self.round_no += 1
    if kamikaze_needs_revenge and kamikaze_user_id is not None:
        self.kamikaze_pending_user_id = kamikaze_user_id
        self.kamikaze_target_id = None
    if not eliminated:
        return (True, "Большинством голосов игрока оставили в живых. Наступает ночь.", [], don_transfer_note, don_successor_id, commissar_transfer_note, commissar_successor_id)
    return (True, "По итогам голосования игрок повешен. Наступает ночь.", eliminated, don_transfer_note, don_successor_id, commissar_transfer_note, commissar_successor_id)


__all__ = [
    "all_alive_day_voted",
    "all_alive_trial_voted",
    "start_day_discussion",
    "start_day_nomination",
    "start_day_trial",
    "resolve_day_nomination",
    "set_trial_vote",
    "trial_vote_counts",
    "_reset_for_night_transition",
    "end_day_no_lynch",
    "resolve_day_trial",
]
