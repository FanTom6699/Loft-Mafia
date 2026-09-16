"""Player-state, team-state, and succession helpers for the Mafia domain."""

from __future__ import annotations

from datetime import datetime

from .constants import (
    PHASE_FINISHED,
    ROLE_CITIZEN,
    ROLE_COMMISSAR,
    ROLE_DON,
    ROLE_MAFIA,
    ROLE_MANIAC,
    ROLE_SERGEANT,
    MAFIA_ROLES,
)


def get_player(self, user_id: int):
    return self.players.get(user_id)


def alive_players(self):
    return [player for player in self.players.values() if player.alive]


def alive_mafia(self):
    return [player for player in self.alive_players() if player.role in MAFIA_ROLES]


def alive_mafia_ids(self) -> set[int]:
    return {player.user_id for player in self.alive_mafia()}


def all_mafia_voted(self) -> bool:
    alive_ids = self.alive_mafia_ids()
    if not alive_ids:
        return False
    voted_ids = {user_id for user_id in self.night_votes if user_id in alive_ids}
    return voted_ids == alive_ids


def _active_mafia_votes(self, blocked_user_id: int | None = None) -> dict[int, int]:
    votes: dict[int, int] = {}
    for mafia_user_id, target_id in self.night_votes.items():
        voter = self.get_player(mafia_user_id)
        if voter is None or not voter.alive or voter.role not in MAFIA_ROLES:
            continue
        if blocked_user_id is not None and mafia_user_id == blocked_user_id:
            continue

        target = self.get_player(target_id)
        if target is None or not target.alive:
            continue
        votes[mafia_user_id] = target.user_id
    return votes


def _choose_mafia_target(self, mafia_votes: dict[int, int]) -> int | None:
    if not mafia_votes:
        return None

    unique_targets = set(mafia_votes.values())
    if len(unique_targets) == 1:
        return next(iter(unique_targets))

    don = next((p for p in self.alive_mafia() if p.role == ROLE_DON), None)
    if don is not None and don.user_id in mafia_votes:
        return mafia_votes[don.user_id]

    votes_by_target: dict[int, int] = {}
    for target_id in mafia_votes.values():
        votes_by_target[target_id] = votes_by_target.get(target_id, 0) + 1
    return max(votes_by_target, key=votes_by_target.get)


def current_mafia_target_id(self) -> int | None:
    mafia_votes = self._active_mafia_votes()
    return self._choose_mafia_target(mafia_votes)


def mark_night_role_announced(self, role: str) -> bool:
    if role in self.announced_night_roles:
        return False
    self.announced_night_roles.add(role)
    return True


def transfer_don_if_needed(self, reason: str) -> tuple[str, int] | None:
    alive_don = next((p for p in self.alive_players() if p.role == ROLE_DON), None)
    if alive_don is not None:
        self.last_don_successor_id = None
        return None

    candidates = [p for p in self.alive_players() if p.role == ROLE_MAFIA]
    if not candidates:
        self.last_don_successor_id = None
        return None

    # Keep succession deterministic: the smallest user_id among alive mafia becomes new Don.
    new_don = min(candidates, key=lambda p: p.user_id)
    new_don.role = ROLE_DON
    self.last_don_successor_id = new_don.user_id
    return f"После {reason} новым Доном становится {self.public_player_mark(new_don)}.", new_don.user_id


def transfer_commissar_if_needed(self) -> tuple[str, int] | None:
    alive_commissar = next((p for p in self.alive_players() if p.role == ROLE_COMMISSAR), None)
    if alive_commissar is not None:
        return None

    candidates = [p for p in self.alive_players() if p.role == ROLE_SERGEANT]
    if not candidates:
        return None

    # Keep succession deterministic: the smallest user_id among alive sergeants becomes commissar.
    new_commissar = min(candidates, key=lambda p: p.user_id)
    new_commissar.role = ROLE_COMMISSAR
    return "👮🏼‍♂️ Сержант унаследовал роль 🕵️‍ Комиссар Каттани", new_commissar.user_id


def remember_commissar_check(self, target_user_id: int, result_role: str) -> None:
    self.commissar_known_roles[target_user_id] = result_role


def set_pending_sergeant_check(self, target_user_id: int, result_role: str) -> None:
    self.pending_sergeant_check = {
        "target_user_id": int(target_user_id),
        "result_role": str(result_role),
    }


def pop_pending_sergeant_check(self) -> dict[str, object] | None:
    payload = self.pending_sergeant_check
    self.pending_sergeant_check = None
    return payload


def forget_dead_commissar_checks(self) -> None:
    alive_ids = {player.user_id for player in self.alive_players()}
    self.commissar_known_roles = {
        user_id: role
        for user_id, role in self.commissar_known_roles.items()
        if user_id in alive_ids
    }


def alive_civilians(self):
    return [
        player
        for player in self.alive_players()
        if player.role not in MAFIA_ROLES and player.role != ROLE_MANIAC
    ]


def check_winner(self) -> str | None:
    alive = self.alive_players()
    mafia_count = len([p for p in alive if p.role in MAFIA_ROLES])
    maniac_count = len([p for p in alive if p.role == ROLE_MANIAC])
    civ_count = len([p for p in alive if p.role not in MAFIA_ROLES and p.role != ROLE_MANIAC])

    if maniac_count == 1 and len(alive) == 1:
        self.phase = PHASE_FINISHED
        self.started = False
        self.winner_team = "Маньяк"
        self.finished_at = datetime.now()
        return "Маньяк"

    # Special neutral-win case requested by game rules:
    # if only maniac and one ordinary citizen remain, maniac wins immediately.
    if maniac_count == 1 and len(alive) == 2:
        non_maniac = next((p for p in alive if p.role != ROLE_MANIAC), None)
        if non_maniac is not None and non_maniac.role == ROLE_CITIZEN:
            self.phase = PHASE_FINISHED
            self.started = False
            self.winner_team = "Маньяк"
            self.finished_at = datetime.now()
            return "Маньяк"

    if mafia_count == 0 and maniac_count == 0:
        self.phase = PHASE_FINISHED
        self.started = False
        self.winner_team = "Мирные жители"
        self.finished_at = datetime.now()
        return "Мирные"

    if mafia_count > 0 and maniac_count == 0 and mafia_count >= civ_count:
        self.phase = PHASE_FINISHED
        self.started = False
        self.winner_team = "Мафия"
        self.finished_at = datetime.now()
        return "Мафия"
    return None


__all__ = [
    "get_player",
    "alive_players",
    "alive_mafia",
    "alive_mafia_ids",
    "all_mafia_voted",
    "_active_mafia_votes",
    "_choose_mafia_target",
    "current_mafia_target_id",
    "mark_night_role_announced",
    "transfer_don_if_needed",
    "transfer_commissar_if_needed",
    "remember_commissar_check",
    "set_pending_sergeant_check",
    "pop_pending_sergeant_check",
    "forget_dead_commissar_checks",
    "alive_civilians",
    "check_winner",
]
