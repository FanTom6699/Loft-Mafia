"""Presentation and report helpers extracted from the historical GameRoom."""

from __future__ import annotations

from datetime import datetime
from html import escape

from .constants import (
    DAY_STAGE_DISCUSSION,
    GAME_MODE_INVISIBLE,
    PHASE_DAY,
    PHASE_FINISHED,
    PHASE_LOBBY,
    PHASE_NIGHT,
    ROLE_ADVOCATE,
    ROLE_EMOJI,
    ROLE_MAFIA,
    ROLE_MANIAC,
)
from .roles import invisible_mode_from_settings, normalize_link_display_name, player_link


def public_player_mark(self, player) -> str:
    if invisible_mode_from_settings(self.settings):
        return escape(self.anonymous_player_label(player))
    return player_link(player)


def commissar_check_result_text(self, checked_player) -> str:
    role = checked_player.role
    emoji = ROLE_EMOJI.get(role, "")
    name_link = self.public_player_mark(checked_player)
    return f"{name_link} - {emoji} <b>{role}</b>"


def pop_night_reports(self) -> dict[int, list[str]]:
    reports = self.night_reports.copy()
    self.night_reports.clear()
    return reports


def add_night_report_line(self, user_id: int, line: str) -> None:
    report_lines = self.night_reports.setdefault(user_id, [])
    if report_lines and report_lines[-1] == line:
        return
    report_lines.append(line)


def queue_last_words(self, players) -> list[int]:
    queued: list[int] = []
    for player in players:
        if player.user_id in self.used_last_words:
            continue
        if player.user_id in self.pending_last_words:
            continue
        self.pending_last_words.add(player.user_id)
        queued.append(player.user_id)
    return queued


def can_send_last_word(self, user_id: int) -> bool:
    return user_id in self.pending_last_words


def consume_last_word(self, user_id: int, text: str) -> tuple[bool, str]:
    if user_id not in self.pending_last_words:
        return False, "Для тебя нет активного предсмертного слова."

    cleaned = text.strip()
    if not cleaned:
        return False, "Предсмертное сообщение не может быть пустым."

    self.pending_last_words.remove(user_id)
    self.used_last_words.add(user_id)
    self.last_words_log[user_id] = cleaned
    return True, cleaned


def set_day_vote(self, voter_user_id: int, target_user_id: int) -> tuple[bool, str]:
    if self.phase != PHASE_DAY:
        return False, "Сейчас не день."
    if self.day_stage != "nomination":
        return False, "Сейчас не этап выбора кандидата."

    voter = self.get_player(voter_user_id)
    target = self.get_player(target_user_id)
    if voter is None or target is None:
        return False, "Игрок не найден."
    if not voter.alive:
        return False, "Ты выбыл из игры."
    if self.day_silenced_user_id is not None and voter.user_id == self.day_silenced_user_id:
        return False, '"Ты со мною забудь обо всём...", - пела 💃🏼 Любовница'
    if not target.alive:
        return False, "Цель уже выбыла."
    if voter.user_id == target.user_id:
        return False, "Нельзя голосовать за себя."

    self.day_votes[voter_user_id] = target_user_id
    return True, "Кандидат выбран."


def resolve_day(self) -> tuple[bool, str, list, str | None, int | None]:
    if self.phase != PHASE_DAY:
        return False, "Сейчас не день.", [], None, None

    if not self.day_votes:
        return False, "Дневные голоса не поданы.", [], None, None

    votes_by_target: dict[int, int] = {}
    for target_id in self.day_votes.values():
        votes_by_target[target_id] = votes_by_target.get(target_id, 0) + 1

    max_votes = max(votes_by_target.values())
    leaders = [target_id for target_id, count in votes_by_target.items() if count == max_votes]

    eliminated = []
    if len(leaders) == 1:
        first = self.get_player(leaders[0])
        if first and first.alive:
            first.alive = False
            eliminated.append(first)
            if first.role == "Самоубийца":
                self.suicide_winners.add(first.user_id)

            if first.role == "Камикадзе":
                candidates = [p for p in self.alive_players() if p.user_id != first.user_id]
                if candidates:
                    import random
                    extra = random.choice(candidates)
                    extra.alive = False
                    eliminated.append(extra)

    self.forget_dead_commissar_checks()

    don_transfer_note: str | None = None
    don_successor_id: int | None = None
    if any(player.role == "Дон" for player in eliminated):
        if eliminated and eliminated[0].role == "Дон":
            reason = "казни Дона на голосовании"
        else:
            reason = "дневной гибели Дона"
        don_transfer_result = self.transfer_don_if_needed(reason)
        if don_transfer_result is not None:
            don_transfer_note, don_successor_id = don_transfer_result

    self.day_votes.clear()
    self.night_votes.clear()
    self.mafia_vote_locked = False
    self.mafia_target_announced = False
    self.announced_night_roles.clear()
    self.trial_vote_message_id = None
    self.doctor_target_id = None
    self.commissar_target_id = None
    self.advocate_target_id = None
    self.maniac_target_id = None
    self.mistress_target_id = None
    self.bum_target_id = None
    self.kamikaze_pending_user_id = None
    self.kamikaze_target_id = None

    winner = self.check_winner()
    if winner:
        return True, f"Игра окончена. Победила команда: {winner}.", eliminated, don_transfer_note, don_successor_id

    self.phase = PHASE_NIGHT
    self.round_no += 1
    if not eliminated:
        return True, "День окончен. Ничья по голосам, никто не выбыл. Наступает ночь.", [], don_transfer_note, don_successor_id
    return True, "День окончен. Наступает ночь.", eliminated, don_transfer_note, don_successor_id


def end_day_without_votes(self):
    return self.end_day_no_lynch()


def pop_night_kill_sources(self) -> dict[int, list[str]]:
    payload = self.night_kill_sources.copy()
    self.night_kill_sources.clear()
    return payload


def alive_role_counts_text(self) -> str:
    counts: dict[str, int] = {}
    for player in self.alive_players():
        counts[player.role] = counts.get(player.role, 0) + 1

    if not counts:
        return ""

    parts = []
    for role, count in sorted(counts.items()):
        if count == 1:
            parts.append(f"{ROLE_EMOJI.get(role, '')} <b>{role}</b>".strip())
        else:
            parts.append(f"{ROLE_EMOJI.get(role, '')} <b>{role}</b> - <b>{count}</b>".strip())
    return "Кто-то из них:\n" + "\n".join(parts)


def alive_players_text(self) -> str:
    alive = self.alive_players()
    if not alive:
        return "Живых игроков нет."

    if invisible_mode_from_settings(self.settings):
        lines = ["<b>Живые игроки:</b>"]
        for index, player in enumerate(alive, start=1):
            lines.append(f"{index}. {self.anonymous_player_label(player)}")
        return "\n".join(lines)

    seat_positions = {p.user_id: i for i, p in enumerate(self.players.values(), start=1)}
    lines = ["<b>Живые игроки:</b>"]
    for player in sorted(alive, key=lambda p: seat_positions.get(p.user_id, 10**9)):
        seat_no = seat_positions.get(player.user_id)
        raw_name = (player.full_name or "").strip()
        fallback_name = f"Игрок {seat_no}" if seat_no is not None else f"Игрок {player.user_id}"
        safe_name = escape(normalize_link_display_name(raw_name, fallback_name))
        if seat_no is None:
            lines.append(f"<a href=\"tg://user?id={player.user_id}\">{safe_name}</a>")
        else:
            lines.append(f"{seat_no}. <a href=\"tg://user?id={player.user_id}\">{safe_name}</a>")
    return "\n".join(lines)


def alive_role_hints_text(self) -> str:
    counts: dict[str, int] = {}
    for player in self.alive_players():
        counts[player.role] = counts.get(player.role, 0) + 1
    if not counts:
        return ""

    parts = []
    for role, count in sorted(counts.items()):
        if count == 1:
            parts.append(f"{ROLE_EMOJI.get(role, '')} <b>{role}</b>".strip())
        else:
            parts.append(f"{ROLE_EMOJI.get(role, '')} <b>{role}</b> - <b>{count}</b>".strip())
    return (
        "<b>Кто-то из них:</b>\n"
        + ", ".join(parts)
        + f"\nВсего: <b>{len(self.alive_players())}</b> чел."
    )


def game_duration_text(self) -> str:
    if self.started_at is None:
        return "0 мин. 0 сек."
    end_dt = self.finished_at or datetime.now()
    total_sec = max(0, int((end_dt - self.started_at).total_seconds()))
    minutes, seconds = divmod(total_sec, 60)
    return f"{minutes} мин. {seconds} сек."


def final_report_text(self) -> str:
    winner = self.winner_team or "Не определено"
    winners = []
    others = []

    for player in self.players.values():
        is_winner = (
            winner == "Мафия" and (player.role in {"Дон", ROLE_MAFIA} or player.role == ROLE_ADVOCATE)
        ) or (
            winner == "Маньяк" and player.role == ROLE_MANIAC
        ) or (
            winner == "Мирные жители"
            and player.role not in {"Дон", ROLE_MAFIA}
            and player.role != ROLE_MANIAC
            and player.role != ROLE_ADVOCATE
        )
        if is_winner and player.alive:
            winners.append(player)
        else:
            others.append(player)

    lines = ["Игра окончена!", f"Победили: {winner}", "", "Победители:"]
    for p in winners:
        lines.append(f"  {player_link(p)} - {ROLE_EMOJI.get(p.role, '')} <b>{p.role}</b>".rstrip())

    lines.extend(["", "Остальные участники:"])
    for p in others:
        lines.append(f"  {player_link(p)} - {ROLE_EMOJI.get(p.role, '')} <b>{p.role}</b>".rstrip())

    if self.suicide_winners:
        lines.extend(["", "Личная победа самоубийцы:"])
        for player in self.players.values():
            if player.user_id in self.suicide_winners:
                lines.append(f"  {player_link(player)}")

    lines.extend(["", f"Игра длилась: {self.game_duration_text()}"])
    return "\n".join(lines)


def night_intro_text(self) -> str:
    return (
        "<b>🌃 Наступает ночь</b>\n"
        "На улицы города выходят лишь самые отважные и бесстрашные.\n"
        "Утром попробуем сосчитать их головы..."
    )


def night_media_caption(self) -> str:
    return (
        "<b>🌃 Наступает ночь</b>\n"
        "На улицы города выходят лишь самые отважные и бесстрашные.\n"
        "Утром попробуем сосчитать их головы..."
    )


def day_intro_text(self) -> str:
    return (
        f"🏙 День {self.round_no}\n"
        "Солнце всходит, подсушивая на тротуарах пролитую ночью кровь..."
    )


def day_media_caption(self) -> str:
    return (
        f"<b>🏙 День {self.round_no}</b>\n"
        "Город просыпается и обсуждает события ночи."
    )


def status_text(self) -> str:
    phase_text = {
        PHASE_LOBBY: "Лобби",
        PHASE_NIGHT: "Ночь",
        PHASE_DAY: "День",
        PHASE_FINISHED: "Завершена",
    }.get(self.phase, self.phase)

    lines = [
        "<b>Статус игры</b>",
        f"Фаза: {phase_text}",
        f"Раунд: {self.round_no if self.started else 0}",
        f"Этап дня: {self.day_stage or '-'}",
        self.alive_players_text(),
    ]
    return "\n".join(lines)


def lobby_text(self) -> str:
    if not self.players:
        return "Лобби пустое."

    reg_state = "открыта" if self.registration_open else "закрыта"
    lines = [
        "<b>Лобби Мафии</b>",
        f"Игроков: {len(self.players)}",
        f"Регистрация: {reg_state}",
        f"Продлений: {self.registration_extensions}",
    ]
    for i, player in enumerate(self.players.values(), start=1):
        if invisible_mode_from_settings(self.settings):
            lines.append(f"{i}. {self.anonymous_player_label(player)}")
        else:
            lines.append(f"{i}. {player_link(player)}")
    return "\n".join(lines)
