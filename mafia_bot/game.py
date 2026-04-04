import random
from dataclasses import dataclass, field
from datetime import datetime
from html import escape


MIN_PLAYERS = 4
PHASE_LOBBY = "lobby"
PHASE_NIGHT = "night"
PHASE_DAY = "day"
PHASE_FINISHED = "finished"

DAY_STAGE_DISCUSSION = "discussion"
DAY_STAGE_NOMINATION = "nomination"
DAY_STAGE_TRIAL = "trial"

ROLE_DON = "Дон"
ROLE_MAFIA = "Мафия"
ROLE_MANIAC = "Маньяк"
ROLE_COMMISSAR = "Комиссар Каттани"
ROLE_DOCTOR = "Доктор"
ROLE_MISTRESS = "Любовница"
ROLE_BUM = "Бомж"
ROLE_ADVOCATE = "Адвокат"
ROLE_SERGEANT = "Сержант"
ROLE_SUICIDE = "Самоубийца"
ROLE_LUCKY = "Счастливчик"
ROLE_KAMIKAZE = "Камикадзе"
ROLE_CITIZEN = "Мирный житель"

MAFIA_ROLES = {ROLE_DON, ROLE_MAFIA}

ROLE_EMOJI = {
    ROLE_DON: "🤵🏻",
    ROLE_MAFIA: "🤵🏼",
    ROLE_MANIAC: "🔪",
    ROLE_COMMISSAR: "🕵️‍",
    ROLE_DOCTOR: "👨🏼‍⚕️",
    ROLE_MISTRESS: "💃🏼",
    ROLE_BUM: "🧙🏼‍♂️",
    ROLE_ADVOCATE: "👨🏼‍💼",
    ROLE_SERGEANT: "👮🏻",
    ROLE_SUICIDE: "☠️",
    ROLE_LUCKY: "🤞",
    ROLE_KAMIKAZE: "💣",
    ROLE_CITIZEN: "👨🏼",
}

ROLE_DESCRIPTION = {
    ROLE_DON: (
        "Ты глава мафии. Выбирай жертву и веди команду к победе."
    ),
    ROLE_COMMISSAR: (
        "Главный городской защитник. Ночью проверяй игроков и вычисляй мафию."
    ),
    ROLE_DOCTOR: (
        "Ночью лечи одного игрока и спасай мирных от убийц."
    ),
    ROLE_MISTRESS: (
        "Тебе нужно выжить в этом суровом мире. Используй свои навыки, чтобы обезвредить любого персонажа на одни сутки :)"
    ),
    ROLE_BUM: (
        "Ночью зайди за бутылкой к игроку и посмотри, кто был у него в гостях."
    ),
    ROLE_ADVOCATE: (
        "Ночью выбери игрока для защиты от проверки Комиссара."
    ),
    ROLE_SERGEANT: (
        "Ты помощник Комиссара и опора мирных жителей."
    ),
    ROLE_SUICIDE: (
        "Твоя цель - быть казненным на дневном голосовании."
    ),
    ROLE_LUCKY: (
        "Обычный мирный с удачей: при одной ночной атаке можешь выжить с шансом 50/50."
    ),
    ROLE_KAMIKAZE: (
        "Если тебя казнят днем, этой ночью ты сможешь забрать с собой одного игрока."
    ),
    ROLE_CITIZEN: (
        "Участвуй в обсуждениях и голосовании, чтобы вычислить мафию."
    ),
    ROLE_MAFIA: (
        "Слушай Дона, голосуй ночью и убирай всех, кто мешает мафии."
    ),
    ROLE_MANIAC: (
        "Ты нейтральный убийца. Для победы нужно остаться единственным выжившим."
    ),
}

ROLE_ACTION_RULES = {
    ROLE_DON: "Ход: ночь. Действие: выбирает цель вместе с мафией; при разногласии решает голос Дона.",
    ROLE_MAFIA: "Ход: ночь. Действие: голосует за цель убийства вместе с мафией.",
    ROLE_MANIAC: "Ход: ночь. Действие: выбирает одного игрока для атаки.",
    ROLE_COMMISSAR: "Ход: ночь. Действие: проверяет одного игрока на связь с мафией.",
    ROLE_DOCTOR: "Ход: ночь. Действие: лечит одного игрока, спасая от ночной атаки и снимая с него эффект Любовницы в эту ночь.",
    ROLE_MISTRESS: "Ход: ночь. Действие: блокирует действие цели на ночь и накладывает молчание в групповом чате на следующий день.",
    ROLE_BUM: "Ход: ночь. Действие: наблюдает за целью и получает отчет о ночных событиях рядом с ней.",
    ROLE_ADVOCATE: "Ход: ночь. Действие: выбирает игрока для защиты от проверки комиссара.",
    ROLE_SERGEANT: "Ход: день. Действие: участвует в обсуждении и голосованиях на стороне мирных.",
    ROLE_SUICIDE: "Ход: день. Цель: быть казненным на дневном голосовании для личной победы.",
    ROLE_LUCKY: "Пассивно: при ночной атаке имеет шанс 50% выжить.",
    ROLE_KAMIKAZE: "Пассивно: если его казнят днем, случайно забирает с собой еще одного игрока.",
    ROLE_CITIZEN: "Ход: день. Действие: участвует в обсуждении и голосованиях.",
}


def role_card_text(role: str, chat_title: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    header = f"<b>Ты - {emoji} {role}!</b>".strip()
    description = ROLE_DESCRIPTION.get(role, "У этой роли пока нет описания.")
    return f"{header}\n{description}"


def all_roles_info_text() -> str:
    ordered_roles = [
        ROLE_DON,
        ROLE_MAFIA,
        ROLE_MANIAC,
        ROLE_COMMISSAR,
        ROLE_DOCTOR,
        ROLE_MISTRESS,
        ROLE_BUM,
        ROLE_ADVOCATE,
        ROLE_SERGEANT,
        ROLE_SUICIDE,
        ROLE_LUCKY,
        ROLE_KAMIKAZE,
        ROLE_CITIZEN,
    ]
    lines = ["<b>Роли и описания</b>"]
    for role in ordered_roles:
        emoji = ROLE_EMOJI.get(role, "")
        desc = ROLE_DESCRIPTION.get(role, "Описание пока не добавлено.")
        action_rule = ROLE_ACTION_RULES.get(role, "Механика роли пока не добавлена.")
        lines.append(
            (
                f"\n{emoji} <b>{role}</b>\n"
                f"Описание: {desc}\n"
                f"Как ходит: {action_rule}"
            ).strip()
        )
    return "\n".join(lines)

ROLE_PLAN_BY_COUNT: dict[int, list[str]] = {
    4: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_DOCTOR],
    5: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_DOCTOR],
    6: [ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY],
    7: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY],
    8: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM],
    9: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM],
    10: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE],
    11: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS],
    12: [ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT],
    13: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT],
    14: [ROLE_CITIZEN, ROLE_CITIZEN, ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC],
    15: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC, ROLE_CITIZEN, ROLE_CITIZEN],
    16: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC, ROLE_ADVOCATE, ROLE_CITIZEN, ROLE_CITIZEN],
    17: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC, ROLE_ADVOCATE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    18: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC, ROLE_ADVOCATE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    19: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC, ROLE_ADVOCATE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    20: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_DOCTOR, ROLE_COMMISSAR, ROLE_LUCKY, ROLE_BUM, ROLE_KAMIKAZE, ROLE_MISTRESS, ROLE_SERGEANT, ROLE_MANIAC, ROLE_ADVOCATE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
}


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

    def commissar_check_result_text(self, checked_player) -> str:
        role = checked_player.role
        emoji = ROLE_EMOJI.get(role, "")
        safe_name = escape((checked_player.full_name or "").strip() or f"Игрок {checked_player.user_id}")
        name_link = f"<a href=\"tg://user?id={checked_player.user_id}\">{safe_name}</a>"
        return f"{name_link} - {emoji} <b>{role}</b>"
    chat_title: str = ""
    players: dict[int, Player] = field(default_factory=dict)
    started: bool = False
    phase: str = PHASE_LOBBY
    round_no: int = 0
    registration_open: bool = False
    registration_extensions: int = 0
    registration_message_id: int | None = None
    night_votes: dict[int, int] = field(default_factory=dict)
    mafia_vote_locked: bool = False
    mafia_target_announced: bool = False
    announced_night_roles: set[str] = field(default_factory=set)
    last_don_successor_id: int | None = None
    day_stage: str | None = None
    day_votes: dict[int, int] = field(default_factory=dict)
    trial_candidate_id: int | None = None
    trial_votes: dict[int, bool] = field(default_factory=dict)
    night_kill_sources: dict[int, list[str]] = field(default_factory=dict)
    day_silenced_user_id: int | None = None
    doctor_target_id: int | None = None
    doctor_self_heal_used: bool = False
    commissar_action_mode: str | None = None
    commissar_target_id: int | None = None
    commissar_shot_target_id: int | None = None
    advocate_target_id: int | None = None
    maniac_target_id: int | None = None
    mistress_target_id: int | None = None
    mistress_last_target_id: int | None = None
    bum_target_id: int | None = None
    bum_last_target_id: int | None = None
    kamikaze_pending_user_id: int | None = None
    kamikaze_target_id: int | None = None
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

    def add_player(self, user_id: int, full_name: str) -> tuple[bool, str]:
        if self.started:
            return False, "Игра уже началась."
        if not self.registration_open:
            return False, "Регистрация закрыта."
        if user_id in self.players:
            self.players[user_id].full_name = full_name
            return False, "Ты уже в лобби."
        self.players[user_id] = Player(user_id=user_id, full_name=full_name)
        return True, "Игрок добавлен."

    def open_registration(self) -> None:
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
        self.mafia_vote_locked = False
        self.mafia_target_announced = False
        self.announced_night_roles.clear()
        self.day_votes.clear()
        self.trial_candidate_id = None
        self.trial_votes.clear()
        self.night_kill_sources.clear()
        self.day_silenced_user_id = None
        self.doctor_target_id = None
        self.doctor_self_heal_used = False
        self.commissar_action_mode = None
        self.commissar_target_id = None
        self.commissar_shot_target_id = None
        self.advocate_target_id = None
        self.maniac_target_id = None
        self.mistress_target_id = None
        self.mistress_last_target_id = None
        self.bum_target_id = None
        self.bum_last_target_id = None
        self.kamikaze_pending_user_id = None
        self.kamikaze_target_id = None
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

    def assign_roles(self) -> None:
        count = len(self.players)
        roles = self.build_roles(count)
        random.shuffle(roles)

        for player, role in zip(self.players.values(), roles):
            player.role = role

        self.started = True
        self.registration_open = False
        self.phase = PHASE_NIGHT
        self.round_no = 1
        self.day_stage = None
        self.night_votes.clear()
        self.mafia_vote_locked = False
        self.mafia_target_announced = False
        self.announced_night_roles.clear()
        self.last_don_successor_id = None
        self.day_votes.clear()
        self.trial_candidate_id = None
        self.trial_votes.clear()
        self.night_kill_sources.clear()
        self.day_silenced_user_id = None
        self.doctor_target_id = None
        self.doctor_self_heal_used = False
        self.commissar_action_mode = None
        self.commissar_target_id = None
        self.commissar_shot_target_id = None
        self.advocate_target_id = None
        self.maniac_target_id = None
        self.mistress_target_id = None
        self.mistress_last_target_id = None
        self.bum_target_id = None
        self.bum_last_target_id = None
        self.kamikaze_pending_user_id = None
        self.kamikaze_target_id = None
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

    @staticmethod
    def build_roles(count: int) -> list[str]:
        if count in ROLE_PLAN_BY_COUNT:
            return ROLE_PLAN_BY_COUNT[count].copy()

        if count < MIN_PLAYERS:
            return [ROLE_DON, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_CITIZEN][:count]

        # For groups above 20, keep scaling with a close mafia/civil ratio.
        roles = ROLE_PLAN_BY_COUNT[20].copy()
        while len(roles) < count:
            mafia_count = sum(1 for role in roles if role in MAFIA_ROLES)
            target_ratio = mafia_count / len(roles)
            if target_ratio < 0.31:
                roles.append(ROLE_MAFIA)
            else:
                roles.append(ROLE_CITIZEN)
        return roles

    def get_player(self, user_id: int) -> Player | None:
        return self.players.get(user_id)

    def alive_players(self) -> list[Player]:
        return [player for player in self.players.values() if player.alive]

    def alive_mafia(self) -> list[Player]:
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
        return f"После {reason} новым Доном становится {new_don.full_name}.", new_don.user_id

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

    def alive_civilians(self) -> list[Player]:
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

    def set_night_vote(self, mafia_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."

        if self.mafia_vote_locked:
            return False, "Ты опоздал..."

        mafia_player = self.get_player(mafia_user_id)
        target_player = self.get_player(target_user_id)
        if mafia_player is None or target_player is None:
            return False, "Игрок не найден."
        if not mafia_player.alive:
            return False, "Ты выбыл из игры."
        if mafia_player.role not in MAFIA_ROLES:
            return False, "Ночью голосовать может только мафия."
        if not target_player.alive:
            return False, "Цель уже выбыла."
        if target_player.user_id == mafia_player.user_id:
            return False, "Нельзя выбрать себя."

        self.night_votes[mafia_user_id] = target_user_id
        if self.all_mafia_voted():
            self.mafia_vote_locked = True
        return True, "Ночной выбор принят."

    def set_doctor_target(self, doctor_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."

        doctor = self.get_player(doctor_user_id)
        target = self.get_player(target_user_id)
        if doctor is None or target is None:
            return False, "Игрок не найден."
        if not doctor.alive:
            return False, "Ты выбыл из игры."
        if doctor.role != ROLE_DOCTOR:
            return False, "Лечить может только доктор."
        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == doctor.user_id and self.doctor_self_heal_used:
            return False, "Ты уже лечил себя в этой игре."

        self.doctor_target_id = target_user_id
        return True, "Доктор принял вызов."

    def all_required_night_actions_done(self) -> bool:
        if self.phase != PHASE_NIGHT:
            return False

        if self.alive_mafia() and not self.all_mafia_voted():
            return False

        doctor_alive = any(p.alive and p.role == ROLE_DOCTOR for p in self.players.values())
        if doctor_alive and self.doctor_target_id is None:
            return False

        commissar_alive = any(p.alive and p.role == ROLE_COMMISSAR for p in self.players.values())
        if commissar_alive:
            if self.round_no >= 2:
                if self.commissar_action_mode is None:
                    return False
                if self.commissar_action_mode == "check" and self.commissar_target_id is None:
                    return False
                if self.commissar_action_mode == "shoot" and self.commissar_shot_target_id is None:
                    return False
            elif self.commissar_target_id is None:
                return False

        advocate_alive = any(p.alive and p.role == ROLE_ADVOCATE for p in self.players.values())
        if advocate_alive and self.advocate_target_id is None:
            return False

        maniac_alive = any(p.alive and p.role == ROLE_MANIAC for p in self.players.values())
        if maniac_alive and self.maniac_target_id is None:
            return False

        mistress_alive = any(p.alive and p.role == ROLE_MISTRESS for p in self.players.values())
        if mistress_alive and self.mistress_target_id is None:
            return False

        bum_alive = any(p.alive and p.role == ROLE_BUM for p in self.players.values())
        if bum_alive and self.bum_target_id is None:
            return False

        if self.kamikaze_pending_user_id is not None and self.kamikaze_target_id is None:
            return False

        return True

    def all_alive_day_voted(self) -> bool:
        if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_NOMINATION:
            return False

        alive_ids = {p.user_id for p in self.alive_players()}
        if not alive_ids:
            return False

        voted_ids = {user_id for user_id in self.day_votes if user_id in alive_ids}
        return voted_ids == alive_ids

    def all_alive_trial_voted(self) -> bool:
        if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_TRIAL:
            return False

        alive_ids = {p.user_id for p in self.alive_players()}
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
        self.trial_votes.clear()
        return True, "Этап голосования за/против запущен."

    def resolve_day_nomination(self) -> tuple[bool, int | None]:
        if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_NOMINATION:
            return False, None

        if not self.day_votes:
            return True, None

        votes_by_target: dict[int, int] = {}
        for target_id in self.day_votes.values():
            target = self.get_player(target_id)
            if target is None or not target.alive:
                continue
            votes_by_target[target_id] = votes_by_target.get(target_id, 0) + 1

        if not votes_by_target:
            return True, None

        max_votes = max(votes_by_target.values())
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
        self.trial_votes.clear()
        self.night_votes.clear()
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

    def resolve_day_trial(self) -> tuple[bool, str, list[Player], str | None, int | None, str | None, int | None]:
        if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_TRIAL:
            return False, "Сейчас не идет этап повешения.", [], None, None, None, None

        eliminated: list[Player] = []
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
            return (
                True,
                f"Игра окончена. Победила команда: {winner}.",
                eliminated,
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            )

        self.phase = PHASE_NIGHT
        self.round_no += 1
        if kamikaze_needs_revenge and kamikaze_user_id is not None:
            self.kamikaze_pending_user_id = kamikaze_user_id
            self.kamikaze_target_id = None
        if not eliminated:
            return (
                True,
                "Большинством голосов игрока оставили в живых. Наступает ночь.",
                [],
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            )
        return (
            True,
            "По итогам голосования игрок повешен. Наступает ночь.",
            eliminated,
            don_transfer_note,
            don_successor_id,
            commissar_transfer_note,
            commissar_successor_id,
        )

    def set_kamikaze_target(self, kamikaze_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."
        if self.kamikaze_pending_user_id != kamikaze_user_id:
            return False, "Сейчас у тебя нет доступного выбора камикадзе."

        kamikaze = self.get_player(kamikaze_user_id)
        target = self.get_player(target_user_id)
        if kamikaze is None or target is None:
            return False, "Игрок не найден."
        if kamikaze.role != ROLE_KAMIKAZE:
            return False, "Это действие доступно только камикадзе."
        if target.user_id == kamikaze.user_id:
            return False, "Нельзя выбрать себя."
        if not target.alive:
            return False, "Цель уже выбыла."

        self.kamikaze_target_id = target_user_id
        return True, "Камикадзе выбрал цель."

    def set_maniac_target(self, maniac_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."

        maniac = self.get_player(maniac_user_id)
        target = self.get_player(target_user_id)
        if maniac is None or target is None:
            return False, "Игрок не найден."
        if not maniac.alive:
            return False, "Ты выбыл из игры."
        if maniac.role != ROLE_MANIAC:
            return False, "Это действие доступно только маньяку."
        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == maniac.user_id:
            return False, "Нельзя выбрать себя."

        self.maniac_target_id = target_user_id
        return True, "Маньяк выбрал цель."

    def set_mistress_target(self, mistress_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."

        mistress = self.get_player(mistress_user_id)
        target = self.get_player(target_user_id)
        if mistress is None or target is None:
            return False, "Игрок не найден."
        if not mistress.alive:
            return False, "Ты выбыл из игры."
        if mistress.role != ROLE_MISTRESS:
            return False, "Это действие доступно только любовнице."
        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == mistress.user_id:
            return False, "Нельзя выбрать себя."
        if self.mistress_last_target_id is not None and target.user_id == self.mistress_last_target_id:
            return False, "Нельзя ходить к одному и тому же игроку две ночи подряд."

        self.mistress_target_id = target_user_id
        return True, "Любовница отвлекла цель на эту ночь."

    def set_bum_target(self, bum_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."

        bum = self.get_player(bum_user_id)
        target = self.get_player(target_user_id)
        if bum is None or target is None:
            return False, "Игрок не найден."
        if not bum.alive:
            return False, "Ты выбыл из игры."
        if bum.role != ROLE_BUM:
            return False, "Это действие доступно только бомжу."
        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == bum.user_id:
            return False, "Нельзя наблюдать за собой."
        if self.bum_last_target_id is not None and target.user_id == self.bum_last_target_id:
            return False, "Нельзя ходить к одному и тому же игроку две ночи подряд."

        self.bum_target_id = target_user_id
        return True, "Бомж отправился наблюдать за целью."

    def check_player_role(self, commissar_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Проверка доступна только ночью."

        commissar = self.get_player(commissar_user_id)
        target = self.get_player(target_user_id)
        if commissar is None or target is None:
            return False, "Игрок не найден."
        if not commissar.alive:
            return False, "Ты выбыл из игры."
        if commissar.role != ROLE_COMMISSAR:
            return False, "Проверять может только комиссар."

        if self.round_no >= 2:
            if self.commissar_action_mode is None:
                return False, "Сначала выбери: проверить или стрелять."
            if self.commissar_action_mode != "check":
                return False, "На эту ночь уже выбран режим стрельбы."
        else:
            self.commissar_action_mode = "check"

        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == commissar.user_id:
            return False, "Нельзя проверить себя."

        self.commissar_target_id = target_user_id
        return True, "Проверка принята. Результат будет утром."

    def set_commissar_action_mode(self, commissar_user_id: int, mode: str) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."
        if self.round_no < 2:
            return False, "Стрелять можно только со второй ночи."

        commissar = self.get_player(commissar_user_id)
        if commissar is None:
            return False, "Игрок не найден."
        if not commissar.alive:
            return False, "Ты выбыл из игры."
        if commissar.role != ROLE_COMMISSAR:
            return False, "Это действие доступно только комиссару."
        if mode not in {"check", "shoot"}:
            return False, "Некорректный режим действия."

        self.commissar_action_mode = mode
        self.commissar_target_id = None
        self.commissar_shot_target_id = None
        if mode == "shoot":
            return True, "Режим выбран: стрелять."
        return True, "Режим выбран: проверять."

    def set_commissar_shot_target(self, commissar_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."
        if self.round_no < 2:
            return False, "Стрелять можно только со второй ночи."
        if self.commissar_action_mode != "shoot":
            return False, "Сначала выбери режим стрельбы."

        commissar = self.get_player(commissar_user_id)
        target = self.get_player(target_user_id)
        if commissar is None or target is None:
            return False, "Игрок не найден."
        if not commissar.alive:
            return False, "Ты выбыл из игры."
        if commissar.role != ROLE_COMMISSAR:
            return False, "Стрелять может только комиссар."
        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == commissar.user_id:
            return False, "Нельзя выбрать себя."

        self.commissar_shot_target_id = target_user_id
        return True, "Комиссар выбрал цель для выстрела."

    def set_advocate_target(self, advocate_user_id: int, target_user_id: int) -> tuple[bool, str]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь."

        advocate = self.get_player(advocate_user_id)
        target = self.get_player(target_user_id)
        if advocate is None or target is None:
            return False, "Игрок не найден."
        if not advocate.alive:
            return False, "Ты выбыл из игры."
        if advocate.role != ROLE_ADVOCATE:
            return False, "Это действие доступно только адвокату."
        if not target.alive:
            return False, "Цель уже выбыла."

        self.advocate_target_id = target_user_id
        return True, "Адвокат выбрал цель для защиты."

    def resolve_night(self) -> tuple[bool, str, list[Player], str | None, int | None, str | None, int | None]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь.", [], None, None, None, None

        self.afk_killed_user_ids.clear()
        self.night_reports.clear()
        self.last_doctor_saved_target_id = None
        self.day_silenced_user_id = None

        mistress = next((p for p in self.alive_players() if p.role == ROLE_MISTRESS), None)
        mistress_target_id: int | None = None
        if mistress is not None and self.mistress_target_id is not None:
            mistress_target_id = self.mistress_target_id

        doctor = next((p for p in self.alive_players() if p.role == ROLE_DOCTOR), None)
        doctor_target_id: int | None = None
        doctor_blocked_by_mistress = (
            doctor is not None
            and mistress_target_id is not None
            and doctor.user_id == mistress_target_id
        )
        if doctor is not None and self.doctor_target_id is not None and not doctor_blocked_by_mistress:
            target = self.get_player(self.doctor_target_id)
            if target is not None and target.alive:
                doctor_target_id = target.user_id
                if target.user_id == doctor.user_id:
                    self.doctor_self_heal_used = True

        if mistress_target_id is not None:
            blocked_target = self.get_player(mistress_target_id)
            if blocked_target is not None and blocked_target.alive:
                self.add_night_report_line(
                    blocked_target.user_id,
                    "\"Ты со мною забудь обо всём...\", - пела 💃🏼 Любовница",
                )
                self.add_night_report_line(
                    blocked_target.user_id,
                    "Пока все голосуют - ты лечишься. 💃🏼 Любовница постаралась...",
                )

        if doctor_target_id is not None:
            healed_target = self.get_player(doctor_target_id)
            if healed_target is not None and healed_target.alive:
                # Add this line later so it stays the final line for the healed player.
                pass

        mafia_votes = self._active_mafia_votes()
        mafia_target_id = self._choose_mafia_target(mafia_votes)

        maniac = next((p for p in self.alive_players() if p.role == ROLE_MANIAC), None)
        maniac_target_id: int | None = None
        if maniac is not None and self.maniac_target_id is not None:
            target = self.get_player(self.maniac_target_id)
            if target is not None and target.alive and target.user_id != maniac.user_id:
                maniac_target_id = target.user_id

        commissar = next((p for p in self.alive_players() if p.role == ROLE_COMMISSAR), None)
        advocate = next((p for p in self.alive_players() if p.role == ROLE_ADVOCATE), None)
        advocate_target_id: int | None = None
        if advocate is not None and self.advocate_target_id is not None:
            protected_target = self.get_player(self.advocate_target_id)
            if protected_target is not None and protected_target.alive:
                advocate_target_id = protected_target.user_id

        commissar_check_target_id: int | None = None
        commissar_shot_target_id: int | None = None
        if commissar is not None:
            if self.round_no >= 2:
                if self.commissar_action_mode == "check":
                    commissar_check_target_id = self.commissar_target_id
                elif self.commissar_action_mode == "shoot":
                    commissar_shot_target_id = self.commissar_shot_target_id
            else:
                commissar_check_target_id = self.commissar_target_id

        if commissar is not None and commissar_check_target_id is not None:
            checked = self.get_player(commissar_check_target_id)
            if checked is not None and checked.alive:
                self.add_night_report_line(checked.user_id, "Кто-то сильно заинтересовался твоей ролью...")
                mafia_checked = checked.role in MAFIA_ROLES
                masked_by_advocate = mafia_checked and advocate_target_id == checked.user_id
                if masked_by_advocate:
                    self.add_night_report_line(checked.user_id, "Но 👨🏼‍💼 Адвокат сказал, что ты 👨🏼 Мирный житель!")
                    self.add_night_report_line(
                        commissar.user_id,
                        f"<a href=\"tg://user?id={checked.user_id}\">{escape((checked.full_name or '').strip() or f'Игрок {checked.user_id}')}</a> - 👨🏼 <b>{ROLE_CITIZEN}</b>",
                    )
                else:
                    self.add_night_report_line(
                        commissar.user_id,
                        self.commissar_check_result_text(checked),
                    )

        attacks: dict[int, list[str]] = {}
        if mafia_target_id is not None:
            attacks.setdefault(mafia_target_id, []).append("мафия")
        if maniac_target_id is not None:
            attacks.setdefault(maniac_target_id, []).append("маньяк")
        if commissar_shot_target_id is not None:
            attacks.setdefault(commissar_shot_target_id, []).append("комиссар")
        if self.kamikaze_pending_user_id is not None and self.kamikaze_target_id is not None:
            attacks.setdefault(self.kamikaze_target_id, []).append("камикадзе")

        def night_kamikaze_revenge_targets(victim_user_id: int, sources: list[str]) -> list[int]:
            targets: list[int] = []

            for source in sources:
                if source == "мафия":
                    # By game rule, from mafia side Kamikaze retaliates only against Don.
                    if don is not None and don.alive:
                        targets.append(don.user_id)

                if source == "маньяк" and maniac is not None and maniac.alive:
                    targets.append(maniac.user_id)

                if source == "комиссар" and commissar is not None and commissar.alive:
                    targets.append(commissar.user_id)

                if source == "камикадзе":
                    revenge_user_id = self.kamikaze_pending_user_id
                    if revenge_user_id is None or revenge_user_id == victim_user_id:
                        continue
                    revenge_player = self.get_player(revenge_user_id)
                    if revenge_player is not None and revenge_player.alive:
                        targets.append(revenge_user_id)

            # Preserve order and remove duplicates.
            return list(dict.fromkeys(targets))

        eliminated: list[Player] = []
        self.night_kill_sources.clear()
        pending_targets = list(attacks.keys())
        processed_targets: set[int] = set()
        while pending_targets:
            target_id = pending_targets.pop(0)
            if target_id in processed_targets:
                continue
            processed_targets.add(target_id)

            target = self.get_player(target_id)
            if target is None or not target.alive:
                continue

            if doctor_target_id == target.user_id:
                self.add_night_report_line(target.user_id, "Тебя убили :(")
                self.add_night_report_line(target.user_id, "Ты можешь отправить сюда своё предсмертное сообщение")
                self.last_doctor_saved_target_id = target.user_id

                if target.role == ROLE_KAMIKAZE:
                    sources = attacks.get(target_id, [])
                    revenge_target_ids = night_kamikaze_revenge_targets(target.user_id, sources)
                    for revenge_target_id in revenge_target_ids:
                        attacks.setdefault(revenge_target_id, []).append("камикадзе")
                        if revenge_target_id not in processed_targets:
                            pending_targets.append(revenge_target_id)
                continue
            sources = attacks.get(target_id, [])
            source_count = len(sources)
            if target.role == ROLE_LUCKY and source_count == 1 and random.random() < 0.5:
                self.add_night_report_line(target.user_id, "Этой ночью тебя пытались убить, но атака не удалась.")
                continue

            target.alive = False
            eliminated.append(target)
            self.night_kill_sources[target.user_id] = sources

            if target.role == ROLE_KAMIKAZE:
                revenge_target_ids = night_kamikaze_revenge_targets(target.user_id, sources)
                for revenge_target_id in revenge_target_ids:
                    attacks.setdefault(revenge_target_id, []).append("камикадзе")
                    if revenge_target_id not in processed_targets:
                        pending_targets.append(revenge_target_id)

        don_transfer_note: str | None = None
        don_successor_id: int | None = None
        commissar_transfer_note: str | None = None
        commissar_successor_id: int | None = None
        if any(player.role == ROLE_DON for player in eliminated):
            don_transfer_result = self.transfer_don_if_needed("убийства Дона ночью")
            if don_transfer_result is not None:
                don_transfer_note, don_successor_id = don_transfer_result
        if any(player.role == ROLE_COMMISSAR for player in eliminated):
            commissar_transfer_result = self.transfer_commissar_if_needed()
            if commissar_transfer_result is not None:
                commissar_transfer_note, commissar_successor_id = commissar_transfer_result

        # Mistress no longer blocks night actions. She only sets day silence if she survived the night.
        if mistress is not None and mistress.alive and mistress_target_id is not None:
            silenced_player = self.get_player(mistress_target_id)
            if silenced_player is not None and silenced_player.alive:
                self.day_silenced_user_id = silenced_player.user_id

        bum = next((p for p in self.alive_players() if p.role == ROLE_BUM), None)
        if bum is not None and self.bum_target_id is not None:
            observed = self.get_player(self.bum_target_id)
            if observed is not None:
                observed_name = (observed.full_name or "").strip() or f"Игрок {observed.user_id}"
                if observed.alive:
                    self.add_night_report_line(observed.user_id, "Ночью рядом с тобой крутился 🧥 Бомж.")
                visitors: list[Player] = []
                seen_ids: set[int] = set()

                def add_visitor(visitor_user_id: int | None) -> None:
                    if visitor_user_id is None:
                        return
                    if visitor_user_id == bum.user_id:
                        return
                    if visitor_user_id in seen_ids:
                        return
                    visitor = self.get_player(visitor_user_id)
                    if visitor is None:
                        return
                    seen_ids.add(visitor_user_id)
                    visitors.append(visitor)

                if doctor_target_id == observed.user_id and doctor is not None:
                    add_visitor(doctor.user_id)
                if commissar_check_target_id == observed.user_id and commissar is not None:
                    add_visitor(commissar.user_id)
                if commissar_shot_target_id == observed.user_id and commissar is not None:
                    add_visitor(commissar.user_id)
                if maniac_target_id == observed.user_id and maniac is not None:
                    add_visitor(maniac.user_id)
                if mistress_target_id == observed.user_id and mistress is not None:
                    add_visitor(mistress.user_id)
                if advocate_target_id == observed.user_id and advocate is not None:
                    add_visitor(advocate.user_id)

                don = next((p for p in self.alive_players() if p.role == ROLE_DON), None)
                if don is not None:
                    don_target_id = mafia_votes.get(don.user_id)
                    if don_target_id == observed.user_id:
                        add_visitor(don.user_id)

                if visitors:
                    visitor_names = ", ".join(
                        ((player.full_name or "").strip() or f"Игрок {player.user_id}")
                        for player in visitors
                    )
                    self.add_night_report_line(
                        bum.user_id,
                        f"Ночью ты пришёл за бутылкой к {observed_name} и увидел там {visitor_names}",
                    )
                else:
                    self.add_night_report_line(
                        bum.user_id,
                        f"Ты выпросил у {observed_name} бутылку и ушёл обратно на улицу. Ничего подозрительного не произошло.",
                    )

        # Keep doctor's confirmation as the last line in personal report.
        if doctor_target_id is not None:
            healed_target = self.get_player(doctor_target_id)
            if healed_target is not None and healed_target.alive:
                target_was_attacked = healed_target.user_id in attacks
                doctor_self_heal_without_attack = (
                    doctor is not None
                    and healed_target.user_id == doctor.user_id
                    and not target_was_attacked
                )
                if doctor_self_heal_without_attack:
                    self.add_night_report_line(
                        healed_target.user_id,
                        "Бинты, скальпель и ножницы не пригодились... И хорошо!",
                    )
                elif not target_was_attacked:
                    self.add_night_report_line(
                        healed_target.user_id,
                        "👨🏼‍⚕️ Доктор приходил к тебе в гости",
                    )
                else:
                    if doctor is not None and healed_target.user_id == doctor.user_id:
                        self.add_night_report_line(healed_target.user_id, "Ты успешно вылечил себя!")
                    else:
                        self.add_night_report_line(healed_target.user_id, "👨🏼‍⚕️ Доктор вылечил тебя")

        active_night_ids: set[int] = set()
        acted_night_ids: set[int] = set()
        for player in self.alive_players():
            if player.role in MAFIA_ROLES:
                active_night_ids.add(player.user_id)
                if player.user_id in self.night_votes:
                    acted_night_ids.add(player.user_id)
            elif player.role == ROLE_DOCTOR:
                active_night_ids.add(player.user_id)
                if self.doctor_target_id is not None:
                    acted_night_ids.add(player.user_id)
            elif player.role == ROLE_COMMISSAR:
                active_night_ids.add(player.user_id)
                if self.round_no >= 2:
                    if self.commissar_action_mode == "check" and self.commissar_target_id is not None:
                        acted_night_ids.add(player.user_id)
                    elif self.commissar_action_mode == "shoot" and self.commissar_shot_target_id is not None:
                        acted_night_ids.add(player.user_id)
                elif self.commissar_target_id is not None:
                    acted_night_ids.add(player.user_id)
            elif player.role == ROLE_ADVOCATE:
                active_night_ids.add(player.user_id)
                if self.advocate_target_id is not None:
                    acted_night_ids.add(player.user_id)
            elif player.role == ROLE_MANIAC:
                active_night_ids.add(player.user_id)
                if self.maniac_target_id is not None:
                    acted_night_ids.add(player.user_id)
            elif player.role == ROLE_MISTRESS:
                active_night_ids.add(player.user_id)
                if self.mistress_target_id is not None:
                    acted_night_ids.add(player.user_id)
            elif player.role == ROLE_BUM:
                active_night_ids.add(player.user_id)
                if self.bum_target_id is not None:
                    acted_night_ids.add(player.user_id)

        if self.kamikaze_pending_user_id is not None:
            active_night_ids.add(self.kamikaze_pending_user_id)
            if self.kamikaze_target_id is not None:
                acted_night_ids.add(self.kamikaze_pending_user_id)

        for user_id in list(self.night_missed_streaks.keys()):
            if user_id not in active_night_ids:
                self.night_missed_streaks.pop(user_id, None)

        for user_id in active_night_ids:
            if user_id in acted_night_ids:
                self.night_missed_streaks[user_id] = 0
                continue

            streak = int(self.night_missed_streaks.get(user_id, 0)) + 1
            self.night_missed_streaks[user_id] = streak
            if streak < 2:
                continue

            player = self.get_player(user_id)
            if player is None or not player.alive:
                continue
            player.alive = False
            eliminated.append(player)
            self.afk_killed_user_ids.add(player.user_id)
            self.night_missed_streaks[player.user_id] = 0

        if don_transfer_note is None and any(player.role == ROLE_DON for player in eliminated):
            don_transfer_result = self.transfer_don_if_needed("гибели Дона из-за бездействия ночью")
            if don_transfer_result is not None:
                don_transfer_note, don_successor_id = don_transfer_result

        if commissar_transfer_note is None and any(player.role == ROLE_COMMISSAR for player in eliminated):
            commissar_transfer_result = self.transfer_commissar_if_needed()
            if commissar_transfer_result is not None:
                commissar_transfer_note, commissar_successor_id = commissar_transfer_result

        self.night_votes.clear()
        self.mafia_vote_locked = False
        self.mafia_target_announced = False
        self.announced_night_roles.clear()
        self.day_stage = DAY_STAGE_DISCUSSION
        self.day_votes.clear()
        self.trial_candidate_id = None
        self.trial_votes.clear()
        self.doctor_target_id = None
        self.commissar_action_mode = None
        self.commissar_target_id = None
        self.commissar_shot_target_id = None
        self.advocate_target_id = None
        self.maniac_target_id = None
        self.mistress_last_target_id = mistress_target_id
        self.mistress_target_id = None
        self.bum_last_target_id = self.bum_target_id
        self.bum_target_id = None
        self.kamikaze_pending_user_id = None
        self.kamikaze_target_id = None

        winner = self.check_winner()
        if winner:
            return (
                True,
                f"Игра окончена. Победила команда: {winner}.",
                eliminated,
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            )

        self.phase = PHASE_DAY
        if not eliminated:
            return (
                True,
                "Удивительно, но этой ночью все выжили.",
                [],
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            )
        return (
            True,
            "Ночь окончена. Наступает день.",
            eliminated,
            don_transfer_note,
            don_successor_id,
            commissar_transfer_note,
            commissar_successor_id,
        )

    def pop_night_reports(self) -> dict[int, list[str]]:
        reports = self.night_reports.copy()
        self.night_reports.clear()
        return reports

    def add_night_report_line(self, user_id: int, line: str) -> None:
        self.night_reports.setdefault(user_id, []).append(line)

    def queue_last_words(self, players: list[Player]) -> list[int]:
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
        if self.day_stage != DAY_STAGE_NOMINATION:
            return False, "Сейчас не этап выбора кандидата."

        voter = self.get_player(voter_user_id)
        target = self.get_player(target_user_id)
        if voter is None or target is None:
            return False, "Игрок не найден."
        if not voter.alive:
            return False, "Ты выбыл из игры."
        if not target.alive:
            return False, "Цель уже выбыла."
        if voter.user_id == target.user_id:
            return False, "Нельзя голосовать за себя."

        self.day_votes[voter_user_id] = target_user_id
        return True, "Кандидат выбран."

    def resolve_day(self) -> tuple[bool, str, list[Player], str | None, int | None]:
        if self.phase != PHASE_DAY:
            return False, "Сейчас не день.", [], None, None

        if not self.day_votes:
            return False, "Дневные голоса не поданы.", [], None, None

        votes_by_target: dict[int, int] = {}
        for target_id in self.day_votes.values():
            votes_by_target[target_id] = votes_by_target.get(target_id, 0) + 1

        max_votes = max(votes_by_target.values())
        leaders = [target_id for target_id, count in votes_by_target.items() if count == max_votes]

        eliminated: list[Player] = []
        if len(leaders) == 1:
            first = self.get_player(leaders[0])
            if first and first.alive:
                first.alive = False
                eliminated.append(first)

                if first.role == ROLE_SUICIDE:
                    self.suicide_winners.add(first.user_id)

                if first.role == ROLE_KAMIKAZE:
                    candidates = [p for p in self.alive_players() if p.user_id != first.user_id]
                    if candidates:
                        extra = random.choice(candidates)
                        extra.alive = False
                        eliminated.append(extra)

        don_transfer_note: str | None = None
        don_successor_id: int | None = None
        if any(player.role == ROLE_DON for player in eliminated):
            if eliminated and eliminated[0].role == ROLE_DON:
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

    def end_day_without_votes(self) -> tuple[bool, str]:
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

        seat_positions = {p.user_id: i for i, p in enumerate(self.players.values(), start=1)}
        lines = ["<b>Живые игроки:</b>"]
        for player in sorted(alive, key=lambda p: seat_positions.get(p.user_id, 10**9)):
            seat_no = seat_positions.get(player.user_id)
            raw_name = (player.full_name or "").strip()
            fallback_name = f"Игрок {seat_no}" if seat_no is not None else f"Игрок {player.user_id}"
            safe_name = escape(raw_name if raw_name else fallback_name)
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
        winners: list[Player] = []
        others: list[Player] = []

        def player_link(player: Player) -> str:
            safe_name = escape((player.full_name or "").strip() or f"Игрок {player.user_id}")
            return f"<a href=\"tg://user?id={player.user_id}\">{safe_name}</a>"

        for player in self.players.values():
            is_winner = (
                winner == "Мафия" and player.role in MAFIA_ROLES
            ) or (
                winner == "Мирные жители" and player.role not in MAFIA_ROLES
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
            f"☀️ Наступает день {self.round_no}\n"
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
            lines.append(f"{i}. {player.full_name}")
        return "\n".join(lines)


class GameStorage:
    def __init__(self) -> None:
        self.rooms: dict[int, GameRoom] = {}

    def create_room(self, chat_id: int, host_id: int) -> tuple[bool, str]:
        if chat_id in self.rooms:
            return False, "Лобби уже существует в этом чате."
        self.rooms[chat_id] = GameRoom(chat_id=chat_id, host_id=host_id)
        return True, "Лобби создано."

    def get_room(self, chat_id: int) -> GameRoom | None:
        return self.rooms.get(chat_id)

    def close_room(self, chat_id: int) -> None:
        self.rooms.pop(chat_id, None)
