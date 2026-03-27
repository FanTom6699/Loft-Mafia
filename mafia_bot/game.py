import random
from dataclasses import dataclass, field
from datetime import datetime


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
ROLE_SUICIDE = "Самоубийца"
ROLE_LUCKY = "Счастливчик"
ROLE_KAMIKAZE = "Камикадзе"
ROLE_CITIZEN = "Мирный житель"

MAFIA_ROLES = {ROLE_DON, ROLE_MAFIA}

ROLE_EMOJI = {
    ROLE_DON: "🤵🏻",
    ROLE_MAFIA: "🤵🏼",
    ROLE_MANIAC: "🔪",
    ROLE_COMMISSAR: "🕵️",
    ROLE_DOCTOR: "👨🏼‍⚕️",
    ROLE_MISTRESS: "💃",
    ROLE_BUM: "🧥",
    ROLE_SUICIDE: "💣",
    ROLE_LUCKY: "🤞",
    ROLE_KAMIKAZE: "🧨",
    ROLE_CITIZEN: "👨🏼",
}

ROLE_DESCRIPTION = {
    ROLE_DON: (
        "Ты руководишь мафиозной стороной. "
        "Ночью вместе с мафией выбираешь цель и ведешь команду к победе."
    ),
    ROLE_MAFIA: (
        "Ты в мафиозной команде. "
        "Твоя задача - ночью помогать устранять соперников и дожать город днем."
    ),
    ROLE_MANIAC: (
        "Ты играешь сам за себя. "
        "Выживи до конца и доведи партию до хаоса, где останешься только ты."
    ),
    ROLE_COMMISSAR: (
        "Ты главный сыщик города. "
        "Ночью проверяешь игроков и ищешь тех, кто связан с мафией."
    ),
    ROLE_DOCTOR: (
        "Ты ночной спасатель. "
        "Выбирай, кого защитить, и не дай убийцам сократить ряды мирных."
    ),
    ROLE_MISTRESS: (
        "Ты мастер отвлечения. "
        "Сбивай планы опасных ролей и помогай городу пережить ночи."
    ),
    ROLE_BUM: (
        "Ты случайный свидетель ночных событий. "
        "Оказывайся рядом с нужными людьми и собирай ценную информацию."
    ),
    ROLE_SUICIDE: (
        "Твоя цель - погибнуть на дневном голосовании. "
        "Чем убедительнее сыграешь, тем быстрее выполнишь личное условие победы."
    ),
    ROLE_LUCKY: (
        "Ты обычный житель, но фортуна на твоей стороне. "
        "Иногда удача спасает тебя от ночной атаки."
    ),
    ROLE_KAMIKAZE: (
        "Если тебя казнят днем, ты сможешь утащить за собой одного игрока. "
        "Используй этот риск как рычаг давления в обсуждении."
    ),
    ROLE_CITIZEN: (
        "Ты мирный житель. "
        "Слушай обсуждения, сопоставляй факты и голосуй против преступников."
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
    ROLE_SUICIDE: "Ход: день. Цель: быть казненным на дневном голосовании для личной победы.",
    ROLE_LUCKY: "Пассивно: при ночной атаке имеет шанс 50% выжить.",
    ROLE_KAMIKAZE: "Пассивно: если его казнят днем, случайно забирает с собой еще одного игрока.",
    ROLE_CITIZEN: "Ход: день. Действие: участвует в обсуждении и голосованиях.",
}


def role_card_text(role: str, chat_title: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    header = f"{emoji} {role}".strip()
    description = ROLE_DESCRIPTION.get(role, "У этой роли пока нет описания.")
    action_rule = ROLE_ACTION_RULES.get(role, "Механика роли пока не добавлена.")
    return (
        "<b>Твоя роль</b>\n"
        f"{header}\n\n"
        f"{description}\n\n"
        f"<b>Как ходит роль</b>\n{action_rule}\n\n"
        f"Чат: {chat_title}"
    )


def all_roles_info_text() -> str:
    ordered_roles = [
        ROLE_DON,
        ROLE_MAFIA,
        ROLE_MANIAC,
        ROLE_COMMISSAR,
        ROLE_DOCTOR,
        ROLE_MISTRESS,
        ROLE_BUM,
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
    4: [ROLE_DON, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_CITIZEN],
    5: [ROLE_DON, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_LUCKY, ROLE_CITIZEN],
    6: [ROLE_DON, ROLE_MAFIA, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_CITIZEN],
    7: [ROLE_DON, ROLE_MAFIA, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_CITIZEN],
    8: [ROLE_DON, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_LUCKY, ROLE_BUM, ROLE_CITIZEN],
    9: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_LUCKY, ROLE_BUM, ROLE_CITIZEN],
    10: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_CITIZEN],
    11: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_CITIZEN],
    12: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN],
    13: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN],
    14: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    15: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    16: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    17: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    18: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    19: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
    20: [ROLE_DON, ROLE_MAFIA, ROLE_MAFIA, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_MISTRESS, ROLE_LUCKY, ROLE_BUM, ROLE_SUICIDE, ROLE_KAMIKAZE, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN, ROLE_CITIZEN],
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
    commissar_target_id: int | None = None
    maniac_target_id: int | None = None
    mistress_target_id: int | None = None
    bum_target_id: int | None = None
    night_reports: dict[int, list[str]] = field(default_factory=dict)
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
        self.commissar_target_id = None
        self.maniac_target_id = None
        self.mistress_target_id = None
        self.bum_target_id = None
        self.night_reports.clear()
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
            self.winner_team = "Маньяк"
            self.finished_at = datetime.now()
            return "Маньяк"

        if mafia_count == 0 and maniac_count == 0:
            self.phase = PHASE_FINISHED
            self.winner_team = "Мирные жители"
            self.finished_at = datetime.now()
            return "Мирные"

        if mafia_count > 0 and maniac_count == 0 and mafia_count >= civ_count:
            self.phase = PHASE_FINISHED
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
        if commissar_alive and self.commissar_target_id is None:
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
        self.day_silenced_user_id = None
        self.doctor_target_id = None
        self.commissar_target_id = None
        self.maniac_target_id = None
        self.mistress_target_id = None
        self.bum_target_id = None

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

    def resolve_day_trial(self) -> tuple[bool, str, list[Player], str | None, int | None]:
        if self.phase != PHASE_DAY or self.day_stage != DAY_STAGE_TRIAL:
            return False, "Сейчас не идет этап повешения.", [], None, None

        eliminated: list[Player] = []
        don_transfer_note: str | None = None
        don_successor_id: int | None = None

        candidate = self.get_player(self.trial_candidate_id) if self.trial_candidate_id is not None else None
        yes_count, no_count = self.trial_vote_counts()
        if candidate is not None and candidate.alive and yes_count > no_count:
            candidate.alive = False
            eliminated.append(candidate)

            if candidate.role == ROLE_SUICIDE:
                self.suicide_winners.add(candidate.user_id)

            if candidate.role == ROLE_KAMIKAZE:
                candidates = [p for p in self.alive_players() if p.user_id != candidate.user_id]
                if candidates:
                    extra = random.choice(candidates)
                    extra.alive = False
                    eliminated.append(extra)

            if candidate.role == ROLE_DON:
                don_transfer_result = self.transfer_don_if_needed("казни Дона на голосовании")
                if don_transfer_result is not None:
                    don_transfer_note, don_successor_id = don_transfer_result

        self._reset_for_night_transition()

        winner = self.check_winner()
        if winner:
            return True, f"Игра окончена. Победила команда: {winner}.", eliminated, don_transfer_note, don_successor_id

        self.phase = PHASE_NIGHT
        self.round_no += 1
        if not eliminated:
            return True, "Большинством голосов игрока оставили в живых. Наступает ночь.", [], don_transfer_note, don_successor_id
        return True, "По итогам голосования игрок повешен. Наступает ночь.", eliminated, don_transfer_note, don_successor_id

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

        if not target.alive:
            return False, "Цель уже выбыла."
        if target.user_id == commissar.user_id:
            return False, "Нельзя проверить себя."

        self.commissar_target_id = target_user_id
        return True, "Проверка принята. Результат будет утром."

    def resolve_night(self) -> tuple[bool, str, list[Player], str | None, int | None]:
        if self.phase != PHASE_NIGHT:
            return False, "Сейчас не ночь.", [], None, None

        self.night_reports.clear()
        self.day_silenced_user_id = None

        mistress = next((p for p in self.alive_players() if p.role == ROLE_MISTRESS), None)
        blocked_user_id_initial: int | None = None
        if mistress is not None and self.mistress_target_id is not None:
            blocked_user_id_initial = self.mistress_target_id

        doctor = next((p for p in self.alive_players() if p.role == ROLE_DOCTOR), None)
        doctor_target_id: int | None = None
        if (
            doctor is not None
            and self.doctor_target_id is not None
            and (blocked_user_id_initial is None or doctor.user_id != blocked_user_id_initial)
        ):
            target = self.get_player(self.doctor_target_id)
            if target is not None and target.alive:
                doctor_target_id = target.user_id

        # Mistress block can be removed if doctor treated the same target this night.
        blocked_user_id = blocked_user_id_initial
        if blocked_user_id is not None and doctor_target_id == blocked_user_id:
            blocked_user_id = None

        if blocked_user_id is not None:
            blocked_player = self.get_player(blocked_user_id)
            if blocked_player is not None and blocked_player.alive:
                self.day_silenced_user_id = blocked_user_id

        if blocked_user_id_initial is not None:
            blocked_target = self.get_player(blocked_user_id_initial)
            if blocked_target is not None and blocked_target.alive:
                self.add_night_report_line(blocked_target.user_id, "💃 Ночью к тебе приходила Любовница.")

        if doctor_target_id is not None:
            healed_target = self.get_player(doctor_target_id)
            if healed_target is not None and healed_target.alive:
                self.add_night_report_line(healed_target.user_id, "👨🏼‍⚕️ Ночью к тебе приходил Доктор.")

        mafia_votes = self._active_mafia_votes(blocked_user_id=blocked_user_id)
        mafia_target_id = self._choose_mafia_target(mafia_votes)

        maniac = next((p for p in self.alive_players() if p.role == ROLE_MANIAC), None)
        maniac_target_id: int | None = None
        if (
            maniac is not None
            and self.maniac_target_id is not None
            and (blocked_user_id is None or maniac.user_id != blocked_user_id)
        ):
            target = self.get_player(self.maniac_target_id)
            if target is not None and target.alive and target.user_id != maniac.user_id:
                maniac_target_id = target.user_id

        commissar = next((p for p in self.alive_players() if p.role == ROLE_COMMISSAR), None)
        if commissar is not None and self.commissar_target_id is not None:
            if blocked_user_id is not None and commissar.user_id == blocked_user_id:
                self.add_night_report_line(commissar.user_id, "Тебя отвлекли этой ночью. Проверка сорвалась.")
            else:
                checked = self.get_player(self.commissar_target_id)
                if checked is not None and checked.alive:
                    self.add_night_report_line(checked.user_id, "🕵️ Ночью тебя проверял Комиссар.")
                    if checked.role in MAFIA_ROLES:
                        self.add_night_report_line(
                            commissar.user_id,
                            f"Проверка: {checked.full_name} выглядит подозрительно.",
                        )
                    else:
                        self.add_night_report_line(
                            commissar.user_id,
                            f"Проверка: {checked.full_name} не замечен в связях с мафией.",
                        )

        attacks: dict[int, list[str]] = {}
        if mafia_target_id is not None:
            attacks.setdefault(mafia_target_id, []).append("мафия")
        if maniac_target_id is not None:
            attacks.setdefault(maniac_target_id, []).append("маньяк")

        eliminated: list[Player] = []
        self.night_kill_sources.clear()
        for target_id in attacks:
            target = self.get_player(target_id)
            if target is None or not target.alive:
                continue

            if doctor_target_id == target.user_id:
                continue
            if target.role == ROLE_LUCKY and random.random() < 0.5:
                continue

            target.alive = False
            eliminated.append(target)
            self.night_kill_sources[target.user_id] = attacks[target_id]

        don_transfer_note: str | None = None
        don_successor_id: int | None = None
        if any(player.role == ROLE_DON for player in eliminated):
            don_transfer_result = self.transfer_don_if_needed("убийства Дона ночью")
            if don_transfer_result is not None:
                don_transfer_note, don_successor_id = don_transfer_result

        bum = next((p for p in self.alive_players() if p.role == ROLE_BUM), None)
        if bum is not None and self.bum_target_id is not None:
            if blocked_user_id is None or bum.user_id != blocked_user_id:
                observed = self.get_player(self.bum_target_id)
                if observed is not None:
                    if observed.alive:
                        self.add_night_report_line(observed.user_id, "🧥 Ночью рядом с тобой крутился Бомж.")
                    notes: list[str] = []
                    if observed.user_id == mafia_target_id:
                        notes.append("У дома цели крутились подозрительные люди.")
                    if observed.user_id == maniac_target_id:
                        notes.append("Рядом заметили одинокую фигуру с ножом.")
                    if observed.user_id == doctor_target_id:
                        notes.append("К цели заходил ночной медик.")
                    if not notes:
                        notes.append("Ночь прошла тихо, явных событий у цели не было.")
                    self.add_night_report_line(bum.user_id, f"Наблюдение за {observed.full_name}:")
                    for note in notes:
                        self.add_night_report_line(bum.user_id, note)

        self.night_votes.clear()
        self.mafia_vote_locked = False
        self.mafia_target_announced = False
        self.announced_night_roles.clear()
        self.day_stage = DAY_STAGE_DISCUSSION
        self.day_votes.clear()
        self.trial_candidate_id = None
        self.trial_votes.clear()
        self.doctor_target_id = None
        self.commissar_target_id = None
        self.maniac_target_id = None
        self.mistress_target_id = None
        self.bum_target_id = None

        winner = self.check_winner()
        if winner:
            return True, f"Игра окончена. Победила команда: {winner}.", eliminated, don_transfer_note, don_successor_id

        self.phase = PHASE_DAY
        if not eliminated:
            return True, "Удивительно, но этой ночью все выжили.", [], don_transfer_note, don_successor_id
        return True, "Ночь окончена. Наступает день.", eliminated, don_transfer_note, don_successor_id

    def pop_night_reports(self) -> dict[int, list[str]]:
        reports = self.night_reports.copy()
        self.night_reports.clear()
        return reports

    def add_night_report_line(self, user_id: int, line: str) -> None:
        self.night_reports.setdefault(user_id, []).append(line)

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
        self.maniac_target_id = None
        self.mistress_target_id = None
        self.bum_target_id = None

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
            parts.append(f"{ROLE_EMOJI.get(role, '')} {role} x{count}".strip())
        return "Кто-то из них:\n" + "\n".join(parts)

    def alive_players_text(self) -> str:
        alive = self.alive_players()
        if not alive:
            return "Живых игроков нет."

        lines = [f"Живые игроки: {len(alive)}"]
        for i, player in enumerate(alive, start=1):
            lines.append(f"{i}. {player.full_name}")
        return "\n".join(lines)

    def alive_role_hints_text(self) -> str:
        roles = {player.role for player in self.alive_players()}
        if not roles:
            return ""

        parts = [f"{ROLE_EMOJI.get(role, '')} {role}".strip() for role in sorted(roles)]
        return (
            "Кто-то из них:\n"
            + ", ".join(parts)
            + f"\nВсего: {len(self.alive_players())} чел."
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
            lines.append(f"  {p.full_name} - {ROLE_EMOJI.get(p.role, '')} {p.role}".rstrip())

        lines.extend(["", "Остальные участники:"])
        for p in others:
            lines.append(f"  {p.full_name} - {ROLE_EMOJI.get(p.role, '')} {p.role}".rstrip())

        if self.suicide_winners:
            lines.extend(["", "Личная победа самоубийцы:"])
            for player in self.players.values():
                if player.user_id in self.suicide_winners:
                    lines.append(f"  {player.full_name}")

        lines.extend(["", f"Игра длилась: {self.game_duration_text()}"])
        return "\n".join(lines)

    def night_intro_text(self) -> str:
        return (
            "🌃 Наступает ночь\n"
            "На улицы города выходят лишь самые отважные и бесстрашные.\n"
            "Утром попробуем сосчитать их головы...\n\n"
            f"{self.alive_players_text()}\n\n"
            "🕵️ Комиссар Каттани ушел искать злодеев...\n"
            "👨🏼‍⚕️ Доктор вышел на ночное дежурство...\n"
            "🤵🏻 Мафия выбирает жертву..."
        )

    def day_intro_text(self) -> str:
        return (
            f"🏙 День {self.round_no}\n"
            "Солнце всходит, подсушивая на тротуарах пролитую ночью кровь..."
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
