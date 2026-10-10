"""Night action validation and target selection for the Mafia domain."""

from __future__ import annotations

from .constants import PHASE_NIGHT, ROLE_ADVOCATE, ROLE_BUM, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_DON, ROLE_KAMIKAZE, ROLE_MAFIA, ROLE_MANIAC, ROLE_MISTRESS, ROLE_SERGEANT, MAFIA_ROLES
from .roles import (
    allow_team_kill_from_settings,
    commissar_can_shoot_this_round,
)


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
    if not allow_team_kill_from_settings(self.settings) and target_player.role in MAFIA_ROLES:
        return False, "Убийство союзников отключено в настройках."

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

    if commissar_can_shoot_this_round(self.settings, self.round_no):
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
    if not commissar_can_shoot_this_round(self.settings, self.round_no):
        return False, "Стрелять в эту ночь нельзя по настройкам."

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
    if not commissar_can_shoot_this_round(self.settings, self.round_no):
        return False, "Стрелять в эту ночь нельзя по настройкам."
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
    if not allow_team_kill_from_settings(self.settings) and target.role in {ROLE_COMMISSAR, ROLE_SERGEANT}:
        return False, "Убийство союзников отключено в настройках."

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
