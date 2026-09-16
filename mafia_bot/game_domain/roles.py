"""Role metadata and role-selection helpers for the Mafia game."""

from html import escape
import unicodedata

from .constants import (
    GAME_MODE_CLASSIC,
    GAME_MODE_INVISIBLE,
    GAME_MODE_TITLES,
    MAFIA_RATIO_TARGETS,
    MAFIA_ROLES,
    ROLE_ADVOCATE,
    ROLE_BUM,
    ROLE_CITIZEN,
    ROLE_COMMISSAR,
    ROLE_DON,
    ROLE_DOCTOR,
    ROLE_KAMIKAZE,
    ROLE_LUCKY,
    ROLE_MAFIA,
    ROLE_MANIAC,
    ROLE_MISTRESS,
    ROLE_SERGEANT,
    ROLE_SUICIDE,
)

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
    ROLE_DON: "Ты глава мафии. Выбирай жертву и веди команду к победе.",
    ROLE_COMMISSAR: "Главный городской защитник. Ночью проверяй игроков и вычисляй мафию.",
    ROLE_DOCTOR: "Ночью лечи одного игрока и спасай мирных от убийц.",
    ROLE_MISTRESS: "Тебе нужно выжить в этом суровом мире. Используй свои навыки, чтобы обезвредить любого персонажа на одни сутки :)",
    ROLE_BUM: "Ночью зайди за бутылкой к игроку и посмотри, кто был у него в гостях.",
    ROLE_ADVOCATE: "Ночью выбери игрока для защиты от проверки Комиссара.",
    ROLE_SERGEANT: "Ты помощник Комиссара и опора мирных жителей.",
    ROLE_SUICIDE: "Твоя цель - быть казненным на дневном голосовании.",
    ROLE_LUCKY: "Обычный мирный с удачей: при одной ночной атаке можешь выжить с шансом 50/50.",
    ROLE_KAMIKAZE: "Если тебя казнят днем, этой ночью ты сможешь забрать с собой одного игрока.",
    ROLE_CITIZEN: "Участвуй в обсуждениях и голосовании, чтобы вычислить мафию.",
    ROLE_MAFIA: "Слушай Дона, голосуй ночью и убирай всех, кто мешает мафии.",
    ROLE_MANIAC: "Ты нейтральный убийца. Для победы нужно остаться единственным выжившим.",
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


def game_mode_from_settings(settings: dict | None) -> str:
    raw_mode = (settings or {}).get("game_mode")
    if raw_mode in GAME_MODE_TITLES:
        return raw_mode
    return GAME_MODE_CLASSIC


def invisible_mode_from_settings(settings: dict | None) -> bool:
    return game_mode_from_settings(settings) == GAME_MODE_INVISIBLE


def role_card_text(role: str, chat_title: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    header = f"<b>Ты - {emoji} {role}!</b>".strip()
    description = ROLE_DESCRIPTION.get(role, "У этой роли пока нет описания.")
    return f"{header}\n{description}"


def all_roles_info_text() -> str:
    ordered_roles = [
        ROLE_DON, ROLE_MAFIA, ROLE_MANIAC, ROLE_COMMISSAR, ROLE_DOCTOR,
        ROLE_MISTRESS, ROLE_BUM, ROLE_ADVOCATE, ROLE_SERGEANT, ROLE_SUICIDE,
        ROLE_LUCKY, ROLE_KAMIKAZE, ROLE_CITIZEN,
    ]
    lines = ["<b>Роли и описания</b>"]
    for role in ordered_roles:
        emoji = ROLE_EMOJI.get(role, "")
        desc = ROLE_DESCRIPTION.get(role, "Описание пока не добавлено.")
        action_rule = ROLE_ACTION_RULES.get(role, "Механика роли пока не добавлена.")
        lines.append(
            f"\n{emoji} <b>{role}</b>\nОписание: {desc}\nКак ходит: {action_rule}"
        )
    return "\n".join(lines)


def adjust_mafia_ratio(roles: list[str], mafia_ratio: str) -> list[str]:
    divisor = MAFIA_RATIO_TARGETS.get(mafia_ratio, 3)
    if not roles:
        return roles
    target_mafia_count = max(1, len(roles) // divisor)
    current_mafia_count = sum(1 for role in roles if role in MAFIA_ROLES)
    adjusted_roles = roles.copy()
    while current_mafia_count > target_mafia_count:
        for index in range(len(adjusted_roles) - 1, -1, -1):
            if adjusted_roles[index] == ROLE_MAFIA:
                adjusted_roles[index] = ROLE_CITIZEN
                current_mafia_count -= 1
                break
        else:
            break
    while current_mafia_count < target_mafia_count:
        for index in range(len(adjusted_roles) - 1, -1, -1):
            if adjusted_roles[index] == ROLE_CITIZEN:
                adjusted_roles[index] = ROLE_MAFIA
                current_mafia_count += 1
                break
        else:
            break
    return adjusted_roles


def apply_role_toggles(roles: list[str], role_toggles: dict[str, bool] | None = None) -> list[str]:
    adjusted_roles: list[str] = []
    for role in roles:
        if role_toggles is not None and role in role_toggles and not bool(role_toggles.get(role, True)):
            adjusted_roles.append(ROLE_CITIZEN)
        else:
            adjusted_roles.append(role)
    return adjusted_roles


def allow_team_kill_from_settings(settings: dict | None) -> bool:
    misc = (settings or {}).get("misc", {})
    return bool(misc.get("allow_team_kill", False))


def commissar_can_shoot_from_settings(settings: dict | None) -> bool:
    misc = (settings or {}).get("misc", {})
    return bool(misc.get("commissar_can_shoot", True))


def commissar_can_shoot_this_round(settings: dict | None, round_no: int) -> bool:
    if not commissar_can_shoot_from_settings(settings):
        return False
    if round_no >= 2:
        return True
    misc = (settings or {}).get("misc", {})
    return bool(misc.get("commissar_first_night_shot", False))


def kamikaze_night_revenge_from_settings(settings: dict | None) -> bool:
    misc = (settings or {}).get("misc", {})
    return bool(misc.get("kamikaze_night_revenge", True))


def action_notifications_from_settings(settings: dict | None) -> bool:
    misc = (settings or {}).get("misc", {})
    return bool(misc.get("action_notifications", True))


def normalize_link_display_name(name: str, fallback: str) -> str:
    normalized = str(name or "")
    normalized = normalized.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch)[0] != "C")
    normalized = " ".join(normalized.split())
    return normalized or fallback


def player_link(player) -> str:
    display_name = normalize_link_display_name(player.full_name, f"Игрок {player.user_id}")
    safe_name = escape(display_name)
    return f'<a href="tg://user?id={player.user_id}">{safe_name}</a>'
