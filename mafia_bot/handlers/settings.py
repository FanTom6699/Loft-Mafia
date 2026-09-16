# Extracted from the original mafia_bot/handlers.py.
# Stage 1 refactor: gameplay behavior is intentionally unchanged.
from ._context import *  # noqa: F401,F403

def game_mode_announcement_text(mode: str) -> str | None:
    if mode == GAME_MODE_INVISIBLE:
        return (
            "━━━━━━━━━━━━━━\n"
            "🔮 <b>РЕЖИМ «НЕВИДИМКА»</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "✨ <b>Пророчество</b>\n\n"
            "<i>Говорят, этой ночью город лишится лиц…</i>\n"
            "<i>Имена исчезнут, а правда растворится во тьме.</i>\n\n"
            "<i>Ты будешь слышать слова,</i>\n"
            "<i>но не узнаешь, кто их произнёс.</i>\n\n"
            "<i>Доверие станет иллюзией,</i>\n"
            "<i>а выбор — игрой вслепую.</i>\n\n"
            "💀 <b>Посмотрим, кто доживёт до конца…</b>\n\n"
            "──────────────\n\n"
            "📜 <b>Правила режима</b>\n\n"
            "👁 <b>Полная анонимность</b>\n"
            "• Все ники скрыты и заменены на «Невидимка»\n\n"
            "🗂 <b>Обычное начало</b>\n"
            "• Регистрация проходит с видимыми никами\n\n"
            "🎭 <b>Скрытая игра</b>\n"
            "• Во время партии скрыты личности игроков\n"
            "• Игровые сообщения и роли остаются, но без привязки к никам\n\n"
            "🔗 <b>Обезличенные списки</b>\n"
            "• Игроки и союзы отображаются без имён\n\n"
            "🔓 <b>Раскрытие после игры</b>\n"
            "• После завершения партии все личности становятся видны\n\n"
            "━━━━━━━━━━━━━━"
        )
    if mode == GAME_MODE_CLASSIC:
        return (
            "<b>Включён режим «Классика».</b>\n\n"
            "Все настройки снова можно менять вручную в меню настроек."
        )
    return None


def is_secret_voting_enabled(room) -> bool:
    return room_chat_settings(room).get("voting_mode", "open") == "secret"


def show_targets_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("show_targets", False))


def show_roles_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("show_roles", True))


def allow_team_kill_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("allow_team_kill", False))


def commissar_can_shoot_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("commissar_can_shoot", True))


def commissar_can_shoot_this_night(room) -> bool:
    settings = room_chat_settings(room).get("misc", {})
    if not bool(settings.get("commissar_can_shoot", True)):
        return False
    if getattr(room, "round_no", 0) >= 2:
        return True
    return bool(settings.get("commissar_first_night_shot", False))


def night_action_skip_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("night_action_skip", False))


def day_vote_skip_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("day_vote_skip", True))


def content_protection_enabled(room) -> bool:
    return bool(room_chat_settings(room).get("misc", {}).get("content_protection", False))


def buffs_enabled(room_or_settings) -> bool:
    if isinstance(room_or_settings, dict):
        return bool(room_or_settings.get("misc", {}).get("buffs_enabled", False))
    return bool(room_chat_settings(room_or_settings).get("misc", {}).get("buffs_enabled", False))


def invisible_mode_enabled(room_or_settings) -> bool:
    if isinstance(room_or_settings, dict):
        return invisible_mode_from_settings(room_or_settings)
    return invisible_mode_from_settings(room_chat_settings(room_or_settings))


def settings_mode_locked_message(settings: dict) -> str:
    mode_title = GAME_MODE_TITLES.get(game_mode_from_settings(settings), "Неизвестный режим")
    return (
        f"Сейчас активен режим «{mode_title}».\n"
        "Чтобы изменить настройки, переключитесь на режим «Классика»."
    )


def settings_mode_locked(settings: dict) -> bool:
    return game_mode_from_settings(settings) != GAME_MODE_CLASSIC


def apply_game_mode_preset(settings: dict, mode: str) -> dict:
    normalized = merge_chat_settings(settings)
    normalized["game_mode"] = mode
    return normalized


def private_settings_main_text(settings: dict) -> str:
    mode_title = GAME_MODE_TITLES.get(game_mode_from_settings(settings), "Классика")
    if settings_mode_locked(settings):
        return (
            "Какие параметры вы хотите изменить?\n\n"
            f"Сейчас активен режим «{mode_title}».\n"
            "Сначала переключитесь на «Классику», чтобы снова менять настройки."
        )
    return f"Какие параметры вы хотите изменить?\n\nТекущий режим: {mode_title}."


def default_chat_settings() -> dict:
    return {
        "game_mode": GAME_MODE_CLASSIC,
        "roles": {role: True for role in SETTINGS_ROLE_OPTIONS},
        "timings": {
            "registration": REGISTRATION_SECONDS,
            "night": NIGHT_PHASE_SECONDS,
            "day": DAY_DISCUSSION_SECONDS,
            "vote": DAY_NOMINATION_SECONDS,
            "trial": DAY_TRIAL_SECONDS,
        },
        "mute": {
            "dead": MUTE_DEAD_PLAYERS,
            "sleeping": MUTE_SLEEPING_PLAYERS,
            "outsiders": MUTE_NON_PLAYERS,
        },
        "misc": {
            "admin_game_only": False,
            "action_notifications": True,
            "allow_team_kill": False,
            "buffs_enabled": False,
            "commissar_can_shoot": True,
            "commissar_first_night_shot": False,
            "content_protection": False,
            "day_vote_skip": True,
            "kamikaze_night_revenge": True,
            "delete_media": False,
            "night_action_skip": False,
            "show_targets": False,
            "show_roles": True,
            "show_killers": False,
        },
        "mafia_ratio": "high",
        "voting_mode": "open",
        "leave_restriction_seconds": LEAVE_RESTRICTION_SECONDS,
    }


def merge_chat_settings(raw_settings: dict | None) -> dict:
    settings = default_chat_settings()
    if not isinstance(raw_settings, dict):
        return settings

    raw_game_mode = raw_settings.get("game_mode")
    if raw_game_mode in GAME_MODE_TITLES:
        settings["game_mode"] = raw_game_mode

    raw_roles = raw_settings.get("roles", {})
    if isinstance(raw_roles, dict):
        for role in SETTINGS_ROLE_OPTIONS:
            if role in raw_roles:
                settings["roles"][role] = bool(raw_roles[role])

    raw_timings = raw_settings.get("timings", {})
    if isinstance(raw_timings, dict):
        for key in settings["timings"]:
            raw_value = raw_timings.get(key)
            if isinstance(raw_value, int) and raw_value > 0:
                settings["timings"][key] = raw_value

    raw_mute = raw_settings.get("mute", {})
    if isinstance(raw_mute, dict):
        for key in settings["mute"]:
            if key in raw_mute:
                settings["mute"][key] = bool(raw_mute[key])

    raw_leave_restriction = raw_settings.get("leave_restriction_seconds")
    if isinstance(raw_leave_restriction, int) and raw_leave_restriction >= 0:
        settings["leave_restriction_seconds"] = raw_leave_restriction

    raw_mafia_ratio = raw_settings.get("mafia_ratio")
    if raw_mafia_ratio in SETTINGS_MAFIA_RATIO_TITLES:
        settings["mafia_ratio"] = raw_mafia_ratio

    raw_voting_mode = raw_settings.get("voting_mode")
    if raw_voting_mode in SETTINGS_VOTING_MODE_TITLES:
        settings["voting_mode"] = raw_voting_mode

    raw_misc = raw_settings.get("misc", {})
    if isinstance(raw_misc, dict):
        settings.setdefault("misc", {})
        if "admin_game_only" in raw_misc:
            settings["misc"]["admin_game_only"] = bool(raw_misc["admin_game_only"])
        if "action_notifications" in raw_misc:
            settings["misc"]["action_notifications"] = bool(raw_misc["action_notifications"])
        if "allow_team_kill" in raw_misc:
            settings["misc"]["allow_team_kill"] = bool(raw_misc["allow_team_kill"])
        if "buffs_enabled" in raw_misc:
            settings["misc"]["buffs_enabled"] = bool(raw_misc["buffs_enabled"])
        if "commissar_can_shoot" in raw_misc:
            settings["misc"]["commissar_can_shoot"] = bool(raw_misc["commissar_can_shoot"])
        if "commissar_first_night_shot" in raw_misc:
            settings["misc"]["commissar_first_night_shot"] = bool(raw_misc["commissar_first_night_shot"])
        if "content_protection" in raw_misc:
            settings["misc"]["content_protection"] = bool(raw_misc["content_protection"])
        if "day_vote_skip" in raw_misc:
            settings["misc"]["day_vote_skip"] = bool(raw_misc["day_vote_skip"])
        if "kamikaze_night_revenge" in raw_misc:
            settings["misc"]["kamikaze_night_revenge"] = bool(raw_misc["kamikaze_night_revenge"])
        if "delete_media" in raw_misc:
            settings["misc"]["delete_media"] = bool(raw_misc["delete_media"])
        if "night_action_skip" in raw_misc:
            settings["misc"]["night_action_skip"] = bool(raw_misc["night_action_skip"])
        if "show_targets" in raw_misc:
            settings["misc"]["show_targets"] = bool(raw_misc["show_targets"])
        if "show_roles" in raw_misc:
            settings["misc"]["show_roles"] = bool(raw_misc["show_roles"])
        if "show_killers" in raw_misc:
            settings["misc"]["show_killers"] = bool(raw_misc["show_killers"])

    return settings


def load_chat_settings(chat_id: int) -> dict:
    return merge_chat_settings(repo.get_chat_settings(chat_id))


def room_chat_settings(room) -> dict:
    return merge_chat_settings(getattr(room, "settings", None))


def apply_room_settings(room, settings: dict) -> dict:
    normalized = merge_chat_settings(settings)
    room.settings = normalized
    return normalized


def save_chat_settings(chat_id: int, settings: dict) -> dict:
    normalized = merge_chat_settings(settings)
    repo.save_chat_settings(chat_id, normalized)
    room = storage.get_room(chat_id)
    if room is not None:
        apply_room_settings(room, normalized)
        persist_room(room)
    return normalized


def settings_callback_data(chat_id: int, *parts: object) -> str:
    return ":".join(["psettings", str(chat_id), *(str(part) for part in parts)])


def selected_square(active: bool) -> str:
    return "⬛" if active else "⬜"


def format_settings_screen_text(text: str) -> str:
    parts = str(text).split("\n", 1)
    header = parts[0].strip()
    if not header:
        return text
    if len(parts) == 1:
        return f"<b>{header}</b>"
    body = parts[1].lstrip("\n")
    return f"<b>{header}</b>\n\n{body}"


def format_leave_duration(seconds: int) -> str:
    if seconds <= 0:
        return "Выключено"
    if seconds % 3600 == 0:
        return f"{seconds // 3600} ч."
    return f"{seconds // 60} мин."


def current_settings_timing_value(settings: dict, key: str) -> int:
    timings = settings.get("timings", {})
    if key == "registration":
        return int(timings.get("registration", REGISTRATION_SECONDS))
    if key == "night":
        return int(timings.get("night", NIGHT_PHASE_SECONDS))
    if key == "day":
        return int(timings.get("day", DAY_DISCUSSION_SECONDS))
    if key == "vote":
        return int(timings.get("vote", DAY_NOMINATION_SECONDS))
    if key == "trial":
        return int(timings.get("trial", DAY_TRIAL_SECONDS))
    return 0


def current_settings_mute_value(settings: dict, key: str) -> bool:
    mute_settings = settings.get("mute", {})
    if key == "dead":
        return bool(mute_settings.get("dead", MUTE_DEAD_PLAYERS))
    if key == "sleeping":
        return bool(mute_settings.get("sleeping", MUTE_SLEEPING_PLAYERS))
    if key == "outsiders":
        return bool(mute_settings.get("outsiders", MUTE_NON_PLAYERS))
    return False


def private_settings_main_keyboard(chat_id: int, settings: dict) -> InlineKeyboardMarkup:
    locked = settings_mode_locked(settings)

    def button(text: str, action: str) -> InlineKeyboardButton:
        if locked and action != "game_mode":
            return InlineKeyboardButton(text=f"🔒 {text}", callback_data=settings_callback_data(chat_id, "mode_locked"))
        return InlineKeyboardButton(text=text, callback_data=settings_callback_data(chat_id, action))

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🎮 Режим игры: {GAME_MODE_TITLES.get(game_mode_from_settings(settings), 'Классика')}", callback_data=settings_callback_data(chat_id, "game_mode"))],
            [button("🎭 Роли", "roles")],
            [button("🕐 Тайминги", "timings")],
            [button("🙊 Молчанка", "mute")],
            [button("🛠 Разное", "misc")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "close"))],
        ]
    )


def private_settings_game_mode_keyboard(chat_id: int, settings: dict) -> InlineKeyboardMarkup:
    current = game_mode_from_settings(settings)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Классика {selected_square(current == GAME_MODE_CLASSIC)}", callback_data=settings_callback_data(chat_id, "game_mode_set", GAME_MODE_CLASSIC))],
            [InlineKeyboardButton(text=f"Невидимка {selected_square(current == GAME_MODE_INVISIBLE)}", callback_data=settings_callback_data(chat_id, "game_mode_set", GAME_MODE_INVISIBLE))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "main"))],
        ]
    )


def private_settings_roles_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, role in enumerate(SETTINGS_ROLE_OPTIONS):
        emoji = ROLE_EMOJI.get(role, "")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🎭 Роль {emoji} {role}".strip(),
                    callback_data=settings_callback_data(chat_id, "role", idx),
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "main"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def private_settings_role_toggle_keyboard(chat_id: int, settings: dict, role_index: int) -> InlineKeyboardMarkup:
    role = SETTINGS_ROLE_OPTIONS[role_index]
    current = bool(settings.get("roles", {}).get(role, True))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Да {selected_square(current)}", callback_data=settings_callback_data(chat_id, "role_set", role_index, 1))],
            [InlineKeyboardButton(text=f"Нет {selected_square(not current)}", callback_data=settings_callback_data(chat_id, "role_set", role_index, 0))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "roles"))],
        ]
    )


def private_settings_roles_text() -> str:
    return (
        "Какую роль вы хотите настроить?\n\n"
        "Если отключить какую-либо роль, вместо неё будет добавлен Мирный житель."
    )


def private_settings_role_toggle_text(role: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    return (
        f"Требуется ли включить роль {emoji} {role}?\n\n"
        "Если отключить эту роль, вместо неё будет добавлен Мирный житель."
    ).strip()


def private_settings_timings_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"🕐 {SETTINGS_TIMING_LABELS['registration']}", callback_data=settings_callback_data(chat_id, "timing", "registration"))],
        [InlineKeyboardButton(text=f"🕐 {SETTINGS_TIMING_LABELS['night']}", callback_data=settings_callback_data(chat_id, "timing", "night"))],
        [InlineKeyboardButton(text=f"🕐 {SETTINGS_TIMING_LABELS['day']}", callback_data=settings_callback_data(chat_id, "timing", "day"))],
        [InlineKeyboardButton(text=f"🕐 {SETTINGS_TIMING_LABELS['vote']}", callback_data=settings_callback_data(chat_id, "timing", "vote"))],
        [InlineKeyboardButton(text=f"🕐 {SETTINGS_TIMING_LABELS['trial']}", callback_data=settings_callback_data(chat_id, "timing", "trial"))],
        [InlineKeyboardButton(text="🕐 Ограничение выхода", callback_data=settings_callback_data(chat_id, "leave"))],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "main"))],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def private_settings_timing_values_keyboard(chat_id: int, settings: dict, key: str) -> InlineKeyboardMarkup:
    current = current_settings_timing_value(settings, key)
    rows: list[list[InlineKeyboardButton]] = []
    for index in range(0, len(SETTINGS_TIMING_OPTIONS), 2):
        row: list[InlineKeyboardButton] = []
        for value in SETTINGS_TIMING_OPTIONS[index:index + 2]:
            row.append(
                InlineKeyboardButton(
                    text=f"{value} {selected_square(value == current)}",
                    callback_data=settings_callback_data(chat_id, "timing_set", key, value),
                )
            )
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "timings"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def private_settings_mute_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🙊 Для убитых", callback_data=settings_callback_data(chat_id, "mute_item", "dead"))],
            [InlineKeyboardButton(text="🙊 Для спящих", callback_data=settings_callback_data(chat_id, "mute_item", "sleeping"))],
            [InlineKeyboardButton(text="🙊 Для неиграющих", callback_data=settings_callback_data(chat_id, "mute_item", "outsiders"))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "main"))],
        ]
    )


def private_settings_mute_toggle_keyboard(chat_id: int, settings: dict, key: str) -> InlineKeyboardMarkup:
    current = current_settings_mute_value(settings, key)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Да {selected_square(current)}", callback_data=settings_callback_data(chat_id, "mute_set", key, 1))],
            [InlineKeyboardButton(text=f"Нет {selected_square(not current)}", callback_data=settings_callback_data(chat_id, "mute_set", key, 0))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "mute"))],
        ]
    )


def private_settings_misc_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🕵🏻 Кол-во мафии", callback_data=settings_callback_data(chat_id, "mafia_ratio"))],
            [InlineKeyboardButton(text="🙋‍♂️ Тайное голосование", callback_data=settings_callback_data(chat_id, "voting_mode"))],
            [InlineKeyboardButton(text="🚨 Оповещение о действиях", callback_data=settings_callback_data(chat_id, "misc_item", "action_notifications"))],
            [InlineKeyboardButton(text="✍️ Защита контента", callback_data=settings_callback_data(chat_id, "misc_item", "content_protection"))],
            [InlineKeyboardButton(text="👤 Показывать цели", callback_data=settings_callback_data(chat_id, "misc_item", "show_targets"))],
            [InlineKeyboardButton(text="🕵🏻 Показывать роли", callback_data=settings_callback_data(chat_id, "misc_item", "show_roles"))],
            [InlineKeyboardButton(text="☠️ Убийство союзников", callback_data=settings_callback_data(chat_id, "misc_item", "allow_team_kill"))],
            [InlineKeyboardButton(text="🔫 Комиссар Каттани стреляет", callback_data=settings_callback_data(chat_id, "misc_item", "commissar_can_shoot"))],
            [InlineKeyboardButton(text="🔫 Выстрел в первую ночь", callback_data=settings_callback_data(chat_id, "misc_item", "commissar_first_night_shot"))],
            [InlineKeyboardButton(text="🚫🌅 Пропуск дневного голосования", callback_data=settings_callback_data(chat_id, "misc_item", "day_vote_skip"))],
            [InlineKeyboardButton(text="💣 Камикадзе взрывается ночью", callback_data=settings_callback_data(chat_id, "misc_item", "kamikaze_night_revenge"))],
            [InlineKeyboardButton(text="🚫🌃 Пропуск ночного действия", callback_data=settings_callback_data(chat_id, "misc_item", "night_action_skip"))],
            [InlineKeyboardButton(text="🖼 Удаление медиа", callback_data=settings_callback_data(chat_id, "misc_item", "delete_media"))],
            [InlineKeyboardButton(text="👑 Админ запускает игру", callback_data=settings_callback_data(chat_id, "misc_item", "admin_game_only"))],
            [InlineKeyboardButton(text="🚀 Включение бафов", callback_data=settings_callback_data(chat_id, "misc_item", "buffs_enabled"))],
            [InlineKeyboardButton(text="💀 Показывать исполнителей", callback_data=settings_callback_data(chat_id, "misc_item", "show_killers"))],
            [InlineKeyboardButton(text="🚪 Ограничение выхода", callback_data=settings_callback_data(chat_id, "leave"))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "main"))],
        ]
    )


def private_settings_mafia_ratio_keyboard(chat_id: int, settings: dict) -> InlineKeyboardMarkup:
    current = settings.get("mafia_ratio", "high")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Больше (1/3) {selected_square(current == 'high')}", callback_data=settings_callback_data(chat_id, "mafia_ratio_set", "high"))],
            [InlineKeyboardButton(text=f"Меньше (1/4) {selected_square(current == 'low')}", callback_data=settings_callback_data(chat_id, "mafia_ratio_set", "low"))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "misc"))],
        ]
    )


def private_settings_voting_mode_keyboard(chat_id: int, settings: dict) -> InlineKeyboardMarkup:
    current = settings.get("voting_mode", "open")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Открытое {selected_square(current == 'open')}", callback_data=settings_callback_data(chat_id, "voting_mode_set", "open"))],
            [InlineKeyboardButton(text=f"Тайное {selected_square(current == 'secret')}", callback_data=settings_callback_data(chat_id, "voting_mode_set", "secret"))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "misc"))],
        ]
    )


def private_settings_misc_toggle_keyboard(chat_id: int, settings: dict, key: str) -> InlineKeyboardMarkup:
    current = bool(settings.get("misc", {}).get(key, False))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Да {selected_square(current)}", callback_data=settings_callback_data(chat_id, "misc_set", key, 1))],
            [InlineKeyboardButton(text=f"Нет {selected_square(not current)}", callback_data=settings_callback_data(chat_id, "misc_set", key, 0))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "misc"))],
        ]
    )


def private_settings_leave_keyboard(chat_id: int, settings: dict) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    current = int(settings.get("leave_restriction_seconds", LEAVE_RESTRICTION_SECONDS))
    for index in range(0, len(SETTINGS_LEAVE_OPTIONS), 2):
        row: list[InlineKeyboardButton] = []
        for value in SETTINGS_LEAVE_OPTIONS[index:index + 2]:
            label = format_leave_duration(value)
            row.append(
                InlineKeyboardButton(
                    text=f"{label} {selected_square(value == current)}",
                    callback_data=settings_callback_data(chat_id, "leave_set", value),
                )
            )
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "misc"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def cmd_settings(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.from_user is None:
        return
    if message.chat.type == "private":
        await message.answer("Настройки вызываются из игрового чата.")
        return

    if not await is_group_settings_admin(message.bot, message.chat.id, message.from_user.id):
        await message.answer(MSG_ADMIN_REQUIRED)
        return

    if not repo.has_private_user(message.from_user.id):
        start_link = await bot_start_link(message.bot)
        await message.answer(
            "Сначала напиши боту в личку /start, затем снова вызови /settings.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Открыть бота", url=start_link)],
                ]
            ),
        )
        return

    settings = load_chat_settings(message.chat.id)
    try:
        await message.bot.send_message(
            message.from_user.id,
            private_settings_main_text(settings),
            reply_markup=private_settings_main_keyboard(message.chat.id, settings),
        )
    except Exception:
        await message.answer("Не смог отправить настройки в ЛС. Напиши боту /start в личку и попробуй снова.")
        return

    await message.answer("Настройки отправлены в ЛС бота.")


async def on_private_settings_callback(callback: CallbackQuery) -> None:
    async def safe_answer(text: str | None = None, show_alert: bool = False) -> None:
        try:
            if text is None:
                await callback.answer()
            else:
                await callback.answer(text, show_alert=show_alert)
        except TelegramBadRequest as e:
            error_text = str(e)
            if "query is too old" in error_text or "query ID is invalid" in error_text:
                return
            raise

    if callback.from_user is None:
        return
    if callback.message is None or callback.message.chat.type != "private":
        await safe_answer("Это меню работает только в ЛС бота.", show_alert=True)
        return

    async def show_settings_screen(text: str, keyboard: InlineKeyboardMarkup | None) -> None:
        formatted_text = format_settings_screen_text(text)
        try:
            await callback.message.edit_text(formatted_text, reply_markup=keyboard)
        except Exception:
            await callback.message.answer(formatted_text, reply_markup=keyboard)

    parts = callback.data.split(":")
    if len(parts) < 3:
        await safe_answer("Неизвестный пункт настроек.", show_alert=True)
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        await safe_answer("Некорректный чат настроек.", show_alert=True)
        return

    if not await is_group_settings_admin(callback.bot, chat_id, callback.from_user.id):
        await safe_answer(
            MSG_ADMIN_REQUIRED,
            show_alert=True,
        )
        return

    action = parts[2]
    settings = load_chat_settings(chat_id)

    if action == "mode_locked":
        await safe_answer(settings_mode_locked_message(settings), show_alert=True)
        return

    if action == "main":
        await show_settings_screen(private_settings_main_text(settings), private_settings_main_keyboard(chat_id, settings))
        await safe_answer()
        return

    if action == "close":
        nickname = user_nickname(callback.from_user)
        await show_settings_screen(
            f"<b>С возвращением, {nickname}!</b>\n\nВыбери нужный раздел кнопками ниже.",
            private_main_menu_keyboard(callback.from_user.id),
        )
        await safe_answer()
        return

    if action == "game_mode":
        await show_settings_screen(SETTINGS_GAME_MODE_TEXT, private_settings_game_mode_keyboard(chat_id, settings))
        await safe_answer()
        return

    if action == "game_mode_set" and len(parts) == 4:
        value = parts[3]
        if value not in GAME_MODE_TITLES:
            await safe_answer("Неизвестный режим.", show_alert=True)
            return
        previous_mode = game_mode_from_settings(settings)
        settings = apply_game_mode_preset(settings, value)
        settings = save_chat_settings(chat_id, settings)
        if previous_mode != value:
            announcement_text = game_mode_announcement_text(value)
            if announcement_text:
                await callback.bot.send_message(chat_id, announcement_text, parse_mode="HTML")
        await show_settings_screen(SETTINGS_GAME_MODE_TEXT, private_settings_game_mode_keyboard(chat_id, settings))
        await safe_answer("Сохранено")
        return

    if settings_mode_locked(settings):
        await safe_answer(settings_mode_locked_message(settings), show_alert=True)
        return

    if action == "roles":
        await show_settings_screen(private_settings_roles_text(), private_settings_roles_keyboard(chat_id))
        await safe_answer()
        return

    if action == "role" and len(parts) == 4:
        try:
            role_index = int(parts[3])
        except ValueError:
            await safe_answer("Некорректная роль.", show_alert=True)
            return
        if role_index < 0 or role_index >= len(SETTINGS_ROLE_OPTIONS):
            await safe_answer("Роль не найдена.", show_alert=True)
            return
        role = SETTINGS_ROLE_OPTIONS[role_index]
        await show_settings_screen(
            private_settings_role_toggle_text(role),
            private_settings_role_toggle_keyboard(chat_id, settings, role_index),
        )
        await safe_answer()
        return

    if action == "role_set" and len(parts) == 5:
        try:
            role_index = int(parts[3])
        except ValueError:
            await safe_answer("Некорректная роль.", show_alert=True)
            return
        if role_index < 0 or role_index >= len(SETTINGS_ROLE_OPTIONS):
            await safe_answer("Роль не найдена.", show_alert=True)
            return
        role = SETTINGS_ROLE_OPTIONS[role_index]
        settings["roles"][role] = parts[4] == "1"
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(
            private_settings_role_toggle_text(role),
            private_settings_role_toggle_keyboard(chat_id, settings, role_index),
        )
        await safe_answer("Сохранено")
        return

    if action == "timings":
        await show_settings_screen("Выберите какие тайминги необходимо изменить:", private_settings_timings_keyboard(chat_id))
        await safe_answer()
        return

    if action == "timing" and len(parts) == 4:
        key = parts[3]
        title = SETTINGS_TIMING_TITLES.get(key)
        if title is None:
            await safe_answer("Неизвестный тайминг.", show_alert=True)
            return
        await show_settings_screen(title, private_settings_timing_values_keyboard(chat_id, settings, key))
        await safe_answer()
        return

    if action == "timing_set" and len(parts) == 5:
        key = parts[3]
        try:
            value = int(parts[4])
        except ValueError:
            await safe_answer("Некорректное значение.", show_alert=True)
            return
        if key not in settings["timings"]:
            await safe_answer("Неизвестный тайминг.", show_alert=True)
            return
        settings["timings"][key] = value
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(SETTINGS_TIMING_TITLES[key], private_settings_timing_values_keyboard(chat_id, settings, key))
        await safe_answer("Сохранено")
        return

    if action == "mute":
        await show_settings_screen("Отключение возможности писать сообщения в чате", private_settings_mute_keyboard(chat_id))
        await safe_answer()
        return

    if action == "mute_item" and len(parts) == 4:
        key = parts[3]
        title = SETTINGS_MUTE_TITLES.get(key)
        if title is None:
            await safe_answer("Неизвестный параметр.", show_alert=True)
            return
        await show_settings_screen(title, private_settings_mute_toggle_keyboard(chat_id, settings, key))
        await safe_answer()
        return

    if action == "mute_set" and len(parts) == 5:
        key = parts[3]
        value = parts[4] == "1"
        if key not in settings["mute"]:
            await safe_answer("Неизвестный параметр.", show_alert=True)
            return
        settings["mute"][key] = value
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(SETTINGS_MUTE_TITLES[key], private_settings_mute_toggle_keyboard(chat_id, settings, key))
        await safe_answer("Сохранено")
        return

    if action == "misc":
        await show_settings_screen("Что вы хотите изменить?", private_settings_misc_keyboard(chat_id))
        await safe_answer()
        return

    if action == "mafia_ratio":
        await show_settings_screen(SETTINGS_MAFIA_RATIO_TEXT, private_settings_mafia_ratio_keyboard(chat_id, settings))
        await safe_answer()
        return

    if action == "mafia_ratio_set" and len(parts) == 4:
        value = parts[3]
        if value not in SETTINGS_MAFIA_RATIO_TITLES:
            await safe_answer("Неизвестный параметр.", show_alert=True)
            return
        settings["mafia_ratio"] = value
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(SETTINGS_MAFIA_RATIO_TEXT, private_settings_mafia_ratio_keyboard(chat_id, settings))
        await safe_answer("Сохранено")
        return

    if action == "voting_mode":
        await show_settings_screen(SETTINGS_VOTING_MODE_TEXT, private_settings_voting_mode_keyboard(chat_id, settings))
        await safe_answer()
        return

    if action == "voting_mode_set" and len(parts) == 4:
        value = parts[3]
        if value not in SETTINGS_VOTING_MODE_TITLES:
            await safe_answer("Неизвестный параметр.", show_alert=True)
            return
        settings["voting_mode"] = value
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(SETTINGS_VOTING_MODE_TEXT, private_settings_voting_mode_keyboard(chat_id, settings))
        await safe_answer("Сохранено")
        return

    if action == "misc_item" and len(parts) == 4:
        key = parts[3]
        title = SETTINGS_MISC_TITLES.get(key)
        if title is None:
            await safe_answer("Неизвестный параметр.", show_alert=True)
            return
        await show_settings_screen(title, private_settings_misc_toggle_keyboard(chat_id, settings, key))
        await safe_answer()
        return

    if action == "misc_set" and len(parts) == 5:
        key = parts[3]
        if key not in settings.get("misc", {}):
            await safe_answer("Неизвестный параметр.", show_alert=True)
            return
        settings["misc"][key] = parts[4] == "1"
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(SETTINGS_MISC_TITLES[key], private_settings_misc_toggle_keyboard(chat_id, settings, key))
        await safe_answer("Сохранено")
        return

    if action == "leave":
        await show_settings_screen(
            "Выберите длительность, в течение которой пользователь не сможет присоединяться к игре, если он досрочно покинул предыдущую игру.",
            private_settings_leave_keyboard(chat_id, settings),
        )
        await safe_answer()
        return

    if action == "leave_set" and len(parts) == 4:
        try:
            settings["leave_restriction_seconds"] = int(parts[3])
        except ValueError:
            await safe_answer("Некорректное значение.", show_alert=True)
            return
        settings = save_chat_settings(chat_id, settings)
        await show_settings_screen(
            "Выберите длительность, в течение которой пользователь не сможет присоединяться к игре, если он досрочно покинул предыдущую игру.",
            private_settings_leave_keyboard(chat_id, settings),
        )
        await safe_answer("Сохранено")
        return

    await safe_answer("Неизвестный пункт настроек.", show_alert=True)

