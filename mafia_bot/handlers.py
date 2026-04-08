import asyncio
import os
import time
import traceback
from datetime import datetime, timedelta
from html import escape

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import CallbackQuery, ChatMemberUpdated, ChatPermissions, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message, User
from aiogram import Bot

from mafia_bot.storage import GameStateRepository
from mafia_bot.game import (
    DAY_STAGE_DISCUSSION,
    DAY_STAGE_NOMINATION,
    DAY_STAGE_TRIAL,
    MAFIA_ROLES,
    MIN_PLAYERS,
    PHASE_DAY,
    PHASE_FINISHED,
    PHASE_NIGHT,
    ROLE_ACTION_RULES,
    ROLE_BUM,
    ROLE_ADVOCATE,
    ROLE_CITIZEN,
    ROLE_COMMISSAR,
    ROLE_DESCRIPTION,
    ROLE_DON,
    ROLE_EMOJI,
    ROLE_DOCTOR,
    ROLE_KAMIKAZE,
    ROLE_LUCKY,
    ROLE_MAFIA,
    ROLE_MANIAC,
    ROLE_MISTRESS,
    ROLE_SERGEANT,
    ROLE_SUICIDE,
    GameStorage,
    all_roles_info_text,
    role_card_text,
)

router = Router()
repo = GameStateRepository()
storage = GameStorage()
storage.rooms = repo.load_rooms()


def read_phase_seconds(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


NIGHT_PHASE_SECONDS = read_phase_seconds("NIGHT_PHASE_SECONDS", 60)
DAY_DISCUSSION_SECONDS = read_phase_seconds("DAY_DISCUSSION_SECONDS", 60)
DAY_NOMINATION_SECONDS = read_phase_seconds("DAY_NOMINATION_SECONDS", 45)
DAY_TRIAL_SECONDS = read_phase_seconds("DAY_TRIAL_SECONDS", 60)
REGISTRATION_SECONDS = read_phase_seconds("REGISTRATION_SECONDS", 120)
REGISTRATION_EXTENSION_SECONDS = read_phase_seconds("REGISTRATION_EXTENSION_SECONDS", 30)
RESTART_EXPIRED_PHASE_POLICY = os.getenv("RESTART_EXPIRED_PHASE_POLICY", "catch_up").strip().lower()
DAY_IMAGE_PATH = os.getenv("DAY_IMAGE_PATH", os.path.join("assets", "day.jpg"))
NIGHT_IMAGE_PATH = os.getenv("NIGHT_IMAGE_PATH", os.path.join("assets", "night.jpg"))

phase_timers: dict[int, asyncio.Task] = {}
registration_timers: dict[int, asyncio.Task] = {}
phase_locks: dict[int, asyncio.Lock] = {}
chat_penalties: dict[int, dict[int, dict[str, float | int | bool]]] = {}
action_menu_messages: dict[int, dict[int, int]] = {}
delete_permission_alerted_chats: set[int] = set()
registration_panel_message_ids: dict[int, int] = {}
registration_notice_message_ids: dict[int, int] = {}
registration_warning_message_ids: dict[int, int] = {}
recent_chat_welcomes: dict[tuple[int, int], float] = {}
OWNER_USER_ID = 5658493362
MISTRESS_DAY_BLOCK_TOAST = "Ты под действием Любовницы."
MUTE_DEAD_PLAYERS = True
MUTE_SLEEPING_PLAYERS = True
MUTE_NON_PLAYERS = True
LEAVE_RESTRICTION_SECONDS = 0

SETTINGS_ROLE_OPTIONS = [
    ROLE_COMMISSAR,
    ROLE_DOCTOR,
    ROLE_SUICIDE,
    ROLE_MISTRESS,
    ROLE_MANIAC,
    ROLE_BUM,
    ROLE_SERGEANT,
    ROLE_ADVOCATE,
    ROLE_LUCKY,
    ROLE_KAMIKAZE,
]
SETTINGS_TIMING_OPTIONS = [30, 45, 60, 75, 90, 120, 180, 240, 300, 360]
SETTINGS_LEAVE_OPTIONS = [0, 1800, 3600, 7200, 10800, 21600, 43200, 86400]
SETTINGS_TIMING_LABELS = {
    "registration": "Регистрация",
    "night": "Ночь",
    "day": "День",
    "vote": "Голосование",
    "trial": "Подтверждение",
}
SETTINGS_TIMING_TITLES = {
    "registration": "Выберите длительность регистрации (сек.)",
    "night": "Выберите длительность ночи (сек.)",
    "day": "Выберите длительность дня (сек.)",
    "vote": "Выберите длительность голосования (сек.)",
    "trial": "Выберите длительность подтверждения голосования (сек.)",
}
SETTINGS_MUTE_LABELS = {
    "dead": "Для убитых",
    "sleeping": "Для спящих",
    "outsiders": "Для неиграющих",
}
SETTINGS_MUTE_TITLES = {
    "dead": "Требуется ли запрещать убитым писать сообщения в чат?",
    "sleeping": "Требуется ли запрещать писать сообщения в чат ночью?",
    "outsiders": "Требуется ли запрещать писать сообщения в чат тем, кто не в игре?",
}
SETTINGS_MISC_TITLES = {
    "admin_game_only": "Разрешить запускать новую игру только администраторам?",
    "action_notifications": "Требуется ли писать пользователям о том, что ночью к ним кто-то пришёл?",
    "allow_team_kill": "Разрешить убийство союзников?\nЕсли выбрать \"нет\", то при выборе жертвы имена союзников будут отсутствовать",
    "buffs_enabled": "Требуется ли включить дополнительные возможности для игроков?",
    "commissar_can_shoot": "Разрешить роли Комиссар Каттани стрелять?\nЕсли выбрать \"нет\", то он будет заниматься только проверкой ролей",
    "commissar_first_night_shot": "Разрешить комиссару стрелять в первую ночь?\nЕсли выбрать \"нет\", то комиссар не сможет стрелять в первую ночь",
    "content_protection": "Требуется ли включить защиту контента?\nЕсли выбрать \"да\", бот запретит копировать и пересылать игровые личные сообщения, а также делать их скриншоты",
    "day_vote_skip": "Разрешить игрокам пропускать ход на дневном голосовании?",
    "kamikaze_night_revenge": "Разрешить Камикадзе отомстить своему убийце ночью?\nЕсли выбрать \"Да\", то Камикадзе при смерти будет уничтожать своего убийцу",
    "delete_media": "Требуется ли удалять публикуемые пользователями фото, видео и аудио?",
    "night_action_skip": "Разрешить игрокам пропускать ход при ночном действии?",
    "show_targets": "Стоит ли показывать выбранные цели?\nЕсли выбрать \"да\", то ночью станут видны цели. Например: Доктор решил зайти к G.Hughes",
    "show_roles": "Стоит ли показывать роли?\nЕсли выбрать \"да\", то бот будет объявлять роли погибших. В обратном случае роли останутся в тайне до завершения игры.",
    "show_killers": "Требуется ли отображать в чате роль того, кто совершил убийство?",
}
SETTINGS_MAFIA_RATIO_TITLES = {
    "high": "Больше (1/3)",
    "low": "Меньше (1/4)",
}
SETTINGS_MAFIA_RATIO_TEXT = (
    "Выберите коэффициент количества мафии.\n"
    "При варианте \"Больше\" каждый 3-й игрок будет мафией, а при варианте \"Меньше\" - каждый 4-й."
)
SETTINGS_VOTING_MODE_TITLES = {
    "open": "Открытое",
    "secret": "Тайное",
}
SETTINGS_VOTING_MODE_TEXT = (
    "Стоит ли делать голосование закрытым и скрывать имена обвиняемых?\n"
    "При варианте \"Открытое\" будет видно кто за кого голосовал, а при варианте \"Тайное\" - не будет видно."
)


def get_phase_lock(chat_id: int) -> asyncio.Lock:
    lock = phase_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        phase_locks[chat_id] = lock
    return lock


def cancel_phase_timer(chat_id: int) -> None:
    timer = phase_timers.get(chat_id)
    if timer is None:
        return

    current = asyncio.current_task()
    if timer is current:
        # The timer callback reached phase end itself; avoid self-cancel that aborts transition.
        phase_timers.pop(chat_id, None)
        return

    phase_timers.pop(chat_id, None)
    timer.cancel()


def cancel_registration_timer(chat_id: int) -> None:
    timer = registration_timers.get(chat_id)
    if timer is None:
        return

    current = asyncio.current_task()
    if timer is current:
        # Timer callback reached registration timeout itself; avoid self-cancel.
        registration_timers.pop(chat_id, None)
        return

    registration_timers.pop(chat_id, None)
    timer.cancel()


def current_day_stage_seconds(room) -> int:
    settings = room_chat_settings(room)
    if room.day_stage == DAY_STAGE_DISCUSSION:
        return int(settings["timings"]["day"])
    if room.day_stage == DAY_STAGE_NOMINATION:
        return int(settings["timings"]["vote"])
    if room.day_stage == DAY_STAGE_TRIAL:
        return int(settings["timings"]["trial"])
    return int(settings["timings"]["vote"])


def clear_chat_penalties(chat_id: int) -> None:
    chat_penalties.pop(chat_id, None)


def persist_room(room) -> None:
    repo.save_room(room)


def remove_room_state(chat_id: int) -> None:
    repo.delete_room(chat_id)


def role_mark_text(role: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    return f"{emoji} <b>{role}</b>".strip()


def is_secret_voting_enabled(room) -> bool:
    return room_chat_settings(room).get("voting_mode", "open") == "secret"


def trial_vote_prompt_text(room, candidate) -> str:
    if candidate is None:
        return "Вы точно хотите линчевать обвиняемого?"
    if is_secret_voting_enabled(room):
        return "Вы точно хотите линчевать обвиняемого?"
    return f"Вы точно хотите линчевать {player_profile_link(candidate)}?"


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


def private_game_send_kwargs(room) -> dict:
    if not content_protection_enabled(room):
        return {}
    return {"protect_content": True}


def night_role_announcement_text(room, role_name: str, target=None, *, variant: str = "default") -> str:
    if show_targets_enabled(room) and target is not None:
        target_mark = player_profile_link(target)
        targeted_announcements = {
            ROLE_COMMISSAR: {
                "default": f"<b>🕵️ Комиссар Каттани</b> решил проверить {target_mark}.",
                "shoot": f"<b>🕵️ Комиссар Каттани</b> решил застрелить {target_mark}.",
            },
            ROLE_BUM: {
                "default": f"<b>🧙🏼‍♂️ Бомж</b> пошёл за бутылкой к {target_mark}.",
            },
            ROLE_MANIAC: {
                "default": f"<b>🔪 Маньяк</b> выбрал целью {target_mark}.",
            },
            ROLE_ADVOCATE: {
                "default": f"<b>👨🏼‍💼 Адвокат</b> решил защищать {target_mark}.",
            },
            ROLE_MISTRESS: {
                "default": f"<b>💃🏼 Любовница</b> решила зайти к {target_mark}.",
            },
            ROLE_DOCTOR: {
                "default": f"<b>👨🏼‍⚕️ Доктор</b> решил зайти к {target_mark}.",
            },
            ROLE_KAMIKAZE: {
                "default": f"<b>💣 Камикадзе</b> решил забрать с собой {target_mark}.",
            },
            ROLE_DON: {
                "default": f"<b>🤵🏻 Мафия</b> выбрала жертву {target_mark}.",
            },
            ROLE_MAFIA: {
                "default": f"<b>🤵🏻 Мафия</b> выбрала жертву {target_mark}.",
            },
        }
        role_variants = targeted_announcements.get(role_name)
        if role_variants is not None:
            return role_variants.get(variant, role_variants.get("default", f"{role_mark_text(role_name)} выбрал цель {target_mark}."))

    role_announcement = {
        ROLE_COMMISSAR: "<b>🕵️ Комиссар Каттани</b> ушёл искать злодеев...",
        ROLE_BUM: "<b>🧙🏼‍♂️ Бомж</b> пошёл к кому-то за бутылкой...",
        ROLE_MANIAC: "<b>🔪 Маньяк</b> спрятался глубоко в кустах...",
        ROLE_ADVOCATE: "<b>👨🏼‍💼 Адвокат</b> ищет мафию для защиты...",
        ROLE_MISTRESS: "<b>💃🏼 Любовница</b> уже ждёт кого-то в гости...",
        ROLE_DOCTOR: "<b>👨🏼‍⚕️ Доктор</b> вышел на ночное дежурство...",
        ROLE_KAMIKAZE: "<b>💣 Камикадзе</b> решил забрать кого-то с собой...",
        ROLE_DON: "<b>🤵🏻 Мафия</b> выбрала жертву...",
        ROLE_MAFIA: "<b>🤵🏻 Мафия</b> выбрала жертву...",
    }
    announcement_text = role_announcement.get(role_name)
    if announcement_text is None:
        role_mark = role_mark_text(role_name)
        return f"{role_mark} сделал ночной ход."
    return announcement_text


def default_chat_settings() -> dict:
    return {
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


def track_action_menu_message(chat_id: int, user_id: int, message_id: int) -> None:
    by_chat = action_menu_messages.setdefault(chat_id, {})
    by_chat[user_id] = message_id


def get_action_menu_message_id(chat_id: int, user_id: int) -> int | None:
    by_chat = action_menu_messages.get(chat_id)
    if by_chat is None:
        return None
    return by_chat.get(user_id)


def clear_action_menu_messages(chat_id: int) -> None:
    action_menu_messages.pop(chat_id, None)


async def clear_registration_panel_message(bot: Bot, chat_id: int) -> None:
    message_id = registration_panel_message_ids.pop(chat_id, None)
    if message_id is None:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def clear_registration_notice_message(bot: Bot, chat_id: int) -> None:
    message_id = registration_notice_message_ids.pop(chat_id, None)
    if message_id is None:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def upsert_registration_warning_message(
    bot: Bot,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    message_id = registration_warning_message_ids.get(chat_id)
    if message_id is not None:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
            )
            return
        except Exception:
            registration_warning_message_ids.pop(chat_id, None)

    try:
        sent = await bot.send_message(chat_id, text, reply_markup=reply_markup)
    except Exception:
        return
    registration_warning_message_ids[chat_id] = sent.message_id


async def clear_registration_warning_message(bot: Bot, chat_id: int) -> None:
    message_id = registration_warning_message_ids.pop(chat_id, None)
    if message_id is None:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def resolve_phase_image_path(image_path: str | None) -> str | None:
    if not image_path:
        return None
    if os.path.exists(image_path):
        return image_path

    base_dir = os.path.dirname(image_path) or "."
    stem = os.path.splitext(os.path.basename(image_path))[0].lower()

    aliases_by_stem = {
        "day": ["day", "den", "d"],
        "night": ["night", "nori", "noch", "n"],
    }
    aliases = aliases_by_stem.get(stem, [stem])
    extensions = [".jpg", ".jpeg", ".png", ".webp"]

    for alias in aliases:
        for ext in extensions:
            candidate = os.path.join(base_dir, f"{alias}{ext}")
            if os.path.exists(candidate):
                return candidate

    return image_path


async def send_phase_media(
    bot: Bot,
    chat_id: int,
    caption: str,
    image_path: str | None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    image_path = resolve_phase_image_path(image_path)
    if image_path and os.path.exists(image_path):
        try:
            await bot.send_photo(
                chat_id,
                photo=FSInputFile(image_path),
                caption=caption,
                reply_markup=reply_markup,
            )
            return
        except Exception:
            pass

    await safe_send_message(bot, chat_id, caption, reply_markup=reply_markup)


async def safe_send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    max_retries: int = 3,
    **kwargs,
):
    attempt = 0
    while True:
        try:
            return await bot.send_message(chat_id, text, **kwargs)
        except TelegramRetryAfter as e:
            attempt += 1
            retry_after = max(int(getattr(e, "retry_after", 1)), 1)
            print(
                f"[RATE_LIMIT] send_message retry: chat_id={chat_id}, "
                f"attempt={attempt}/{max_retries}, retry_after={retry_after}s"
            )
            if attempt > max_retries:
                print(f"[RATE_LIMIT] send_message dropped after retries: chat_id={chat_id}")
                return None
            await asyncio.sleep(retry_after)


def skipped_turn_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Пропустить ход",
                    callback_data=f"noop:skip:{chat_id}",
                )
            ]
        ]
    )


def locked_choice_keyboard(selected_name: str) -> InlineKeyboardMarkup:
    label = f"Ты выбрал {selected_name}".strip()
    if len(label) > 64:
        label = label[:61] + "..."
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=label, callback_data="noop:locked")]]
    )


def locked_choice_text(room, actor_user_id: int, selected_name: str, selected_user_id: int | None = None) -> str:
    prompt = build_action_prompt_text(room, actor_user_id)
    safe_name = escape(selected_name)
    selected_mark = safe_name
    if selected_user_id is not None:
        selected_mark = f"<a href=\"tg://user?id={selected_user_id}\">{safe_name}</a>"
    return f"{prompt}\n\nТы выбрал {selected_mark}"


def night_skipped_user_ids(room) -> list[int]:
    skipped: list[int] = []

    for player in room.alive_players():
        if player.role in {ROLE_DON, ROLE_MAFIA} and player.user_id not in room.night_votes:
            skipped.append(player.user_id)
        elif player.role == "Доктор" and room.doctor_target_id is None:
            skipped.append(player.user_id)
        elif player.role == "Комиссар Каттани":
            if commissar_can_shoot_this_night(room):
                if not commissar_can_shoot_enabled(room):
                    if room.commissar_target_id is None:
                        skipped.append(player.user_id)
                elif room.commissar_action_mode is None:
                    skipped.append(player.user_id)
                elif room.commissar_action_mode == "check" and room.commissar_target_id is None:
                    skipped.append(player.user_id)
                elif room.commissar_action_mode == "shoot" and room.commissar_shot_target_id is None:
                    skipped.append(player.user_id)
            elif room.commissar_target_id is None:
                skipped.append(player.user_id)
        elif player.role == ROLE_ADVOCATE and room.advocate_target_id is None:
            skipped.append(player.user_id)
        elif player.role == "Маньяк" and room.maniac_target_id is None:
            skipped.append(player.user_id)
        elif player.role == "Любовница" and room.mistress_target_id is None:
            skipped.append(player.user_id)
        elif player.role == "Бомж" and room.bum_target_id is None:
            skipped.append(player.user_id)

    if room.kamikaze_pending_user_id is not None and room.kamikaze_target_id is None:
        skipped.append(room.kamikaze_pending_user_id)

    return skipped


def skipped_night_chat_text(player) -> str | None:
    if player.role == ROLE_BUM:
        return "🚷 🧙🏼‍♂️ Бомж сегодня отдыхает"
    if player.role == ROLE_DOCTOR:
        return "🚷 👨🏼‍⚕️ Доктор предпочитает не играть"
    if player.role == ROLE_MANIAC:
        return "🚷 🔪 Маньяк предпочитает не играть"
    if player.role == ROLE_MISTRESS:
        return "🚷 💃🏼 Любовница предпочитает не играть"
    if player.role == ROLE_DON:
        return "🚷 🤵🏻 Дон сегодня отдыхает"
    if player.role == ROLE_COMMISSAR:
        return "🚷 🕵️‍ Комиссар Каттани не хочет участвовать в наших баталиях"
    return None


async def mark_skipped_night_menus(bot: Bot, room, skipped_user_ids: list[int]) -> None:
    if not skipped_user_ids:
        return

    keyboard = skipped_turn_keyboard(room.chat_id)
    chat_messages: list[str] = []
    for user_id in skipped_user_ids:
        player = room.get_player(user_id)
        if player is not None:
            chat_text = skipped_night_chat_text(player)
            if chat_text is not None:
                chat_messages.append(chat_text)
        message_id = get_action_menu_message_id(room.chat_id, user_id)
        if message_id is None:
            continue
        skipped_text = f"{build_action_prompt_text(room, user_id)}\n\nВы пропустили ход."
        try:
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=message_id,
                text=skipped_text,
                reply_markup=keyboard,
            )
        except Exception:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=user_id,
                    message_id=message_id,
                    reply_markup=keyboard,
                )
            except Exception:
                continue

    for chat_text in dict.fromkeys(chat_messages):
        await safe_send_message(bot, room.chat_id, chat_text)


def format_killer_sources_text(sources: list[str]) -> str:
    if not sources:
        return ""

    labels: list[str] = []
    for source in sources:
        if source == "мафия":
            labels.append(role_mark_text(ROLE_DON))
        elif source == "маньяк":
            labels.append(role_mark_text(ROLE_MANIAC))
        elif source == "комиссар":
            labels.append(role_mark_text(ROLE_COMMISSAR))
        elif source == "камикадзе":
            labels.append(role_mark_text(ROLE_KAMIKAZE))
        else:
            labels.append(source)

    # Keep source order but remove duplicates.
    unique_labels = list(dict.fromkeys(labels))
    if len(unique_labels) == 1:
        return f"Говорят, у него в гостях был {unique_labels[0]}."

    return "Говорят, у него в гостях были " + " и ".join(unique_labels) + "."


def registration_remaining_seconds(room) -> int:
    if room.phase_started_at is None or room.phase_duration_seconds is None:
        return int(room_chat_settings(room)["timings"]["registration"])
    elapsed = int((datetime.now() - room.phase_started_at).total_seconds())
    remaining = int(room.phase_duration_seconds) - elapsed
    return remaining if remaining > 0 else 0


async def start_registration_timer(room, bot: Bot, seconds: int) -> None:
    cancel_registration_timer(room.chat_id)
    if seconds <= 0:
        seconds = 1
    room.phase_started_at = datetime.now()
    room.phase_duration_seconds = seconds
    persist_room(room)
    print(f"[DEBUG] registration_timer_started: chat_id={room.chat_id}, seconds={seconds}")

    async def worker() -> None:
        try:
            warning_mark = 30
            if seconds > warning_mark:
                await asyncio.sleep(seconds - warning_mark)
                current_room = storage.get_room(room.chat_id)
                if current_room is not None and not current_room.started and current_room.registration_open:
                    me = await bot.get_me()
                    join_link = f"https://t.me/{me.username}?start=join_{room.chat_id}"
                    await upsert_registration_warning_message(
                        bot,
                        room.chat_id,
                        f"До окончания регистрации осталось {warning_mark} сек.",
                        reply_markup=registration_lobby_keyboard(join_link),
                    )
                await asyncio.sleep(warning_mark)
            else:
                await asyncio.sleep(seconds)
            print(f"[DEBUG] registration_timer_fired: chat_id={room.chat_id}")
            await process_registration_timeout(bot, room.chat_id)
        except asyncio.CancelledError:
            print(f"[DEBUG] registration_timer_cancelled: chat_id={room.chat_id}")
            return
        except Exception as e:
            print(f"[ERROR] registration_timer_worker: chat_id={room.chat_id}, error={e!r}")

    registration_timers[room.chat_id] = asyncio.create_task(worker())


async def launch_game_from_registration(bot: Bot, room, chat_id: int, chat_title: str | None) -> None:
    cancel_registration_timer(chat_id)
    room.close_registration()
    try:
        room.assign_roles()
        prime_room_documents(room)
        prime_room_shields(room)
        print(
            f"[DEBUG] launch_game_from_registration: chat_id={chat_id}, "
            f"started={room.started}, phase={room.phase}, round_no={room.round_no}, players={len(room.players)}"
        )
    except Exception as e:
        try:
            await bot.send_message(chat_id, f"Не удалось начать игру. Ошибка: {e!r}\nПопробуй /stop и создай лобби заново.")
        except Exception as e2:
            print(f"[ERROR] Не удалось отправить сообщение об ошибке: {e2!r}")
        print(f"[ERROR] assign_roles: {e!r}")
        return

    try:
        await clear_registration_post(bot, room)
    except Exception as e:
        print(f"[ERROR] clear_registration_post: {e!r}")
    try:
        await clear_registration_panel_message(bot, chat_id)
    except Exception as e:
        print(f"[ERROR] clear_registration_panel_message: {e!r}")
    try:
        await clear_registration_notice_message(bot, chat_id)
    except Exception as e:
        print(f"[ERROR] clear_registration_notice_message: {e!r}")
    try:
        await clear_registration_warning_message(bot, chat_id)
    except Exception as e:
        print(f"[ERROR] clear_registration_warning_message: {e!r}")
    try:
        clear_action_menu_messages(chat_id)
    except Exception as e:
        print(f"[ERROR] clear_action_menu_messages: {e!r}")
    try:
        persist_room(room)
    except Exception as e:
        print(f"[ERROR] persist_room: {e!r}")

    try:
        await bot.send_message(
            chat_id,
            "<b>Игра начинается!</b>\n\n"
            "<i>В течение нескольких секунд бот пришлёт вам личное сообщение с ролью и её описанием.</i>",
        )
    except Exception as e:
        print(f"[ERROR] send_message(Игра начинается): {e!r}")

    async def send_role_cards() -> None:
        async def send_one_role_card(player) -> tuple[str, bool]:
            name = player_display_name(player)
            try:
                card_text = role_card_for_player(room, player, chat_title or room.chat_title or "Групповой чат")
                await asyncio.wait_for(
                    bot.send_message(player.user_id, card_text, **private_game_send_kwargs(room)),
                    timeout=8,
                )
                return name, True
            except Exception as e:
                print(f"[ERROR] send_one_role_card({name}): {e!r}")
                return name, False

        try:
            results = await asyncio.gather(
                *(send_one_role_card(player) for player in room.players.values()),
                return_exceptions=False,
            )
        except Exception as e:
            print(f"[ERROR] send_role_cards gather: {e!r}")
            results = []
        failed_names = [name for name, ok in results if not ok]

        if failed_names:
            try:
                await bot.send_message(
                    chat_id,
                    "Не смог отправить роли игрокам: "
                    + ", ".join(failed_names)
                    + ". Пусть напишут боту /start в личке.",
                )
            except Exception as e:
                print(f"[ERROR] send_message(Не смог отправить роли): {e!r}")

    try:
        await send_role_cards()
    except Exception as e:
        print(f"[ERROR] send_role_cards: {e!r}")

    # Wait 2 seconds before announcing night; roles are already sent at this point.
    await asyncio.sleep(2)

    keyboard: InlineKeyboardMarkup | None = None
    try:
        keyboard = await night_action_keyboard(bot)
    except Exception as e:
        print(f"[ERROR] night_action_keyboard: {e!r}")
        keyboard = None

    try:
        await send_phase_media(bot, chat_id, room.night_media_caption(), NIGHT_IMAGE_PATH, reply_markup=keyboard)
    except Exception as e:
        print(f"[ERROR] send_phase_media: {e!r}")
        try:
            await bot.send_message(chat_id, room.night_media_caption())
        except Exception as e2:
            print(f"[ERROR] send_message(night_media_caption): {e2!r}")

    try:
        await bot.send_message(chat_id, night_status_text(room), reply_markup=keyboard)
    except Exception as e:
        print(f"[ERROR] send_message(Живых игроков): {e!r}")

    try:
        await start_phase_timer(room, bot)
    except Exception as e:
        print(f"[ERROR] start_phase_timer: {e!r}")

    try:
        await push_phase_action_menus(bot, room)
    except Exception as e:
        print(f"[ERROR] push_phase_action_menus: {e!r}")


async def process_registration_timeout(bot: Bot, chat_id: int) -> None:
    print(f"[DEBUG] process_registration_timeout_enter: chat_id={chat_id}")
    try:
        room = storage.get_room(chat_id)
    except Exception as e:
        print(f"[ERROR] storage.get_room: {e!r}")
        return
    if room is None:
        print(f"[ERROR] process_registration_timeout: room is None for chat_id={chat_id}")
        return
    if room.started:
        print(f"[ERROR] process_registration_timeout: already started for chat_id={chat_id}")
        return

    if len(room.players) < MIN_PLAYERS:
        try:
            await clear_registration_post(bot, room)
        except Exception as e:
            print(f"[ERROR] clear_registration_post (min players): {e!r}")
        try:
            await clear_registration_panel_message(bot, chat_id)
        except Exception as e:
            print(f"[ERROR] clear_registration_panel_message (min players): {e!r}")
        try:
            await clear_registration_notice_message(bot, chat_id)
        except Exception as e:
            print(f"[ERROR] clear_registration_notice_message (min players): {e!r}")
        try:
            await clear_registration_warning_message(bot, chat_id)
        except Exception as e:
            print(f"[ERROR] clear_registration_warning_message (min players): {e!r}")
        try:
            cancel_registration_timer(chat_id)
        except Exception as e:
            print(f"[ERROR] cancel_registration_timer (min players): {e!r}")
        try:
            clear_chat_penalties(chat_id)
        except Exception as e:
            print(f"[ERROR] clear_chat_penalties (min players): {e!r}")
        try:
            clear_action_menu_messages(chat_id)
        except Exception as e:
            print(f"[ERROR] clear_action_menu_messages (min players): {e!r}")
        try:
            remove_room_state(chat_id)
        except Exception as e:
            print(f"[ERROR] remove_room_state (min players): {e!r}")
        try:
            storage.close_room(chat_id)
        except Exception as e:
            print(f"[ERROR] storage.close_room (min players): {e!r}")
        try:
            await bot.send_message(
                chat_id,
                f"Регистрация отменена: недостаточно игроков. Нужно минимум {MIN_PLAYERS}.",
            )
        except Exception as e:
            print(f"[ERROR] send_message(Регистрация отменена): {e!r}")
        return

    try:
        await launch_game_from_registration(bot, room, chat_id, room.chat_title)
    except Exception as e:
        print(f"[ERROR] launch_game_from_registration: {e!r}")
        return

    refreshed = storage.get_room(chat_id)
    print(
        f"[DEBUG] process_registration_timeout post-launch: chat_id={chat_id}, "
        f"room_exists={refreshed is not None}, "
        f"started={getattr(refreshed, 'started', None)}, "
        f"phase={getattr(refreshed, 'phase', None)}, "
        f"registration_open={getattr(refreshed, 'registration_open', None)}, "
        f"players={len(refreshed.players) if refreshed is not None else None}"
    )
    if refreshed is None or not refreshed.started or refreshed.phase != PHASE_NIGHT:
        try:
            await bot.send_message(
                chat_id,
                "Не удалось автоматически запустить игру. Нажми /start для ручного старта.",
            )
        except Exception as e:
            print(f"[ERROR] send_message(auto-start failed): {e!r}")


def ensure_stats_recorded(room) -> None:
    if room.phase != PHASE_FINISHED:
        return
    if room.stats_recorded:
        return
    repo.record_finished_game_stats(room)
    room.stats_recorded = True
    persist_room(room)


def format_player_stats_text(stats: dict) -> str:
    games = int(stats.get("games_played", 0))
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    mafia_games = int(stats.get("mafia_games", 0))
    maniac_games = int(stats.get("maniac_games", 0))
    civilian_games = int(stats.get("civilian_games", 0))
    money = int(stats.get("money", 0))
    tickets = int(stats.get("tickets", 0))
    last_role = str(stats.get("last_role", "") or "-")
    name = str(stats.get("display_name", "Игрок"))
    total_special_games = mafia_games + maniac_games + civilian_games

    return (
        "<b>Твоя статистика</b>\n\n"
        f"👤 Игрок: <b>{escape(name)}</b>\n\n"
        f"🎮 Всего партий: <b>{games}</b>\n"
        f"🏆 Побед: <b>{wins}</b>\n"
        f"💀 Поражений: <b>{losses}</b>\n\n"
        f"🕴 За мафию: <b>{mafia_games}</b>\n"
        f"🔪 За маньяка: <b>{maniac_games}</b>\n"
        f"🙂 За мирных: <b>{civilian_games}</b>\n\n"
        f"📚 Учтено партий по ролям: <b>{total_special_games}</b>"
    )


def format_endgame_currency_text(player, stats: dict, won: bool) -> str:
    name = escape((player.full_name or "").strip() or f"Игрок {player.user_id}")
    money = int(stats.get("money", 0))
    tickets = int(stats.get("tickets", 0))
    if won:
        return (
            "<b>Игра завершена</b>\n"
            f'За победу в роли "{escape(player.role)}" тебе начислили 💵 10!\n\n'
            f"👤 {name}\n\n"
            f"💵 Деньги: {money}\n"
            f"🎟Билетики: {tickets}"
        )
    return (
        "<b>Игра завершена</b>\n\n"
        f"👤 {name}\n\n"
        f"💵 Деньги: {money}\n"
        f"🎟Билетики: {tickets}"
    )


def format_private_profile_text(display_name: str, stats: dict | None) -> str:
    safe_name = escape((display_name or "").strip() or "Игрок")
    money = int((stats or {}).get("money", 0))
    tickets = int((stats or {}).get("tickets", 0))
    buff_documents = int((stats or {}).get("buff_documents", 0))
    buff_shield = int((stats or {}).get("buff_shield", 0))
    buff_active_role = int((stats or {}).get("buff_active_role", 0))
    last_role = str((stats or {}).get("last_role", "") or "-")
    last_role_mark = role_mark_text(last_role) if last_role != "-" else "-"

    return (
        "<b>Игровой профиль</b>\n\n"
        f"👤 <b>{safe_name}</b>\n\n"
        f"💵 Деньги: <b>{money}</b>\n"
        f"🎟 Билетики: <b>{tickets}</b>\n\n"
        "<b>🚀 Бафы</b>\n"
        f"📁 Документы: <b>{buff_documents}</b>\n"
        f"🛡 Защита: <b>{buff_shield}</b>\n"
        f"🕺 Активная роль: <b>{buff_active_role}</b>\n\n"
        f"🎭 Последняя роль: {last_role_mark}"
    )


BUFF_CATALOG = {
    "documents": {
        "title": "📁 Документы",
        "price": "💵150",
        "price_value": 150,
        "currency": "money",
        "inventory_key": "buff_documents",
        "success_name": "Документы",
        "description": "Фальшивые документы могут пригодиться когда твою роль кто-то захочет проверить",
        "details": "Если документы были в инвентаре до старта партии, первая проверка комиссара для мафии, адвоката или маньяка покажет мирного жителя.",
    },
    "shield": {
        "title": "🛡 Защита",
        "price": "💵100",
        "price_value": 100,
        "currency": "money",
        "inventory_key": "buff_shield",
        "success_name": "Защита",
        "description": "Один раз может спасти тебе жизнь",
        "details": "Если защита уже была в инвентаре до старта партии, она сработает один раз за игру при первой попытке ночного убийства.",
    },
    "active_role": {
        "title": "🕺 Активная роль",
        "price": "💎1",
        "price_value": 1,
        "currency": "tickets",
        "inventory_key": "buff_active_role",
        "success_name": "Активная роль",
        "description": "Даёт 99% шанс выпадения активной роли",
        "details": "Каркас механики: предмет будет влиять на выдачу роли перед стартом новой партии.",
    },
}


def format_buffs_shop_text() -> str:
    return (
        "<b>Что будем покупать?</b>\n\n"
        "📁 <b>Документы</b>\n"
        f"{BUFF_CATALOG['documents']['description']}\n\n"
        "🛡 <b>Защита</b>\n"
        f"{BUFF_CATALOG['shield']['description']}\n\n"
        "🕺 <b>Активная роль</b>\n"
        f"{BUFF_CATALOG['active_role']['description']}"
    )


def format_buff_details_text(key: str, stats: dict | None) -> str:
    item = BUFF_CATALOG[key]
    inventory_key = str(item["inventory_key"])
    owned = int((stats or {}).get(inventory_key, 0))
    status_line = ""
    if key in {"documents", "shield"}:
        room = get_player_profile_room(int((stats or {}).get("user_id", 0))) if stats is not None else None
        if room is not None and room.get_player(int((stats or {}).get("user_id", 0))) is not None:
            user_id = int((stats or {}).get("user_id", 0))
            if key == "shield":
                if user_id in room.spent_shield_user_ids:
                    status_line = "🧯 На эту игру защита уже была потрачена.\n\n"
                elif user_id in room.shielded_user_ids:
                    status_line = "✨ Защита активна в текущей игре.\n\n"
                elif owned > 0:
                    status_line = "⏳ Купленная во время этой игры защита сработает только в следующей партии.\n\n"
            if key == "documents":
                if user_id in room.spent_documents_user_ids:
                    status_line = "📂 На эту игру документы уже были использованы.\n\n"
                elif user_id in room.documented_user_ids:
                    status_line = "✨ Документы активны в текущей игре.\n\n"
                elif owned > 0:
                    status_line = "⏳ Купленные во время этой игры документы сработают только в следующей партии.\n\n"
    return (
        f"<b>{item['title']}</b>\n\n"
        f"{item['description']}\n\n"
        f"💰 Цена: <b>{item['price']}</b>\n"
        f"🎒 В инвентаре: <b>{owned}</b>\n\n"
        f"{status_line}"
        f"{item['details']}\n\n"
        "<i>Активная роль пока остаётся каркасом. Защита и Документы уже работают.</i>"
    )


def prime_room_documents(room) -> None:
    room.documented_user_ids.clear()
    room.spent_documents_user_ids.clear()
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None:
            continue
        if int(stats.get("buff_documents", 0)) > 0:
            room.arm_documents(player.user_id)


def prime_room_shields(room) -> None:
    room.shielded_user_ids.clear()
    room.spent_shield_user_ids.clear()
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None:
            continue
        if int(stats.get("buff_shield", 0)) > 0:
            room.arm_shield(player.user_id)


async def send_endgame_currency_summaries(bot: Bot, room) -> None:
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None:
            continue
        won = False
        if room.winner_team == "Мафия":
            won = player.role in MAFIA_ROLES
        elif room.winner_team == "Маньяк":
            won = player.role == ROLE_MANIAC
        elif room.winner_team == "Мирные жители":
            won = player.role not in MAFIA_ROLES and player.role != ROLE_MANIAC
        try:
            await bot.send_message(
                player.user_id,
                format_endgame_currency_text(player, stats, won),
                **private_game_send_kwargs(room),
            )
        except Exception:
            continue


async def notify_missing_delete_permission_once(bot: Bot, chat_id: int) -> None:
    if chat_id in delete_permission_alerted_chats:
        return
    delete_permission_alerted_chats.add(chat_id)
    try:
        await bot.send_message(
            chat_id,
            "Я не могу удалять сообщения. Выдайте боту право администратора: Удалять сообщения.",
        )
    except Exception:
        pass


async def safe_delete_message(message: Message) -> None:
    try:
        await message.delete()
    except TelegramForbiddenError:
        if message.chat.type in {"group", "supergroup"}:
            await notify_missing_delete_permission_once(message.bot, message.chat.id)
        return
    except TelegramBadRequest as e:
        error_text = str(e).lower()
        if message.chat.type in {"group", "supergroup"} and (
            "not enough rights" in error_text
            or "have no rights" in error_text
            or "message can't be deleted" in error_text
            or "message cannot be deleted" in error_text
        ):
            await notify_missing_delete_permission_once(message.bot, message.chat.id)
        return
    except Exception:
        return


async def cleanup_group_command_message(message: Message) -> None:
    if message.chat.type in {"group", "supergroup"}:
        await safe_delete_message(message)


async def delete_message_later(message: Message, delay_seconds: int) -> None:
    await asyncio.sleep(delay_seconds)
    await safe_delete_message(message)


def should_send_chat_welcome(chat_id: int, user_id: int, ttl_seconds: int = 30) -> bool:
    now = time.monotonic()
    expired_keys = [key for key, timestamp in recent_chat_welcomes.items() if now - timestamp > ttl_seconds]
    for key in expired_keys:
        recent_chat_welcomes.pop(key, None)

    welcome_key = (chat_id, user_id)
    previous = recent_chat_welcomes.get(welcome_key)
    if previous is not None and now - previous <= ttl_seconds:
        return False

    recent_chat_welcomes[welcome_key] = now
    return True


async def send_group_welcome(bot: Bot, chat_id: int, user: User) -> None:
    if user.is_bot:
        return
    if not should_send_chat_welcome(chat_id, user.id):
        return

    start_link = await bot_start_link(bot)
    safe_name = escape(user_nickname(user))
    sent = await bot.send_message(
        chat_id,
        (
            f"Привет, {safe_name} 👋\n"
            "Добро пожаловать в <b>Loft Mafia Bot</b> 🎭\n\n"
            "❗️Перед началом игры просим ознакомиться с правилами — @rules_loft ❗️\n\n"
            "🎮 Чтобы начать игру, нажми:\n"
            f"👉 <a href=\"{start_link}\">Начать в боте</a>\n\n"
            "🤍 Прекрасных игр вам и хорошего настроения! 🤍"
        ),
    )
    asyncio.create_task(delete_message_later(sent, 60))


def get_or_create_penalty(chat_id: int, user_id: int) -> dict[str, float | int | bool]:
    by_chat = chat_penalties.setdefault(chat_id, {})
    state = by_chat.get(user_id)
    if state is None:
        state = {
            "warned": False,
            "current_block_seconds": 0,
            "blocked_until": 0.0,
        }
        by_chat[user_id] = state
    return state


def is_user_blocked(chat_id: int, user_id: int) -> bool:
    state = get_or_create_penalty(chat_id, user_id)
    blocked_until = float(state["blocked_until"])
    return blocked_until > time.time()


def blocked_seconds_left(chat_id: int, user_id: int) -> int:
    state = get_or_create_penalty(chat_id, user_id)
    blocked_until = float(state["blocked_until"])
    remaining = int(blocked_until - time.time())
    return remaining if remaining > 0 else 0


async def notify_registration_blocked(bot: Bot, chat_id: int, user_id: int) -> None:
    remaining = blocked_seconds_left(chat_id, user_id)
    if remaining <= 0:
        return
    try:
        await bot.send_message(
            user_id,
            (
                "Ты временно не можешь зарегистрироваться в игру.\n"
                f"На тебе действует мут игрового чата: еще {remaining} сек."
            ),
        )
    except Exception:
        return


async def is_group_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in {"administrator", "creator"}


async def bot_has_delete_permission(bot: Bot, chat_id: int) -> bool:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
    except Exception:
        return False

    if member.status == "creator":
        return True
    if member.status != "administrator":
        return False
    return bool(getattr(member, "can_delete_messages", False))


async def process_rule_violation(message: Message) -> None:
    if message.from_user is None:
        return

    chat_id = message.chat.id
    user_id = message.from_user.id
    state = get_or_create_penalty(chat_id, user_id)

    if not bool(state["warned"]):
        state["warned"] = True
        try:
            await message.bot.send_message(
                user_id,
                (
                    "Предупреждение: ты нарушил правила игрового чата.\n"
                    "Следующее нарушение: блокировка писать в чат на 10 секунд.\n"
                    "Каждое следующее нарушение увеличивает блокировку на +10 секунд."
                ),
            )
        except Exception:
            pass
        return

    next_block = int(state["current_block_seconds"]) + 10
    state["current_block_seconds"] = next_block
    state["blocked_until"] = time.time() + next_block

    try:
        await message.bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_audios=False,
                can_send_documents=False,
                can_send_photos=False,
                can_send_videos=False,
                can_send_video_notes=False,
                can_send_voice_notes=False,
                can_send_polls=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False,
                can_change_info=False,
                can_invite_users=False,
                can_pin_messages=False,
            ),
            until_date=datetime.now() + timedelta(seconds=next_block),
        )
    except Exception:
        pass

    try:
        await message.bot.send_message(
            user_id,
            f"Блокировка писать в игровой чат: {next_block} сек.",
        )
    except Exception:
        pass


def user_nickname(user: User) -> str:
    nickname = (user.full_name or "").strip()
    if nickname:
        return nickname
    if user.username:
        return f"@{user.username}"
    return f"Игрок {user.id}"


def player_display_name(player) -> str:
    name = (player.full_name or "").strip()
    if name:
        return name
    return f"Игрок {player.user_id}"


def player_profile_link(player) -> str:
    safe_name = escape(player_display_name(player))
    return f"<a href=\"tg://user?id={player.user_id}\">{safe_name}</a>"


async def registration_join_link(message: Message, chat_id: int) -> str:
    me = await message.bot.get_me()
    return f"https://t.me/{me.username}?start=join_{chat_id}"


async def bot_start_link(bot: Bot) -> str:
    me = await bot.get_me()
    return f"https://t.me/{me.username}?start=welcome"


def registration_text(room) -> str:
    lines = ["<b>Ведётся набор в игру</b>", ""]
    if not room.players:
        lines.append("Зарегистрировались::")
        lines.append("Пока никто не зарегистрировался.")
        lines.append("")
        lines.append("Итого <b>0</b> чел.")
        return "\n".join(lines)

    joined_names = ", ".join(player_profile_link(player) for player in room.players.values())
    lines.append("Зарегистрировались::")
    lines.append(joined_names)
    lines.append("")
    lines.append(f"Итого <b>{len(room.players)}</b> чел.")
    return "\n".join(lines)


def registration_post_text(room) -> str:
    remaining = registration_remaining_seconds(room)
    if remaining <= 0:
        remaining = int(room_chat_settings(room)["timings"]["registration"])
    return registration_text(room) + f"\n\nДо окончания регистрации осталось <b>{remaining}</b> сек."


async def private_bot_link(bot: Bot) -> str:
    me = await bot.get_me()
    return f"https://t.me/{me.username}"


async def night_action_keyboard(bot: Bot) -> InlineKeyboardMarkup:
    link = await private_bot_link(bot)
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Посмотреть роль", url=link)]],
    )


def trial_vote_keyboard(chat_id: int, yes_count: int, no_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"👍 {yes_count}", callback_data=f"trial:yes:{chat_id}"),
            ],
            [
                InlineKeyboardButton(text=f"👎 {no_count}", callback_data=f"trial:no:{chat_id}"),
            ],
        ]
    )


async def refresh_registration_post(message: Message, room) -> None:
    if room.registration_message_id is None:
        return
    join_link = await registration_join_link(message, room.chat_id)
    try:
        await message.bot.edit_message_text(
            chat_id=room.chat_id,
            message_id=room.registration_message_id,
            text=registration_post_text(room),
            reply_markup=registration_lobby_keyboard(join_link),
        )
    except Exception:
        return


async def pin_registration_post(bot: Bot, room) -> None:
    if room.registration_message_id is None:
        return
    try:
        await bot.pin_chat_message(
            chat_id=room.chat_id,
            message_id=room.registration_message_id,
            disable_notification=True,
        )
    except Exception:
        return


async def clear_registration_post(bot: Bot, room) -> None:
    message_id = room.registration_message_id
    if message_id is None:
        return

    room.registration_message_id = None
    persist_room(room)

    try:
        await bot.unpin_chat_message(chat_id=room.chat_id, message_id=message_id)
    except Exception:
        pass

    try:
        await bot.delete_message(chat_id=room.chat_id, message_id=message_id)
    except Exception:
        pass


def registration_lobby_keyboard(join_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🕴🏻 Присоединиться", url=join_link),
            ],
        ]
    )


def registration_panel(panel_message_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Создать лобби", callback_data=f"reg:start:{panel_message_id}"),
            ],
        ]
    )


def private_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👤 Игровой профиль", callback_data="pmenu:profile"),
                InlineKeyboardButton(text="🎭 Роли", callback_data="pmenu:roles"),
            ],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="pmenu:stats")],
        ]
    )


def private_profile_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Магазин", callback_data="pmenu:buffs")],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="pmenu:main")],
        ]
    )


def private_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="pmenu:main")]]
    )


def private_buffs_shop_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📁 Документы - 💵150", callback_data="pmenu:buff:documents")],
            [InlineKeyboardButton(text="🛡 Защита - 💵100", callback_data="pmenu:buff:shield")],
            [InlineKeyboardButton(text="🕺 Активная роль - 💎1", callback_data="pmenu:buff:active_role")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="pmenu:profile")],
        ]
    )


def private_buff_details_keyboard(key: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    item = BUFF_CATALOG[key]
    rows.append([InlineKeyboardButton(text=f"Купить за {item['price']}", callback_data=f"pmenu:buy:{key}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="pmenu:buffs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


PRIVATE_ROLE_ORDER = [
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


def private_roles_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, role in enumerate(PRIVATE_ROLE_ORDER):
        emoji = ROLE_EMOJI.get(role, "")
        rows.append([InlineKeyboardButton(text=f"{emoji} {role}".strip(), callback_data=f"pmenu:role:{idx}")])
    rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="pmenu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def private_back_to_roles_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="pmenu:roles")]]
    )


def private_role_details_text(role: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    description = ROLE_DESCRIPTION.get(role, "Описание пока не добавлено.")
    action_rule = ROLE_ACTION_RULES.get(role, "Механика роли пока не добавлена.")
    return (
        f"{emoji} <b>{role}</b>\n\n"
        f"{description}\n\n"
        f"<b>Как ходит роль</b>\n{action_rule}"
    )


def private_settings_main_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎭 Роли", callback_data=settings_callback_data(chat_id, "roles"))],
            [InlineKeyboardButton(text="🕐 Тайминги", callback_data=settings_callback_data(chat_id, "timings"))],
            [InlineKeyboardButton(text="🙊 Молчанка", callback_data=settings_callback_data(chat_id, "mute"))],
            [InlineKeyboardButton(text="🛠 Разное", callback_data=settings_callback_data(chat_id, "misc"))],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=settings_callback_data(chat_id, "close"))],
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


def get_private_action_room(user_id: int):
    active_rooms = []
    for room in storage.rooms.values():
        if not room.started:
            continue
        player = room.get_player(user_id)
        if player is not None and player.alive:
            active_rooms.append(room)

    if len(active_rooms) == 1:
        return active_rooms[0]
    return None


def get_player_profile_room(user_id: int):
    rooms = []
    for room in storage.rooms.values():
        player = room.get_player(user_id)
        if player is None:
            continue
        if not room.started or room.phase == PHASE_FINISHED:
            continue
        rooms.append(room)
    if len(rooms) == 1:
        return rooms[0]
    return None


def get_pending_last_word_room(user_id: int):
    rooms = []
    for room in storage.rooms.values():
        if room.can_send_last_word(user_id):
            rooms.append(room)
    if len(rooms) == 1:
        return rooms[0]
    return None


def selected_target_for_actor(room, actor_user_id: int) -> int | None:
    actor = room.get_player(actor_user_id)
    if actor is None:
        return None

    if room.phase == "night":
        if actor_user_id in getattr(room, "night_skipped_user_ids", set()):
            return 0
        if actor.role in {"Дон", "Мафия"}:
            return room.night_votes.get(actor_user_id)
        if actor.role == "Доктор":
            return room.doctor_target_id
        if actor.role == "Комиссар Каттани":
            if room.commissar_action_mode == "shoot":
                return room.commissar_shot_target_id
            return room.commissar_target_id
        if actor.role == ROLE_ADVOCATE:
            return room.advocate_target_id
        if actor.role == "Маньяк":
            return room.maniac_target_id
        if actor.role == "Любовница":
            return room.mistress_target_id
        if actor.role == "Бомж":
            return room.bum_target_id
        if actor.role == ROLE_KAMIKAZE and room.kamikaze_pending_user_id == actor_user_id:
            return room.kamikaze_target_id

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION:
        return room.day_votes.get(actor_user_id)

    return None


def build_action_keyboard(room, actor_user_id: int) -> InlineKeyboardMarkup | None:
    actor = room.get_player(actor_user_id)
    if actor is None:
        return None

    if (
        room.phase == PHASE_DAY
        and room.day_stage == DAY_STAGE_NOMINATION
        and room.day_silenced_user_id is not None
        and actor.user_id == room.day_silenced_user_id
    ):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Пока все голосуют - ты лечишься...",
                        callback_data="noop:silenced",
                    )
                ]
            ]
        )

    kamikaze_revenge_mode = (
        room.phase == PHASE_NIGHT
        and actor.role == ROLE_KAMIKAZE
        and room.kamikaze_pending_user_id == actor_user_id
    )
    if not actor.alive and not kamikaze_revenge_mode:
        return None

    rows: list[list[InlineKeyboardButton]] = []
    alive_players = room.alive_players()
    alive_targets = [p for p in alive_players if p.user_id != actor_user_id]
    selected_target_id = selected_target_for_actor(room, actor_user_id)

    def mark(name: str, user_id: int) -> str:
        return f"✅ {name}" if selected_target_id == user_id else name

    def target_label(target, teammate_mark: str = "") -> str:
        base_name = player_display_name(target)
        extra_mark = ""
        if actor.role == ROLE_COMMISSAR:
            known_role = room.commissar_known_roles.get(target.user_id)
            if known_role:
                extra_mark = ROLE_EMOJI.get(known_role, "")
        return f"{teammate_mark} {extra_mark} {base_name}".strip()

    if room.phase == "night":
        if actor.role in {"Дон", "Мафия"}:
            for target in alive_targets:
                if not allow_team_kill_enabled(room) and target.role in {ROLE_DON, ROLE_MAFIA}:
                    continue
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:kill:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Доктор":
            for target in room.alive_players():
                if room.doctor_self_heal_used and target.user_id == actor_user_id:
                    continue
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:heal:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Комиссар Каттани":
            if commissar_can_shoot_this_night(room) and room.commissar_action_mode is None:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text="Проверить",
                            callback_data=f"act:commode:{room.chat_id}:1",
                        )
                    ]
                )
                rows.append(
                    [
                        InlineKeyboardButton(
                            text="Стрелять",
                            callback_data=f"act:commode:{room.chat_id}:2",
                        )
                    ]
                )
            elif commissar_can_shoot_this_night(room) and room.commissar_action_mode == "shoot":
                for target in alive_targets:
                    if not allow_team_kill_enabled(room) and target.role in {ROLE_COMMISSAR, ROLE_SERGEANT}:
                        continue
                    rows.append(
                        [
                            InlineKeyboardButton(
                                text=mark(target_label(target), target.user_id),
                                callback_data=f"act:cshot:{room.chat_id}:{target.user_id}",
                            )
                        ]
                    )
            else:
                for target in alive_targets:
                    rows.append(
                        [
                            InlineKeyboardButton(
                                text=mark(target_label(target), target.user_id),
                                callback_data=f"act:check:{room.chat_id}:{target.user_id}",
                            )
                        ]
                    )
        if actor.role == ROLE_ADVOCATE:
            for target in room.alive_players():
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:advocate:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Маньяк":
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:maniac:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Любовница":
            for target in alive_targets:
                if room.mistress_last_target_id is not None and target.user_id == room.mistress_last_target_id:
                    continue
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:mistress:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Бомж":
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:bum:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if kamikaze_revenge_mode:
            for target in alive_players:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target), target.user_id),
                            callback_data=f"act:kamikaze:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION:
        mafia_teammate_ids: set[int] = set()
        if actor.role in {ROLE_DON, ROLE_MAFIA}:
            mafia_teammate_ids = {p.user_id for p in alive_players if p.role in {ROLE_DON, ROLE_MAFIA}}

        for target in alive_targets:
            teammate_mark = " 🤵🏻" if target.user_id in mafia_teammate_ids else ""
            rows.append(
                [
                    InlineKeyboardButton(
                        text=mark(target_label(target, teammate_mark), target.user_id),
                        callback_data=f"act:vote:{room.chat_id}:{target.user_id}",
                    )
                ]
            )
        if day_vote_skip_enabled(room):
            skip_text = "✅ Пропустить ход" if selected_target_id == 0 else "Пропустить ход"
            rows.append(
                [
                    InlineKeyboardButton(
                        text=skip_text,
                        callback_data=f"act:skipvote:{room.chat_id}:0",
                    )
                ]
            )

    if room.phase == "night" and rows and night_action_skip_enabled(room):
        skip_text = "✅ Пропустить ход" if selected_target_id == 0 else "Пропустить ход"
        rows.append(
            [
                InlineKeyboardButton(
                    text=skip_text,
                    callback_data=f"noop:skip:{room.chat_id}",
                )
            ]
        )

    if not rows:
        return None

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_action_prompt_text(room, actor_user_id: int) -> str:
    actor = room.get_player(actor_user_id)
    if actor is None:
        return "Выбери действие на текущую фазу:"

    if (
        room.phase == PHASE_NIGHT
        and actor.role == ROLE_KAMIKAZE
        and room.kamikaze_pending_user_id == actor_user_id
    ):
        return "Тебя линчевали на дневном собрании :(\nКого заберём с собой в могилу?"

    if not actor.alive:
        return "Выбери действие на текущую фазу:"

    if room.phase == "night":
        if actor.role in {ROLE_DON, ROLE_MAFIA}:
            return "<b>Мафия проводит голосование за следующую жертву:</b>"
        if actor.role == ROLE_MANIAC:
            return "<b>Кого будет убивать?</b>"
        if actor.role == ROLE_MISTRESS:
            return "<b>С кем будем спать?</b>"
        if actor.role == ROLE_DOCTOR:
            return "<b>Кого будем лечить?</b>"
        if actor.role == ROLE_BUM:
            return "<b>К кому пойдём за бутылкой?</b>"
        if actor.role == ROLE_ADVOCATE:
            return "<b>Кого будем защищать от правосудия?</b>"
        if actor.role == ROLE_COMMISSAR:
            if commissar_can_shoot_this_night(room):
                if room.commissar_action_mode is None:
                    return "<b>Проверить или стрелять?</b>"
                if room.commissar_action_mode == "shoot":
                    return "<b>Кого будем убивать?</b>"
            return "<b>Кого будем проверять?</b>"
        return "Сейчас у твоей роли нет активных ночных действий."

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION:
        if room.day_silenced_user_id is not None and actor.user_id == room.day_silenced_user_id:
            return "Пока все голосуют - ты лечишься. 💃🏼 Любовница постаралась..."
        return "Пришло время искать виноватых!\nКого ты хочешь линчевать?"

    return "Выбери действие на текущую фазу:"


def night_status_text(room) -> str:
    alive = room.alive_players()
    seat_positions = {p.user_id: i for i, p in enumerate(room.players.values(), start=1)}

    def format_sleep_left(seconds: int) -> str:
        total = max(0, int(seconds))
        minutes, secs = divmod(total, 60)
        if minutes > 0 and secs > 0:
            return f"{minutes} мин. {secs} сек."
        if minutes > 0:
            return f"{minutes} мин."
        return f"{secs} сек."

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

    lines.append(f"\n<b>Спать осталось {format_sleep_left(int(room_chat_settings(room)['timings']['night']))}</b>")
    return "\n".join(lines)


async def refresh_private_action_message(callback: CallbackQuery, room, actor_user_id: int, status_text: str | None = None) -> None:
    keyboard = build_action_keyboard(room, actor_user_id)
    if keyboard is None:
        return

    prompt = build_action_prompt_text(room, actor_user_id)
    text = prompt if not status_text else f"{prompt}\n\n{status_text}"
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=keyboard)
        except Exception:
            pass


async def send_action_menu(message: Message) -> None:
    if message.chat.type != "private":
        await message.answer("Игровые действия выполняются в ЛС бота.")
        return

    room = get_private_action_room(message.from_user.id)
    if room is None:
        await message.answer(
            "Не нашел активную игру для твоего аккаунта. "
            "Если игр несколько, пока поддерживается только одна активная игра на игрока."
        )
        return

    keyboard = build_action_keyboard(room, message.from_user.id)
    if keyboard is None:
        await message.answer("Сейчас у твоей роли нет доступных действий.", **private_game_send_kwargs(room))
        return

    prompt_text = build_action_prompt_text(room, message.from_user.id)
    sent = await message.answer(prompt_text, reply_markup=keyboard, **private_game_send_kwargs(room))
    track_action_menu_message(room.chat_id, message.from_user.id, sent.message_id)


async def push_phase_action_menus(bot: Bot, room) -> None:
    for player in room.alive_players():
        keyboard = build_action_keyboard(room, player.user_id)
        if keyboard is None:
            continue
        try:
            prompt_text = build_action_prompt_text(room, player.user_id)
            sent = await bot.send_message(
                player.user_id,
                prompt_text,
                reply_markup=keyboard,
                **private_game_send_kwargs(room),
            )
            track_action_menu_message(room.chat_id, player.user_id, sent.message_id)
        except Exception:
            await bot.send_message(
                room.chat_id,
                f"Не смог отправить меню хода игроку {player.full_name}."
                " Пусть напишет боту /start в личке."
            )


async def push_kamikaze_revenge_menu(bot: Bot, room) -> None:
    user_id = room.kamikaze_pending_user_id
    if user_id is None:
        return
    if room.kamikaze_target_id is not None:
        return

    keyboard = build_action_keyboard(room, user_id)
    if keyboard is None:
        return

    try:
        prompt_text = build_action_prompt_text(room, user_id)
        sent = await bot.send_message(user_id, prompt_text, reply_markup=keyboard, **private_game_send_kwargs(room))
        track_action_menu_message(room.chat_id, user_id, sent.message_id)
    except Exception:
        await bot.send_message(
            room.chat_id,
            "Не смог отправить меню камикадзе. Пусть напишет боту /start в личке.",
        )


async def push_trial_vote_menus(bot: Bot, room, candidate) -> None:
    candidate_name = player_display_name(candidate)
    print(f"[TRIAL] push_trial_vote_menus: candidate={candidate_name}, chat_id={room.chat_id}")
    yes_count, no_count = room.trial_vote_counts()
    try:
        await bot.send_message(
            room.chat_id,
            trial_vote_prompt_text(room, candidate),
            reply_markup=trial_vote_keyboard(room.chat_id, yes_count, no_count),
        )
    except Exception as e:
        print(f"[ERROR] push_trial_vote_menus: chat_id={room.chat_id}, error={e!r}")


async def send_mafia_private_update(room, bot, text: str) -> None:
    for player in room.alive_players():
        if player.role not in {ROLE_DON, ROLE_MAFIA}:
            continue
        try:
            await bot.send_message(player.user_id, text, **private_game_send_kwargs(room))
        except Exception:
            continue


async def announce_don_transfer(room, bot: Bot, don_successor_id: int | None) -> None:
    if don_successor_id is None:
        return

    new_don = room.get_player(don_successor_id)
    if new_don is None:
        return

    don_name = player_display_name(new_don)
    await bot.send_message(room.chat_id, "🤵🏼 Мафия унаследовал роль 🤵🏻 Дон")
    await send_mafia_private_update(
        room,
        bot,
        f"{don_name} - новый 🤵🏻 Дон",
    )


async def announce_commissar_transfer(room, bot: Bot, commissar_successor_id: int | None) -> None:
    if commissar_successor_id is None:
        return

    await bot.send_message(room.chat_id, "👮🏼‍♂️ Сержант унаследовал роль 🕵️‍ Комиссар Каттани")
    try:
        await bot.send_message(
            commissar_successor_id,
            "Теперь ты 🕵️‍ Комиссар Каттани",
            **private_game_send_kwargs(room),
        )
    except Exception:
        pass


async def prompt_last_words(bot: Bot, room, eliminated) -> None:
    if room.phase == PHASE_FINISHED:
        return
    queued_user_ids = room.queue_last_words(eliminated)
    if queued_user_ids:
        persist_room(room)
    for user_id in queued_user_ids:
        try:
            await bot.send_message(
                user_id,
                (
                    "<b>Тебя убили :(</b>\n"
                    "Ты можешь отправить сюда своё предсмертное сообщение"
                ),
                **private_game_send_kwargs(room),
            )
        except Exception:
            continue


def compact_night_report_messages(lines: list[str]) -> list[str]:
    if not lines:
        return []

    messages: list[str] = []
    i = 0
    while i < len(lines):
        current = lines[i]
        next_line = lines[i + 1] if i + 1 < len(lines) else None

        if current == "Тебя убили :(" and next_line == "Ты можешь отправить сюда своё предсмертное сообщение":
            messages.append(f"<b>{current}</b>\n{next_line}")
            i += 2
            continue

        messages.append(current)
        i += 1

    return messages


async def process_night_end(bot: Bot, chat_id: int, timer_reason: str | None = None) -> None:
    lock = get_phase_lock(chat_id)
    async with lock:
        room = storage.get_room(chat_id)
        if room is None or room.phase != "night":
            return

        cancel_phase_timer(chat_id)
        mafia_alive_tonight = any(player.alive and player.role in {ROLE_DON, ROLE_MAFIA} for player in room.players.values())
        mafia_target_tonight = room.current_mafia_target_id()
        skipped_user_ids = night_skipped_user_ids(room)
        await mark_skipped_night_menus(bot, room, skipped_user_ids)
        (
            ok,
            info,
            eliminated,
            don_transfer_note,
            don_successor_id,
            commissar_transfer_note,
            commissar_successor_id,
        ) = room.resolve_night()
        if not ok:
            await bot.send_message(chat_id, info)
            return

        if timer_reason:
            try:
                await bot.send_message(chat_id, timer_reason)
            except Exception as e:
                print(f"[ERROR] process_night_end: failed to send timer_reason for chat_id={chat_id}, error={e!r}")

        if mafia_alive_tonight and mafia_target_tonight is not None and not room.mafia_target_announced:
            await safe_send_message(bot, chat_id, "🤵🏻 Мафия определилась с общей целью.")
            room.mafia_target_announced = True

        reports = room.pop_night_reports()
        kill_sources = room.pop_night_kill_sources()
        afk_killed_ids = set(room.afk_killed_user_ids)
        lucky_triggered = False
        for user_id, lines in reports.items():
            try:
                for message_text in compact_night_report_messages(lines):
                    if "пытались убить, но тебе повезло" in message_text.lower():
                        lucky_triggered = True
                    await safe_send_message(bot, user_id, message_text, **private_game_send_kwargs(room))
            except Exception:
                continue

        spent_shield_user_ids = room.pop_spent_shield_user_ids()
        shield_triggered = False
        for user_id in spent_shield_user_ids:
            if repo.consume_shield_buff(user_id):
                shield_triggered = True

        spent_documents_user_ids = room.pop_spent_documents_user_ids()
        for user_id in spent_documents_user_ids:
            repo.consume_documents_buff(user_id)

        if shield_triggered:
            await bot.send_message(chat_id, "🌟 Кто-то из игроков потратил защиту")

        if lucky_triggered:
            await bot.send_message(chat_id, "☝️ Кому-то из игроков повезло")

        if mafia_alive_tonight and mafia_target_tonight is None:
            await bot.send_message(chat_id, "🚷 🤵🏻 Дон сегодня отдыхает")

        await send_phase_media(bot, chat_id, room.day_media_caption(), DAY_IMAGE_PATH)

        if eliminated:
            show_killers = bool(room_chat_settings(room).get("misc", {}).get("show_killers", False))
            show_roles = show_roles_enabled(room)
            for dead in eliminated:
                sources = kill_sources.get(dead.user_id, [])
                killer_text = format_killer_sources_text(sources) if show_killers else ""
                dead_mark = player_profile_link(dead)
                if show_roles:
                    role_text = role_mark_text(dead.role)
                    text = f"Сегодня был жестоко убит {role_text} {dead_mark}"
                else:
                    text = f"Сегодня был жестоко убит {dead_mark}"
                if killer_text:
                    text += f"\n{killer_text}"
                await bot.send_message(chat_id, text)
            if room.phase != PHASE_FINISHED:
                non_afk_eliminated = [player for player in eliminated if player.user_id not in afk_killed_ids]
                await prompt_last_words(bot, room, non_afk_eliminated)

            for dead in eliminated:
                if dead.user_id not in afk_killed_ids:
                    continue
                try:
                    await safe_send_message(
                        bot,
                        dead.user_id,
                        "Ты бездействовал больше 2 ночей подряд и был убит...",
                        **private_game_send_kwargs(room),
                    )
                except Exception:
                    pass
                dead_mark = player_profile_link(dead)
                await safe_send_message(
                    bot,
                    chat_id,
                    "Кто-то из жителей слышал, как "
                    f"{dead_mark} кричал перед смертью:\n"
                    "<b>Я больше не бу-у-у-у-ду спать во время игры-ы-ы-ы-ы-ы-!</b>",
                )
        else:
            await bot.send_message(chat_id, "🤷 Удивительно, но этой ночью все выжили")

        if don_transfer_note:
            await announce_don_transfer(room, bot, don_successor_id)
        if commissar_transfer_note:
            await announce_commissar_transfer(room, bot, commissar_successor_id)

        room.afk_killed_user_ids.clear()

        day_summary = (
            room.alive_players_text()
            + "\n\n"
            + room.alive_role_hints_text()
            + "\n\n"
            + "Сейчас самое время обсудить результаты ночи, разобраться в причинах и следствиях..."
        )
        await bot.send_message(chat_id, day_summary)

        if room.phase == PHASE_FINISHED:
            room.pending_last_words.clear()
            stats_already_recorded = room.stats_recorded
            ensure_stats_recorded(room)
            if not stats_already_recorded:
                await send_endgame_currency_summaries(bot, room)
            await bot.send_message(chat_id, room.final_report_text())
            cancel_phase_timer(chat_id)
            persist_room(room)
            return

        room.start_day_discussion()
        await start_phase_timer(room, bot)
        persist_room(room)


async def process_day_end(bot: Bot, chat_id: int, timer_reason: str | None = None) -> None:

    lock = get_phase_lock(chat_id)
    async with lock:
        print(f"[PHASE] process_day_end called for chat_id={chat_id}")
        room = storage.get_room(chat_id)
        if room is None:
            print(f"[PHASE] process_day_end: room is None for chat_id={chat_id}")
            return
        if room.phase != "day":
            print(f"[PHASE] process_day_end: phase is not 'day' (actual: {room.phase}) for chat_id={chat_id}")
            return

        print(f"[PHASE] process_day_end: current day_stage={room.day_stage}")
        cancel_phase_timer(chat_id)

        if timer_reason:
            print(f"[PHASE] process_day_end: timer_reason={timer_reason}")
            try:
                await bot.send_message(chat_id, timer_reason)
            except Exception as e:
                print(f"[ERROR] process_day_end: failed to send timer_reason for chat_id={chat_id}, error={e!r}")

        if room.day_stage == DAY_STAGE_DISCUSSION:
            print(f"[PHASE] process_day_end: switching to nomination stage for chat_id={chat_id}")
            room.start_day_nomination()
            persist_room(room)
            private_keyboard = await night_action_keyboard(bot)
            await bot.send_message(
                chat_id,
                (
                    "<b>Пришло время определить и наказать виновных.</b>\n"
                    f"Голосование продлится {int(room_chat_settings(room)['timings']['vote'])} секунд"
                ),
                reply_markup=private_keyboard,
            )
            await push_phase_action_menus(bot, room)
            await start_phase_timer(room, bot)
            print(f"[PHASE] process_day_end: nomination stage started, timer set for chat_id={chat_id}")
            return

        if room.day_stage == DAY_STAGE_NOMINATION:
            ok, candidate_id = room.resolve_day_nomination()
            print(f"[PHASE] process_day_end: nomination resolved for chat_id={chat_id}, ok={ok}, candidate_id={candidate_id}")
            if not ok:
                print(f"[PHASE] process_day_end: nomination resolve failed for chat_id={chat_id}")
                await bot.send_message(chat_id, "Не удалось обработать выбор кандидата.")
                return

            if candidate_id is None:
                print(f"[PHASE] process_day_end: no single candidate selected, ending day without lynch for chat_id={chat_id}")
                actual_votes = [
                    target_id
                    for target_id in room.day_votes.values()
                    if target_id and (room.get_player(target_id) is not None)
                ]
                if actual_votes:
                    await bot.send_message(
                        chat_id,
                        "Голоса на этапе выбора кандидата разделились поровну.\n"
                        "🗿 Жители решили никого не вешать...",
                    )
                else:
                    await bot.send_message(
                        chat_id,
                        "Голосование окончено\n🗿 Жители решили никого не вешать...",
                    )

                ok_end, info_end = room.end_day_no_lynch()
                print(f"[PHASE] process_day_end: end_day_no_lynch result for chat_id={chat_id}, ok_end={ok_end}, info={info_end}")
                if ok_end:
                    keyboard = await night_action_keyboard(bot)
                    await send_phase_media(
                        bot,
                        chat_id,
                        room.night_media_caption(),
                        NIGHT_IMAGE_PATH,
                        reply_markup=keyboard,
                    )
                    await bot.send_message(
                        chat_id,
                        night_status_text(room),
                        reply_markup=keyboard,
                    )
                    await push_phase_action_menus(bot, room)
                    await push_kamikaze_revenge_menu(bot, room)
                    await start_phase_timer(room, bot)
                    persist_room(room)
                    print(f"[PHASE] process_day_end: transitioned to night after no-lynch for chat_id={chat_id}")
                else:
                    await bot.send_message(chat_id, info_end)
                return

            candidate = room.get_player(candidate_id)
            if candidate is None:
                print(f"[PHASE] process_day_end: candidate_id={candidate_id} not found, forcing no-lynch for chat_id={chat_id}")
                await bot.send_message(chat_id, "Кандидат не найден. День завершается без повешения.")
                ok_end, _ = room.end_day_no_lynch()
                if ok_end:
                    keyboard = await night_action_keyboard(bot)
                    await send_phase_media(
                        bot,
                        chat_id,
                        room.night_media_caption(),
                        NIGHT_IMAGE_PATH,
                        reply_markup=keyboard,
                    )
                    await bot.send_message(
                        chat_id,
                        night_status_text(room),
                        reply_markup=keyboard,
                    )
                    await push_phase_action_menus(bot, room)
                    await push_kamikaze_revenge_menu(bot, room)
                    await start_phase_timer(room, bot)
                    persist_room(room)
                    print(f"[PHASE] process_day_end: transitioned to night after missing candidate fallback for chat_id={chat_id}")
                return

            room.start_day_trial(candidate.user_id)
            print(f"[PHASE] process_day_end: trial started for chat_id={chat_id}, candidate_id={candidate.user_id}, candidate_name={candidate.full_name}")
            persist_room(room)
            await push_trial_vote_menus(bot, room, candidate)
            await start_phase_timer(room, bot)
            print(f"[PHASE] process_day_end: trial menus sent and timer started for chat_id={chat_id}")
            return

        if room.day_stage == DAY_STAGE_TRIAL:
            yes_count, no_count = room.trial_vote_counts()
            print(f"[PHASE] process_day_end: resolving trial for chat_id={chat_id}, yes_count={yes_count}, no_count={no_count}, votes={room.trial_votes}")
            (
                ok,
                info,
                eliminated,
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            ) = room.resolve_day_trial()
            if not ok:
                print(f"[PHASE] process_day_end: trial resolve failed for chat_id={chat_id}, info={info}")
                await safe_send_message(bot, chat_id, info)
                return

            if eliminated:
                first = eliminated[0]
                first_mark = player_profile_link(first)
                verdict_target = "обвиняемого" if is_secret_voting_enabled(room) else first_mark
                reveal_roles = show_roles_enabled(room)
                await safe_send_message(
                    bot,
                    chat_id,
                    f"<b>Результаты голосования:</b>\n<b>{yes_count}</b> 👍  |  <b>{no_count}</b> 👎\n\nВешаем {verdict_target}! :)",
                )
                if reveal_roles:
                    role_text = role_mark_text(first.role)
                    await asyncio.sleep(2)
                    await safe_send_message(bot, chat_id, f"{first_mark} был {role_text}")
                await asyncio.sleep(2)
                try:
                    if first.role == ROLE_KAMIKAZE:
                        await safe_send_message(
                            bot,
                            first.user_id,
                            "Тебя линчевали на дневном собрании :(\nКого заберём с собой в могилу?",
                        )
                    else:
                        await safe_send_message(
                            bot,
                            first.user_id,
                            "тебя линчевали на дневном голосовании",
                        )
                except Exception:
                    pass
                if first.role == "Самоубийца":
                    await safe_send_message(bot, chat_id, "💀 <b>Самоубийца</b> выполнил личную цель победы.")
                if first.role == "Камикадзе" and len(eliminated) > 1:
                    second = eliminated[1]
                    second_mark = player_profile_link(second)
                    if reveal_roles:
                        second_role = role_mark_text(second.role)
                        await safe_send_message(bot, chat_id, f"💣 Камикадзе забрал с собой {second_mark} ({second_role}).")
                    else:
                        await safe_send_message(bot, chat_id, f"💣 Камикадзе забрал с собой {second_mark}.")
            else:
                await safe_send_message(
                    bot,
                    chat_id,
                    "Мнения жителей разошлись\n"
                    f"(<b>{yes_count}</b> 👍 | <b>{no_count}</b> 👎 )... Разошлись и сами жители, так никого и не повесив...",
                )

            if don_transfer_note:
                await announce_don_transfer(room, bot, don_successor_id)
            if commissar_transfer_note:
                await announce_commissar_transfer(room, bot, commissar_successor_id)

            if room.phase == PHASE_FINISHED:
                room.pending_last_words.clear()
                stats_already_recorded = room.stats_recorded
                ensure_stats_recorded(room)
                if not stats_already_recorded:
                    await send_endgame_currency_summaries(bot, room)
                await safe_send_message(bot, chat_id, room.final_report_text())
                cancel_phase_timer(chat_id)
                persist_room(room)
                return

            await asyncio.sleep(2)
            keyboard = await night_action_keyboard(bot)
            await send_phase_media(
                bot,
                chat_id,
                room.night_media_caption(),
                NIGHT_IMAGE_PATH,
                reply_markup=keyboard,
            )
            await safe_send_message(
                bot,
                chat_id,
                night_status_text(room),
                reply_markup=keyboard,
            )
            await push_phase_action_menus(bot, room)
            await push_kamikaze_revenge_menu(bot, room)
            await start_phase_timer(room, bot)
            persist_room(room)
            return

        print(f"[ERROR] process_day_end: unknown day_stage for chat_id={chat_id}, day_stage={room.day_stage}, phase={room.phase}")
        await bot.send_message(chat_id, "Не удалось определить текущий этап дня.")


async def phase_timer_worker(bot: Bot, chat_id: int, phase: str, duration_sec: int) -> None:
    try:
        await asyncio.sleep(duration_sec)
        room = storage.get_room(chat_id)
        if room is None or room.phase != phase:
            return

        if phase == "night":
            await process_night_end(bot, chat_id, timer_reason=None)
        elif phase == "day":
            await process_day_end(bot, chat_id, timer_reason=None)
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"[ERROR] phase_timer_worker: chat_id={chat_id}, phase={phase}, error={e!r}")


async def start_phase_timer(
    room,
    bot: Bot,
    remaining_sec: int | None = None,
    reset_deadline: bool = True,
) -> None:
    cancel_phase_timer(room.chat_id)

    settings = room_chat_settings(room)
    phase_duration = int(settings["timings"]["night"])
    if room.phase == "night":
        phase_duration = int(settings["timings"]["night"])
    elif room.phase == "day":
        phase_duration = current_day_stage_seconds(room)
    else:
        return

    if remaining_sec is None:
        remaining_sec = phase_duration
    if remaining_sec <= 0:
        remaining_sec = 1

    if reset_deadline:
        room.phase_started_at = datetime.now()
        room.phase_duration_seconds = phase_duration
    else:
        if room.phase_started_at is None:
            room.phase_started_at = datetime.now()
        if room.phase_duration_seconds is None:
            room.phase_duration_seconds = phase_duration

    phase_timers[room.chat_id] = asyncio.create_task(
        phase_timer_worker(bot, room.chat_id, room.phase, remaining_sec)
    )
    persist_room(room)


async def restore_runtime_state(bot: Bot) -> None:
    for room in storage.rooms.values():
        apply_room_settings(room, room.settings)
        if not room.started and room.registration_open:
            remaining = registration_remaining_seconds(room)
            if remaining <= 0:
                await process_registration_timeout(bot, room.chat_id)
            else:
                await start_registration_timer(room, bot, remaining)
            continue

        if not room.started or room.phase in {PHASE_FINISHED, "lobby"}:
            continue
        if room.phase == PHASE_DAY and room.day_stage is None:
            room.start_day_discussion()
            persist_room(room)

        base_duration = room.phase_duration_seconds
        if base_duration is None or base_duration <= 0:
            if room.phase == PHASE_NIGHT:
                base_duration = int(room_chat_settings(room)["timings"]["night"])
            else:
                base_duration = current_day_stage_seconds(room)

        remaining_sec: int | None = None
        if room.phase_started_at is not None:
            elapsed = int((datetime.now() - room.phase_started_at).total_seconds())
            remaining_sec = base_duration - elapsed

        if remaining_sec is not None and remaining_sec <= 0:
            if RESTART_EXPIRED_PHASE_POLICY == "restart":
                # Restart the current phase from a full timer after reboot instead of auto-closing it.
                remaining_sec = base_duration
                room.phase_started_at = datetime.now()
                room.phase_duration_seconds = base_duration
                persist_room(room)
                print(
                    f"[PHASE] restore_runtime_state: expired phase restarted for chat_id={room.chat_id}, "
                    f"phase={room.phase}, duration={remaining_sec}s"
                )
            else:
                if room.phase == PHASE_NIGHT:
                    await process_night_end(
                        bot,
                        room.chat_id,
                        timer_reason=None
                    )
                elif room.phase == PHASE_DAY:
                    await process_day_end(
                        bot,
                        room.chat_id,
                        timer_reason="⏱ Время дня истекло во время перезапуска. Фаза закрыта автоматически."
                    )
                continue

        await start_phase_timer(room, bot, remaining_sec=remaining_sec, reset_deadline=False)
        await push_phase_action_menus(bot, room)


async def maybe_finish_phase_early(bot: Bot, room) -> None:
    if room.phase == "night" and room.all_required_night_actions_done():
        await process_night_end(bot, room.chat_id, timer_reason=None)
        return

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION and room.all_alive_day_voted():
        await process_day_end(bot, room.chat_id, timer_reason=None)
        return

    if room.phase == "day" and room.day_stage == DAY_STAGE_TRIAL and room.all_alive_trial_voted():
        await process_day_end(bot, room.chat_id, timer_reason=None)


def mafia_allies_text(room) -> str:
    allies = [player for player in room.players.values() if player.role in {ROLE_DON, ROLE_MAFIA}]
    if not allies:
        return ""

    lines = ["", "<b>Запомни своих союзников:</b>"]
    for ally in allies:
        role_mark = role_mark_text(ally.role)
        lines.append(f"  {ally.full_name} - {role_mark}")
    return "\n".join(lines)


def city_power_allies_text(room, role: str) -> str:
    lines: list[str] = []
    if role == ROLE_SERGEANT:
        commissar = next((player for player in room.players.values() if player.role == ROLE_COMMISSAR), None)
        if commissar is not None:
            lines.append(f"     {commissar.full_name} - 🕵️‍ Комиссар Каттани")
    elif role == ROLE_COMMISSAR:
        sergeant = next((player for player in room.players.values() if player.role == ROLE_SERGEANT), None)
        if sergeant is not None:
            lines.append(f"     {sergeant.full_name} - 👮🏼‍♂️ Сержант")

    if not lines:
        return ""

    return "\n\n<b>Запомни своих союзников:</b>\n" + "\n".join(lines)


def role_card_for_player(room, player, chat_title: str) -> str:
    if player.role == ROLE_SERGEANT:
        base = (
            "<b>Ты - 👮🏼‍♂️ Сержант!</b>\n"
            "Помощник комиссара Каттани. Он будет информировать тебя о своих действиях "
            "и держать в курсе событий. Если комиссар погибнет - ты займёшь его место."
        )
        return base + city_power_allies_text(room, ROLE_SERGEANT)

    if player.role == ROLE_COMMISSAR:
        base = (
            "<b>Ты - 🕵️‍ Комиссар Каттани!</b>\n"
            "Главный городской защитник и гроза мафии..."
        )
        return base + city_power_allies_text(room, ROLE_COMMISSAR)

    card_text = role_card_text(player.role, chat_title)
    if player.role in {ROLE_DON, ROLE_MAFIA}:
        card_text += mafia_allies_text(room)
    return card_text


@router.message(CommandStart(), F.chat.type == "private")
async def cmd_start(message: Message, command: CommandObject) -> None:
    is_private_first_visit = False
    nickname = ""
    keyboard = private_main_menu_keyboard()
    if message.chat.type == "private":
        nickname = user_nickname(message.from_user)
        is_private_first_visit = repo.touch_private_user(message.from_user.id, nickname)

    if message.chat.type == "private" and command.args and command.args.startswith("join_"):
        if is_private_first_visit:
            await message.answer(
                (
                    f"<b>Привет, {nickname}! 👋</b>\n\n"
                    "Добро пожаловать в <b>Loft Mafia Bot</b> 🎭\n\n"
                    "Здесь ты будешь получать роль, делать ходы и смотреть свой игровой профиль."
                ),
                reply_markup=keyboard,
            )
        try:
            chat_id = int(command.args.split("_", maxsplit=1)[1])
        except ValueError:
            await message.answer("Некорректная ссылка приглашения.")
            return

        room = storage.get_room(chat_id)
        if room is None:
            await message.answer("Регистрация не найдена или уже завершена.")
            return

        if is_user_blocked(chat_id, message.from_user.id):
            await notify_registration_blocked(message.bot, chat_id, message.from_user.id)
            return

        if not room.registration_open or room.started:
            await message.answer("Регистрация уже закрыта.")
            return

        ok, info = room.add_player(message.from_user.id, nickname)
        if not ok:
            if info == "Ты уже в лобби.":
                persist_room(room)
                await refresh_registration_post(message, room)
            await message.answer(info)
            return
        persist_room(room)

        await message.answer(
            (
                f"Ты присоединился к игре в <b>{room.chat_title or room.chat_id}</b>."
            )
        )
        await refresh_registration_post(message, room)
        return

    if message.chat.type == "private":
        if is_private_first_visit:
            text = (
                f"<b>Привет, {nickname}! 👋</b>\n\n"
                "Добро пожаловать в <b>Loft Mafia Bot</b> 🎭\n\n"
                "Здесь ты будешь получать роль, делать ходы и смотреть свой игровой профиль.\n\n"
                "Чтобы начать игру, зайди в игровой чат и зарегистрируйся в лобби."
            )
        else:
            text = (
                f"<b>С возвращением, {nickname}!</b>\n\n"
                "Выбери нужный раздел кнопками ниже."
            )
        await message.answer(
            text,
            reply_markup=keyboard,
        )
        return


@router.message(F.chat.type.in_({"group", "supergroup"}), F.new_chat_members)
async def on_new_chat_members(message: Message) -> None:
    members = [member for member in (message.new_chat_members or []) if not member.is_bot]
    if not members:
        return
    for member in members:
        await send_group_welcome(message.bot, message.chat.id, member)


@router.chat_member(F.chat.type.in_({"group", "supergroup"}))
async def on_chat_member_joined(update: ChatMemberUpdated) -> None:
    user = update.new_chat_member.user
    if user.is_bot:
        return

    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status
    joined_statuses = {"member", "administrator", "creator", "restricted"}
    if new_status not in joined_statuses:
        return
    if old_status not in {"left", "kicked"}:
        return

    await send_group_welcome(update.bot, update.chat.id, user)

@router.message(Command("roles"))
async def cmd_roles(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type == "private":
        await message.answer("Выберите роль:", reply_markup=private_roles_keyboard())
        return
    await message.answer(all_roles_info_text())


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    await cleanup_group_command_message(message)
    stats = repo.get_player_stats(message.from_user.id)
    if stats is None:
        if message.chat.type == "private":
            await message.answer(
                "Пока нет сохраненной статистики. Сыграй хотя бы одну завершенную партию.",
                reply_markup=private_back_to_menu_keyboard(),
            )
            return
        await message.answer("Пока нет сохраненной статистики. Сыграй хотя бы одну завершенную партию.")
        return
    if message.chat.type == "private":
        await message.answer(format_player_stats_text(stats), reply_markup=private_back_to_menu_keyboard())
        return
    await message.answer(format_player_stats_text(stats))


@router.message(Command("profile"))
async def cmd_profile(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type != "private":
        await message.answer("Профиль доступен в ЛС бота.")
        return
    stats = repo.get_player_stats(message.from_user.id)
    await message.answer(
        format_private_profile_text(user_nickname(message.from_user), stats),
        reply_markup=private_profile_keyboard(),
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.from_user is None:
        return
    if message.chat.type == "private":
        await message.answer("Настройки вызываются из игрового чата.")
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
            "Какие параметры вы хотите изменить?",
            reply_markup=private_settings_main_keyboard(message.chat.id),
        )
    except Exception:
        await message.answer("Не смог отправить настройки в ЛС. Напиши боту /start в личку и попробуй снова.")
        return

    await message.answer("Настройки отправлены в ЛС бота.")


@router.message(Command("action"))
async def cmd_action(message: Message) -> None:
    await cleanup_group_command_message(message)
    await message.answer("Меню хода отправляется автоматически при старте каждой фазы.")


@router.message(Command("panel"))
async def cmd_panel(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type == "private":
        await message.answer("Панель доступна в групповом чате.")
        return

    sent = await message.answer("Панель регистрации:")
    registration_panel_message_ids[message.chat.id] = sent.message_id
    await message.bot.edit_message_reply_markup(
        chat_id=message.chat.id,
        message_id=sent.message_id,
        reply_markup=registration_panel(sent.message_id),
    )


@router.message(Command("game"))
async def cmd_create(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type == "private":
        await message.answer("Создавай лобби в групповом чате.")
        return

    if not await bot_has_delete_permission(message.bot, message.chat.id):
        await message.answer(
            "Не могу открыть регистрацию: дайте боту право администратора «Удалять сообщения»."
        )
        return

    chat_settings = load_chat_settings(message.chat.id)
    if bool(chat_settings.get("misc", {}).get("admin_game_only", False)):
        if not await is_group_admin(message.bot, message.chat.id, message.from_user.id):
            await message.answer("Запуск новой игры разрешён только администраторам.")
            return

    if is_user_blocked(message.chat.id, message.from_user.id):
        await notify_registration_blocked(message.bot, message.chat.id, message.from_user.id)
        await message.answer("Ты не можешь запустить регистрацию, пока действует мут. Проверь ЛС бота.")
        return

    room = storage.get_room(message.chat.id)
    if room is None:
        ok, info = storage.create_room(chat_id=message.chat.id, host_id=message.from_user.id)
        if not ok:
            await message.answer(info)
            return
        room = storage.get_room(message.chat.id)
    elif room.started and room.phase != PHASE_FINISHED:
        await message.answer("Игра уже идет. Заверши текущую игру перед новой регистрацией.")
        return

    if room is not None and room.registration_open:
        # If registration is already open, make sure timeout worker is actually running.
        existing_timer = registration_timers.get(message.chat.id)
        if existing_timer is None or existing_timer.done():
            remaining = registration_remaining_seconds(room)
            if remaining <= 0:
                await process_registration_timeout(message.bot, message.chat.id)
                return
            await start_registration_timer(room, message.bot, remaining)

        room.chat_title = message.chat.title or "Групповой чат"
        join_link = await registration_join_link(message, message.chat.id)
        if room.registration_message_id is None:
            sent = await message.answer(
                registration_post_text(room),
                reply_markup=registration_lobby_keyboard(join_link),
            )
            room.registration_message_id = sent.message_id
            persist_room(room)
            await pin_registration_post(message.bot, room)
        else:
            await refresh_registration_post(message, room)
            await pin_registration_post(message.bot, room)
        info_message = await message.answer(
            "Лобби уже создано. Используй существующее сообщение регистрации "
            "и кнопку регистрации под ним."
        )
        registration_notice_message_ids[message.chat.id] = info_message.message_id
        return

    if room is None:
        await message.answer("Не удалось создать лобби. Попробуй еще раз.")
        return

    room.chat_title = message.chat.title or "Групповой чат"
    apply_room_settings(room, load_chat_settings(message.chat.id))
    room.players.clear()
    room.open_registration()
    persist_room(room)
    join_link = await registration_join_link(message, message.chat.id)

    sent = await message.answer(
        registration_post_text(room),
        reply_markup=registration_lobby_keyboard(join_link),
    )
    room.registration_message_id = sent.message_id
    persist_room(room)
    await pin_registration_post(message.bot, room)
    await start_registration_timer(room, message.bot, int(room_chat_settings(room)["timings"]["registration"]))


@router.message(Command("join"))
async def cmd_join(message: Message) -> None:
    await cleanup_group_command_message(message)
    await message.answer("Вход в лобби только через inline-кнопку Зарегистрироваться под постом лобби.")


@router.message(Command("leave"))
async def cmd_leave(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        return

    if not room.started or room.phase == PHASE_FINISHED:
        return

    player = room.get_player(message.from_user.id)
    if player is None:
        await message.answer("Тебя нет в этой игре.")
        return

    if room.started and room.phase != PHASE_FINISHED:
        if not player.alive:
            await message.answer("Ты уже выбыл из игры.")
            return

        player.alive = False
        room.check_winner()
        persist_room(room)

        leaver_mark = player_profile_link(player)
        leave_text = f"{leaver_mark} не выдержал гнетущей атмосферы этого города и повесился."
        if show_roles_enabled(room):
            leave_text += f"\nОн был {role_mark_text(player.role)}"
        await message.answer(leave_text)

        try:
            await message.bot.send_message(player.user_id, "Ты вышел из игры", **private_game_send_kwargs(room))
        except Exception:
            pass

        if room.phase == PHASE_FINISHED:
            stats_already_recorded = room.stats_recorded
            ensure_stats_recorded(room)
            if not stats_already_recorded:
                await send_endgame_currency_summaries(message.bot, room)
            await message.answer(room.final_report_text())
            cancel_phase_timer(message.chat.id)

        persist_room(room)
        return

@router.message(Command("lobby"))
async def cmd_lobby(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await message.answer(room.lobby_text())


@router.message(Command("extend"))
async def cmd_extend(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return
    if room.started or not room.registration_open:
        await message.answer("Регистрация уже закрыта.")
        return

    room.extend_registration()
    remaining = registration_remaining_seconds(room)
    new_seconds = remaining + REGISTRATION_EXTENSION_SECONDS
    await start_registration_timer(room, message.bot, new_seconds)
    persist_room(room)
    join_link = await registration_join_link(message, message.chat.id)
    await upsert_registration_warning_message(
        message.bot,
        message.chat.id,
        f"Регистрация продлена на {REGISTRATION_EXTENSION_SECONDS} сек. "
        f"Осталось {new_seconds} сек. Продлений: {room.registration_extensions}.",
        reply_markup=registration_lobby_keyboard(join_link),
    )
    await refresh_registration_post(message, room)


@router.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type == "private":
        await message.answer("Останавливать игру нужно в групповом чате.")
        return

    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    if room.registration_open and not room.started:
        room.close_registration()
        persist_room(room)
        await clear_registration_post(message.bot, room)
        await clear_registration_panel_message(message.bot, message.chat.id)
        await clear_registration_notice_message(message.bot, message.chat.id)
        await clear_registration_warning_message(message.bot, message.chat.id)
        cancel_phase_timer(message.chat.id)
        cancel_registration_timer(message.chat.id)
        clear_chat_penalties(message.chat.id)
        clear_action_menu_messages(message.chat.id)
        remove_room_state(message.chat.id)
        storage.close_room(message.chat.id)
        await message.answer("Регистрация отменена, лобби удалено.")
        return

    cancel_phase_timer(message.chat.id)
    cancel_registration_timer(message.chat.id)
    clear_chat_penalties(message.chat.id)
    clear_action_menu_messages(message.chat.id)
    await clear_registration_post(message.bot, room)
    await clear_registration_panel_message(message.bot, message.chat.id)
    await clear_registration_notice_message(message.bot, message.chat.id)
    await clear_registration_warning_message(message.bot, message.chat.id)
    remove_room_state(message.chat.id)
    storage.close_room(message.chat.id)
    await message.answer("Игра остановлена.")


@router.message(Command("start"))
async def cmd_begin(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    if not room.registration_open:
        await message.answer("Регистрация уже закрыта.")
        return

    if len(room.players) < MIN_PLAYERS:
        await message.answer(f"Нужно минимум {MIN_PLAYERS} игрока(ов).")
        return

    await launch_game_from_registration(message.bot, room, message.chat.id, message.chat.title)


@router.callback_query(F.data.startswith("reg:"))
async def on_registration_action(callback: CallbackQuery) -> None:
    if callback.message is None or callback.from_user is None:
        return

    chat_id = callback.message.chat.id
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    room = storage.get_room(chat_id)

    if action == "start":
        if len(parts) != 3:
            await callback.answer()
            return
        try:
            panel_message_id = int(parts[2])
        except ValueError:
            await callback.answer()
            return
        if registration_panel_message_ids.get(chat_id) != panel_message_id:
            await callback.answer()
            return

        chat_settings = load_chat_settings(chat_id)
        if bool(chat_settings.get("misc", {}).get("admin_game_only", False)):
            if not await is_group_admin(callback.bot, chat_id, callback.from_user.id):
                await callback.answer("Запуск новой игры разрешён только администраторам.", show_alert=True)
                return

        if is_user_blocked(chat_id, callback.from_user.id):
            await notify_registration_blocked(callback.bot, chat_id, callback.from_user.id)
            await callback.answer("Пока действует мут, создание лобби недоступно.", show_alert=True)
            return

        if not await bot_has_delete_permission(callback.bot, chat_id):
            await callback.answer(
                "Не могу открыть регистрацию: дайте боту право администратора «Удалять сообщения».",
                show_alert=True,
            )
            return

        if room is None:
            ok, info = storage.create_room(chat_id=chat_id, host_id=callback.from_user.id)
            if not ok:
                await callback.answer(info, show_alert=True)
                return
            room = storage.get_room(chat_id)

        room.chat_title = callback.message.chat.title or "Групповой чат"
        apply_room_settings(room, load_chat_settings(chat_id))
        room.open_registration()
        persist_room(room)
        await start_registration_timer(room, callback.bot, int(room_chat_settings(room)["timings"]["registration"]))
        join_link = await registration_join_link(callback.message, chat_id)
        sent = await callback.message.answer(
            registration_post_text(room),
            reply_markup=registration_lobby_keyboard(join_link),
        )
        room.registration_message_id = sent.message_id
        persist_room(room)
        await pin_registration_post(callback.bot, room)
        info_message = await callback.message.answer("Лобби создано. Игроки могут входить через кнопку ниже")
        registration_notice_message_ids[chat_id] = info_message.message_id
        await callback.answer("Готово")
        return

    if action == "join":
        if not repo.has_private_user(callback.from_user.id):
            await callback.answer(
                "Сначала напиши боту в личку /start, затем вернись и нажми регистрацию еще раз.",
                show_alert=True,
            )
            return
        if is_user_blocked(chat_id, callback.from_user.id):
            await notify_registration_blocked(callback.bot, chat_id, callback.from_user.id)
            await callback.answer("Пока действует мут, регистрация недоступна.", show_alert=True)
            return
        if room is None:
            await callback.answer("Сначала создай лобби.", show_alert=True)
            return
        if not room.registration_open or room.started:
            await callback.answer("Регистрация закрыта.", show_alert=True)
            return

        ok, info = room.add_player(callback.from_user.id, user_nickname(callback.from_user))
        if not ok:
            if info == "Ты уже в лобби.":
                persist_room(room)
                await refresh_registration_post(callback.message, room)
            await callback.answer(info, show_alert=True)
            return
        persist_room(room)
        await refresh_registration_post(callback.message, room)
        await callback.answer("Ты зарегистрирован")
        return

    if action == "leave":
        if room is None:
            await callback.answer("Лобби не найдено.", show_alert=True)
            return

        ok, info = room.remove_player(callback.from_user.id)
        if not ok:
            await callback.answer(info, show_alert=True)
            return
        persist_room(room)

        if room.players:
            await refresh_registration_post(callback.message, room)
            await callback.answer("Ты вышел из лобби")
        else:
            await clear_registration_post(callback.bot, room)
            await clear_registration_panel_message(callback.bot, chat_id)
            await clear_registration_notice_message(callback.bot, chat_id)
            cancel_phase_timer(chat_id)
            cancel_registration_timer(chat_id)
            clear_chat_penalties(chat_id)
            clear_action_menu_messages(chat_id)
            remove_room_state(chat_id)
            storage.close_room(chat_id)
            await callback.message.answer("Лобби пустое и закрыто.")
            await callback.answer("Лобби закрыто")
        return

    if room is None:
        await callback.answer("Сначала запусти регистрацию.", show_alert=True)
        return

    if action == "finish_start":
        if room.started:
            await callback.answer("Игра уже началась.", show_alert=True)
            return
        if len(room.players) < MIN_PLAYERS:
            await callback.answer(f"Нужно минимум {MIN_PLAYERS} игрока(ов).", show_alert=True)
            return

        await launch_game_from_registration(callback.bot, room, chat_id, callback.message.chat.title)
        await callback.answer("Игра начата")
        return

    if action == "finish_cancel":
        if room.started:
            await callback.answer("Игра уже началась.", show_alert=True)
            return
        if room.registration_open:
            room.close_registration()
            persist_room(room)
        await clear_registration_post(callback.bot, room)
        cancel_phase_timer(chat_id)
        cancel_registration_timer(chat_id)
        clear_chat_penalties(chat_id)
        clear_action_menu_messages(chat_id)
        remove_room_state(chat_id)
        storage.close_room(chat_id)
        await callback.message.answer("Регистрация завершена. Игра отменена.")
        await callback.answer("Игра отменена")
        return

    if action == "cancel":
        if room.registration_open:
            room.close_registration()
            persist_room(room)
        await clear_registration_post(callback.bot, room)
        cancel_phase_timer(chat_id)
        cancel_registration_timer(chat_id)
        clear_chat_penalties(chat_id)
        clear_action_menu_messages(chat_id)
        remove_room_state(chat_id)
        storage.close_room(chat_id)
        await callback.message.answer("Регистрация отменена, лобби удалено.")
        await callback.answer("Отменено")
        return

    await callback.answer("Неизвестное действие", show_alert=True)


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await message.answer(room.status_text())


@router.message(Command("id"))
async def cmd_id(message: Message) -> None:
    await cleanup_group_command_message(message)

    replied = message.reply_to_message
    if replied is not None and replied.from_user is not None:
        name = escape(user_nickname(replied.from_user))
        await message.answer(f"ID {name}: <code>{replied.from_user.id}</code>")
        return

    if message.from_user is not None:
        await message.answer(f"Твой ID: <code>{message.from_user.id}</code>")
        return

    await message.answer("Не удалось определить ID.")


@router.callback_query(F.data.startswith("trial:"))
async def on_trial_callback(callback: CallbackQuery) -> None:
    try:
        if callback.message is None or callback.from_user is None:
            return

        parts = callback.data.split(":")
        if len(parts) != 3:
            await callback.answer("Некорректное голосование.", show_alert=True)
            return

        _, raw_vote, raw_chat_id = parts
        if raw_vote not in {"yes", "no"}:
            await callback.answer("Некорректный вариант голоса.", show_alert=True)
            return

        try:
            chat_id = int(raw_chat_id)
        except ValueError:
            await callback.answer("Некорректный чат.", show_alert=True)
            return

        room = storage.get_room(chat_id)
        if room is None or room.phase != PHASE_DAY or room.day_stage != DAY_STAGE_TRIAL:
            await callback.answer("Сейчас нет активного голосования за/против.", show_alert=True)
            return

        if callback.message.chat.id != room.chat_id:
            await callback.answer("Голосование проходит в групповом чате.", show_alert=True)
            return

        voter = room.get_player(callback.from_user.id)
        if voter is None or not voter.alive:
            await callback.answer("Ты не в игре.")
            return

        approve = raw_vote == "yes"
        ok, info = room.set_trial_vote(callback.from_user.id, approve)
        if not ok:
            if info == "Пока все голосуют - ты лечишься. 💃🏼 Любовница постаралась...":
                await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
                return
            await callback.answer(info, show_alert=True)
            return
        await callback.answer(info)
        persist_room(room)

        # Update shared group vote message after vote.
        yes_count, no_count = room.trial_vote_counts()
        candidate = room.get_player(room.trial_candidate_id) if room.trial_candidate_id is not None else None
        try:
            await callback.message.edit_text(
                trial_vote_prompt_text(room, candidate),
                reply_markup=trial_vote_keyboard(chat_id, yes_count, no_count),
            )
        except Exception:
            try:
                await callback.message.edit_reply_markup(reply_markup=trial_vote_keyboard(chat_id, yes_count, no_count))
            except Exception:
                pass

        # If all eligible voters have voted, proceed to next phase.
        candidate_id = room.trial_candidate_id
        eligible_voter_ids = {
            player.user_id
            for player in room.alive_players()
            if candidate_id is None or player.user_id != candidate_id
        }
        received_vote_ids = {user_id for user_id in room.trial_votes if user_id in eligible_voter_ids}
        trial_complete = received_vote_ids == eligible_voter_ids

        if room.all_alive_trial_voted() or trial_complete:
            print(
                "[TRIAL] voting_complete "
                f"chat_id={room.chat_id}, eligible={len(eligible_voter_ids)}, received={len(received_vote_ids)}, "
                f"yes={yes_count}, no={no_count}"
            )
            completion_text = f"{trial_vote_prompt_text(room, candidate)}\n\nГолосование завершено"
            try:
                await callback.message.edit_text(completion_text)
            except Exception:
                try:
                    await callback.message.edit_reply_markup(reply_markup=None)
                except Exception:
                    pass
            await process_day_end(callback.bot, room.chat_id, timer_reason=None)
        else:
            await maybe_finish_phase_early(callback.bot, room)
    except Exception as e:
        print(f"[ERROR] on_trial_callback: chat_id={getattr(getattr(callback, 'message', None), 'chat', None) and callback.message.chat.id}, error={e!r}")
        print(traceback.format_exc())
        try:
            await callback.answer("Произошла ошибка при голосовании. Попробуй еще раз.", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("act:"))
async def on_action_callback(callback: CallbackQuery) -> None:
    if callback.message is None or callback.from_user is None:
        return

    if callback.message.chat.type != "private":
        await callback.answer("Игровые действия доступны только в ЛС бота.", show_alert=True)
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer("Некорректное действие.", show_alert=True)
        return

    _, action, raw_chat_id, raw_target_id = parts
    try:
        chat_id = int(raw_chat_id)
        target_id = int(raw_target_id)
    except ValueError:
        await callback.answer("Некорректные параметры.", show_alert=True)
        return

    room = storage.get_room(chat_id)
    if room is None:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    actor = room.get_player(callback.from_user.id)
    if actor is None:
        await callback.answer("Ты не в игре.")
        return
    if not actor.alive:
        is_kamikaze_revenge = (
            room.phase == PHASE_NIGHT
            and actor.role == ROLE_KAMIKAZE
            and room.kamikaze_pending_user_id == actor.user_id
        )
        if not is_kamikaze_revenge:
            await callback.answer("Ты не в игре.")
            return

    if room.phase == PHASE_NIGHT and callback.from_user.id in getattr(room, "night_skipped_user_ids", set()):
        await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
        return

    async def announce_night_role_once(role_name: str, target=None, *, variant: str = "default") -> None:
        if room.phase != "night":
            return
        if not room.mark_night_role_announced(role_name):
            return
        announcement_text = night_role_announcement_text(room, role_name, target, variant=variant)
        await safe_send_message(callback.bot, room.chat_id, announcement_text, parse_mode="HTML")

    if action == "kill":
        if room.night_votes.get(callback.from_user.id) is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_night_vote(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            actor = room.get_player(callback.from_user.id)
            target = room.get_player(target_id)
            if actor is not None and target is not None:
                role_mark = role_mark_text(actor.role)
                await send_mafia_private_update(
                    room,
                    callback.bot,
                    f"{role_mark} {actor.full_name} проголосовал за {target.full_name}",
                )

            if room.mafia_vote_locked:
                final_target_id = room.current_mafia_target_id()
                final_target = room.get_player(final_target_id) if final_target_id is not None else None
                final_name = final_target.full_name if final_target is not None else "цель"
                await send_mafia_private_update(
                    room,
                    callback.bot,
                    "Голосование мафии завершено\n"
                    f"Мафия принесла в жертву {final_name}.",
                )
                if final_target is not None and not room.mafia_target_announced:
                    await safe_send_message(
                        callback.bot,
                        room.chat_id,
                        night_role_announcement_text(room, ROLE_DON, final_target),
                        parse_mode="HTML",
                    )
                    room.mafia_target_announced = True
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "heal":
        if room.doctor_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_doctor_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "commode":
        if not commissar_can_shoot_enabled(room):
            await callback.answer("Стрельба Комиссара отключена в настройках.", show_alert=True)
            return
        if not commissar_can_shoot_this_night(room):
            await callback.answer("Стрелять в эту ночь нельзя по настройкам.", show_alert=True)
            return
        if room.commissar_target_id is not None or room.commissar_shot_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return

        mode = "check" if target_id == 1 else "shoot" if target_id == 2 else None
        if mode is None:
            await callback.answer("Некорректный выбор действия.", show_alert=True)
            return
        ok, info = room.set_commissar_action_mode(callback.from_user.id, mode)
        await callback.answer(info, show_alert=not ok)
        if ok:
            if mode == "shoot":
                await callback.bot.send_message(room.chat_id, "🕵️‍ Комиссар Каттани уже зарядил свой пистолет...")
            await refresh_private_action_message(callback, room, callback.from_user.id)
            persist_room(room)
        return

    if action == "check":
        if room.commissar_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.check_player_role(callback.from_user.id, target_id)
        await callback.answer("Проверка принята." if ok else info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "cshot":
        if room.commissar_shot_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_commissar_shot_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target, variant="shoot")
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "advocate":
        if room.advocate_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_advocate_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "vote":
        if room.phase == PHASE_DAY and room.day_stage == DAY_STAGE_TRIAL:
            await callback.answer("Сейчас идет повешение: можно только поставить 👍 или 👎 в чате.", show_alert=True)
            return
        if callback.from_user.id in room.day_votes:
            await callback.answer("Выбор уже зафиксирован до конца голосования.", show_alert=True)
            return

        ok, info = room.set_day_vote(callback.from_user.id, target_id)
        if ok:
            await callback.answer(info)
            target = room.get_player(target_id)
            voter = room.get_player(callback.from_user.id)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "кандидата"
                selected_user_id = None

            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )

            if voter is not None and target is not None and not is_secret_voting_enabled(room):
                await callback.bot.send_message(
                    room.chat_id,
                    f"{player_profile_link(voter)} проголосовал за {player_profile_link(target)}",
                )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        else:
            if info == "Пока все голосуют - ты лечишься. 💃🏼 Любовница постаралась...":
                await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
                return
            await callback.answer(info, show_alert=True)
        return

    if action == "skipvote":
        if room.phase == PHASE_DAY and room.day_stage == DAY_STAGE_TRIAL:
            await callback.answer("Сейчас идет повешение: можно только поставить 👍 или 👎 в чате.", show_alert=True)
            return
        if room.phase != PHASE_DAY or room.day_stage != DAY_STAGE_NOMINATION:
            await callback.answer("Сейчас не этап выбора кандидата.", show_alert=True)
            return
        if not day_vote_skip_enabled(room):
            await callback.answer("Пропуск дневного голосования отключен в настройках.", show_alert=True)
            return

        if callback.from_user.id in room.day_votes:
            await callback.answer("Выбор уже зафиксирован до конца голосования.", show_alert=True)
            return

        voter = room.get_player(callback.from_user.id)
        if voter is None or not voter.alive:
            await callback.answer("Ты не в игре.")
            return
        if room.day_silenced_user_id is not None and voter.user_id == room.day_silenced_user_id:
            await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
            return

        room.day_votes[callback.from_user.id] = 0
        await callback.answer("Пропуск голосования принят.")
        await callback.message.edit_text(
            build_action_prompt_text(room, callback.from_user.id) + "\n\nТы выбрал пропуск",
        )
        if not is_secret_voting_enabled(room):
            await callback.bot.send_message(
                room.chat_id,
                f"{player_profile_link(voter)} пропускает голосование.",
            )

        await maybe_finish_phase_early(callback.bot, room)
        persist_room(room)
        return

    if action == "maniac":
        if room.maniac_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_maniac_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "mistress":
        if room.mistress_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_mistress_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "bum":
        if room.bum_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_bum_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "kamikaze":
        if room.kamikaze_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_kamikaze_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role, target)
            if target is not None:
                selected_name = player_display_name(target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    await callback.answer("Неизвестный тип действия.", show_alert=True)


@router.callback_query(F.data.startswith("noop:"))
async def on_noop_callback(callback: CallbackQuery) -> None:
    if callback.data == "noop:actionhint":
        await callback.answer("Открой ЛС бота: меню хода придет автоматически по фазе.", show_alert=True)
        return
    if callback.data == "noop:locked":
        await callback.answer("Выбор уже зафиксирован.", show_alert=True)
        return
    if callback.data.startswith("noop:skip:"):
        if callback.from_user is None or callback.message is None:
            return
        try:
            chat_id = int(callback.data.split(":", 2)[2])
        except (IndexError, ValueError):
            await callback.answer("Некорректный пропуск хода.", show_alert=True)
            return
        room = storage.get_room(chat_id)
        if room is None or room.phase != PHASE_NIGHT:
            await callback.answer("Сейчас нельзя пропустить ход.", show_alert=True)
            return
        if callback.message.chat.type != "private":
            await callback.answer("Пропуск ночного хода доступен только в ЛС бота.", show_alert=True)
            return
        if not night_action_skip_enabled(room):
            await callback.answer("Пропуск ночного хода отключен в настройках.", show_alert=True)
            return
        ok, info = room.set_night_skip(callback.from_user.id)
        await callback.answer(info, show_alert=not ok)
        if not ok:
            return
        try:
            await callback.message.edit_text(
                build_action_prompt_text(room, callback.from_user.id) + "\n\nТы выбрал пропуск",
            )
        except Exception:
            pass
        persist_room(room)
        await maybe_finish_phase_early(callback.bot, room)
        return
    if callback.data == "noop:silenced":
        await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
        return
    await callback.answer("Вы пропустили ход.", show_alert=True)


@router.callback_query(F.data.startswith("pmenu:"))
async def on_private_menu_callback(callback: CallbackQuery) -> None:
    async def safe_answer(text: str | None = None, show_alert: bool = False) -> None:
        try:
            if text is None:
                await callback.answer()
            else:
                await callback.answer(text, show_alert=show_alert)
        except TelegramBadRequest as e:
            # Ignore stale callback query errors after long delays/restarts.
            error_text = str(e)
            if "query is too old" in error_text or "query ID is invalid" in error_text:
                return
            raise

    if callback.from_user is None:
        return
    if callback.message is None or callback.message.chat.type != "private":
        await safe_answer("Это меню работает только в ЛС бота.", show_alert=True)
        return

    action = callback.data.split(":", maxsplit=1)[1]

    async def show_menu_screen(text: str, keyboard: InlineKeyboardMarkup) -> None:
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await callback.message.answer(text, reply_markup=keyboard)

    if action == "main":
        nickname = user_nickname(callback.from_user)
        await show_menu_screen(
            (
                f"<b>С возвращением, {nickname}!</b>\n\n"
                "Выбери нужный раздел кнопками ниже."
            ),
            private_main_menu_keyboard(),
        )
        await safe_answer()
        return

    if action == "roles":
        await show_menu_screen("Выберите роль:", private_roles_keyboard())
        await safe_answer()
        return
    if action.startswith("role:"):
        raw_idx = action.split(":", maxsplit=1)[1]
        try:
            idx = int(raw_idx)
        except ValueError:
            await safe_answer("Некорректная роль.", show_alert=True)
            return
        if idx < 0 or idx >= len(PRIVATE_ROLE_ORDER):
            await safe_answer("Роль не найдена.", show_alert=True)
            return
        role = PRIVATE_ROLE_ORDER[idx]
        await show_menu_screen(private_role_details_text(role), private_back_to_roles_keyboard())
        await safe_answer()
        return
    if action == "stats":
        stats = repo.get_player_stats(callback.from_user.id)
        if stats is None:
            await show_menu_screen(
                "Пока нет сохраненной статистики. Сыграй хотя бы одну завершенную партию.",
                private_back_to_menu_keyboard(),
            )
        else:
            await show_menu_screen(format_player_stats_text(stats), private_back_to_menu_keyboard())
        await safe_answer()
        return
    if action == "profile":
        stats = repo.get_player_stats(callback.from_user.id)
        await show_menu_screen(
            format_private_profile_text(user_nickname(callback.from_user), stats),
            private_profile_keyboard(),
        )
        await safe_answer()
        return
    if action == "buffs":
        await show_menu_screen(format_buffs_shop_text(), private_buffs_shop_keyboard())
        await safe_answer()
        return
    if action.startswith("buff:"):
        key = action.split(":", maxsplit=1)[1]
        if key not in BUFF_CATALOG:
            await safe_answer("Неизвестный баф.", show_alert=True)
            return
        stats = repo.get_player_stats(callback.from_user.id)
        await show_menu_screen(format_buff_details_text(key, stats), private_buff_details_keyboard(key))
        await safe_answer()
        return
    if action.startswith("buy:"):
        key = action.split(":", maxsplit=1)[1]
        item = BUFF_CATALOG.get(key)
        if item is None:
            await safe_answer("Неизвестный баф.", show_alert=True)
            return
        currency_column = str(item["currency"])
        currency_label = "денег" if currency_column == "money" else "билетиков"
        ok, info, stats = repo.purchase_buff(
            callback.from_user.id,
            user_nickname(callback.from_user),
            inventory_column=str(item["inventory_key"]),
            currency_column=currency_column,
            price=int(item["price_value"]),
            currency_label=currency_label,
        )
        if ok:
            await show_menu_screen(
                format_private_profile_text(user_nickname(callback.from_user), stats),
                private_profile_keyboard(),
            )
            await safe_answer(f"Приобретено: {item['success_name']}")
            return
        await safe_answer(info, show_alert=True)
        return
    await safe_answer("Неизвестный пункт меню.", show_alert=True)


@router.callback_query(F.data.startswith("psettings:"))
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
        try:
            await callback.message.edit_text(text, reply_markup=keyboard)
        except Exception:
            await callback.message.answer(text, reply_markup=keyboard)

    parts = callback.data.split(":")
    if len(parts) < 3:
        await safe_answer("Неизвестный пункт настроек.", show_alert=True)
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        await safe_answer("Некорректный чат настроек.", show_alert=True)
        return
    action = parts[2]
    settings = load_chat_settings(chat_id)

    if action == "main":
        await show_settings_screen("Какие параметры вы хотите изменить?", private_settings_main_keyboard(chat_id))
        await safe_answer()
        return

    if action == "close":
        nickname = user_nickname(callback.from_user)
        await show_settings_screen(
            f"<b>С возвращением, {nickname}!</b>\n\nВыбери нужный раздел кнопками ниже.",
            private_main_menu_keyboard(),
        )
        await safe_answer()
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


@router.message(F.chat.type == "private", F.text)
async def on_private_text(message: Message) -> None:
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    last_word_room = get_pending_last_word_room(message.from_user.id)
    if last_word_room is not None:
        if last_word_room.phase == PHASE_FINISHED:
            if message.from_user.id in last_word_room.pending_last_words:
                last_word_room.pending_last_words.discard(message.from_user.id)
                persist_room(last_word_room)
            await message.answer("Игра закончилась. Предсмертное сообщение больше нельзя отправить.")
            return

        ok, payload = last_word_room.consume_last_word(message.from_user.id, text)
        if not ok:
            await message.answer(payload)
            return
        persist_room(last_word_room)

        player = last_word_room.get_player(message.from_user.id)
        raw_name = player.full_name if player is not None else f"Игрок {message.from_user.id}"
        safe_name = escape((raw_name or "").strip() or f"Игрок {message.from_user.id}")
        player_mark = f"<a href=\"tg://user?id={message.from_user.id}\">{safe_name}</a>"
        safe_payload = escape(payload)
        await message.answer("Предсмертное сообщение принято.", **private_game_send_kwargs(last_word_room))
        await message.bot.send_message(
            last_word_room.chat_id,
            f"Кто-то из жителей слышал, как {player_mark} кричал перед смертью:\n<b>{safe_payload}</b>",
        )
        return

    room = get_private_action_room(message.from_user.id)
    if room is None:
        return

    actor = room.get_player(message.from_user.id)
    if actor is None or not actor.alive:
        return

    if room.phase != "night":
        return

    teammate_roles: set[str] | None = None
    if actor.role in {ROLE_DON, ROLE_MAFIA}:
        teammate_roles = {ROLE_DON, ROLE_MAFIA}
    elif actor.role in {ROLE_COMMISSAR, ROLE_SERGEANT}:
        teammate_roles = {ROLE_COMMISSAR, ROLE_SERGEANT}

    if teammate_roles is None:
        return

    relay_author = player_display_name(actor)
    relay_text = f"{relay_author}:\n{text}"
    for teammate in room.alive_players():
        if teammate.role not in teammate_roles:
            continue
        if teammate.user_id == actor.user_id:
            continue
        try:
            await message.bot.send_message(teammate.user_id, relay_text, **private_game_send_kwargs(room))
        except Exception:
            continue


@router.message(
    F.chat.type.in_({"group", "supergroup"}),
    F.from_user.id == OWNER_USER_ID,
    F.text.regexp(r"(?i)^\s*бот\s+выйд[иmм]\s*$"),
)
async def on_owner_exit_phrase(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is not None:
        await clear_registration_post(message.bot, room)
        cancel_phase_timer(message.chat.id)
        cancel_registration_timer(message.chat.id)
        clear_chat_penalties(message.chat.id)
        clear_action_menu_messages(message.chat.id)
        remove_room_state(message.chat.id)
        storage.close_room(message.chat.id)

    await message.reply("Слушаюсь, выходим.")
    try:
        await message.bot.leave_chat(message.chat.id)
    except Exception:
        pass


@router.message(
    F.chat.type.in_({"group", "supergroup"}),
    F.text.regexp(r"(?i)\b5658493362\b"),
)
async def on_developer_phrase(message: Message) -> None:
    display_name = "пользователь"
    try:
        owner_chat = await message.bot.get_chat(OWNER_USER_ID)
        display_name = escape((owner_chat.full_name or "").strip() or display_name)
    except Exception:
        pass

    dev_link = f"<a href=\"tg://user?id={OWNER_USER_ID}\">{display_name}</a>"
    await message.reply(f"Это {dev_link}")


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def enforce_group_game_rules(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None or not room.started or room.phase == PHASE_FINISHED:
        return

    if message.from_user is None or message.from_user.is_bot:
        return

    if message.text and message.text.startswith("!"):
        if await is_group_admin(message.bot, message.chat.id, message.from_user.id):
            return

    if is_user_blocked(message.chat.id, message.from_user.id):
        await safe_delete_message(message)
        await process_rule_violation(message)
        return

    sender = room.get_player(message.from_user.id)
    is_participant = sender is not None
    is_alive_player = sender is not None and sender.alive
    is_command = bool(message.text and message.text.startswith("/"))
    misc_settings = room_chat_settings(room).get("misc", {})
    media_delete_enabled = bool(misc_settings.get("delete_media", False))
    has_forbidden_media = media_delete_enabled and any(
        [
            bool(message.photo),
            message.video is not None,
            message.animation is not None,
            message.audio is not None,
            message.document is not None,
            message.voice is not None,
            message.video_note is not None,
        ]
    )
    has_forbidden_attachment = any(
        [
            message.poll is not None,
            message.location is not None,
            message.contact is not None,
            message.dice is not None,
            message.game is not None,
        ]
    )

    if has_forbidden_media or has_forbidden_attachment:
        await safe_delete_message(message)
        await process_rule_violation(message)
        return

    if room.phase == PHASE_NIGHT:
        mute_settings = room_chat_settings(room)["mute"]
        if is_participant and not is_alive_player:
            if bool(mute_settings["dead"]):
                await safe_delete_message(message)
                await process_rule_violation(message)
            return

        # At night, participants are muted without penalties to avoid blocking gameplay.
        if is_participant:
            if bool(mute_settings["sleeping"]) and not (is_alive_player and is_command):
                await safe_delete_message(message)
            return

        # Non-participants still receive regular penalties.
        if bool(mute_settings["outsiders"]) and not (is_alive_player and is_command):
            await safe_delete_message(message)
            await process_rule_violation(message)
        return

    if room.phase == PHASE_DAY:
        mute_settings = room_chat_settings(room)["mute"]
        # Silenced by mistress effect: messages are deleted for this day without penalty escalation.
        if room.day_silenced_user_id is not None and message.from_user.id == room.day_silenced_user_id:
            await safe_delete_message(message)
            return

        if is_participant and not is_alive_player:
            if bool(mute_settings["dead"]):
                await safe_delete_message(message)
                await process_rule_violation(message)
            return

        if not is_participant:
            if bool(mute_settings["outsiders"]):
                await safe_delete_message(message)
                await process_rule_violation(message)
            return

        # At day, only alive players can speak in the group chat.
        if not is_alive_player and bool(mute_settings["dead"]):
            await safe_delete_message(message)
            await process_rule_violation(message)
        return


