# Shared runtime context extracted from handlers.py.
import asyncio
import os
import random
import time
import traceback
import unicodedata
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
    GAME_MODE_CLASSIC,
    GAME_MODE_INVISIBLE,
    GAME_MODE_TITLES,
    MAFIA_ROLES,
    MAX_PLAYERS,
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
    game_mode_from_settings,
    invisible_mode_from_settings,
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
MISTRESS_DAY_BLOCK_TOAST = "Пока все голосуют - ты лечишься. 💃🏼 Любовница постаралась..."
DAY_SILENCED_VOTE_TEXT = "Пока все голосуют - ты лечишься. 💃🏼 Любовница постаралась..."
MSG_ADMIN_REQUIRED = "Недоступно: нужны права администратора с правом «Изменение информации группы»."
MSG_LOBBY_NOT_FOUND = "Лобби не найдено. Создай новое лобби через /game."
MSG_REGISTRATION_CLOSED = "Регистрация уже закрыта. Дождись следующего набора."
MSG_GAME_ALREADY_RUNNING = "Игра уже идет. Дождись завершения текущей партии."
MSG_REGISTRATION_CANCELLED_ADMIN = "Регистрация отменена администратором."
MSG_GAME_CANCELLED_ADMIN = "Игра отменена администратором."
MSG_GAME_STOPPED_ADMIN = "Игра остановлена администратором."
MSG_NOT_IN_GAME = "Недоступно: ты не участвуешь в текущей игре."
MSG_INVALID_ACTION = "Недоступно: некорректное действие."
MUTE_DEAD_PLAYERS = True
MUTE_SLEEPING_PLAYERS = True
MUTE_NON_PLAYERS = True
LEAVE_RESTRICTION_SECONDS = 0


def read_user_id_set(name: str) -> set[int]:
    raw = os.getenv(name, "")
    values: set[int] = set()
    for item in raw.split(","):
        candidate = item.strip()
        if not candidate:
            continue
        try:
            values.add(int(candidate))
        except ValueError:
            continue
    return values


TICKET_MANAGER_USER_IDS = {OWNER_USER_ID, 7272018388, *read_user_id_set("TICKET_MANAGER_USER_IDS")}

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
SETTINGS_GAME_MODE_TEXT = (
    "Выберите игровой режим.\n"
    "В режиме \"Невидимка\" бот включает фиксированную анонимность и блокирует изменение остальных настроек, пока вы не вернётесь в \"Классику\"."
)
