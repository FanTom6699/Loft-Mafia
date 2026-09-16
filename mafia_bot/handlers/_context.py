# Shared runtime context for split handlers. Generated from the original handlers.py.
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

