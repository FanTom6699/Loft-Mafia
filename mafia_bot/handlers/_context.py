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
