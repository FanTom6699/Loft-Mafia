import asyncio
import os
import time
from datetime import datetime
from html import escape

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message, User
from aiogram import Bot

from mafia_bot.storage import GameStateRepository
from mafia_bot.game import (
    DAY_STAGE_DISCUSSION,
    DAY_STAGE_NOMINATION,
    DAY_STAGE_TRIAL,
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


NIGHT_PHASE_SECONDS = read_phase_seconds("NIGHT_PHASE_SECONDS", 90)
DAY_DISCUSSION_SECONDS = read_phase_seconds("DAY_DISCUSSION_SECONDS", 60)
DAY_NOMINATION_SECONDS = read_phase_seconds("DAY_NOMINATION_SECONDS", 60)
DAY_TRIAL_SECONDS = read_phase_seconds("DAY_TRIAL_SECONDS", 60)
REGISTRATION_SECONDS = read_phase_seconds("REGISTRATION_SECONDS", 60)
REGISTRATION_EXTENSION_SECONDS = read_phase_seconds("REGISTRATION_EXTENSION_SECONDS", 30)
RESTART_EXPIRED_PHASE_POLICY = os.getenv("RESTART_EXPIRED_PHASE_POLICY", "catch_up").strip().lower()
DAY_IMAGE_PATH = os.getenv("DAY_IMAGE_PATH", os.path.join("assets", "day.jpg"))
NIGHT_IMAGE_PATH = os.getenv("NIGHT_IMAGE_PATH", os.path.join("assets", "night.jpg"))

phase_timers: dict[int, asyncio.Task] = {}
registration_timers: dict[int, asyncio.Task] = {}
phase_locks: dict[int, asyncio.Lock] = {}
chat_penalties: dict[int, dict[int, dict[str, float | int | bool]]] = {}
action_menu_messages: dict[int, dict[int, int]] = {}


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
    timer = registration_timers.pop(chat_id, None)
    if timer is not None:
        timer.cancel()


def current_day_stage_seconds(room) -> int:
    if room.day_stage == DAY_STAGE_DISCUSSION:
        return DAY_DISCUSSION_SECONDS
    if room.day_stage == DAY_STAGE_NOMINATION:
        return DAY_NOMINATION_SECONDS
    if room.day_stage == DAY_STAGE_TRIAL:
        return DAY_TRIAL_SECONDS
    return DAY_NOMINATION_SECONDS


def clear_chat_penalties(chat_id: int) -> None:
    chat_penalties.pop(chat_id, None)


def persist_room(room) -> None:
    repo.save_room(room)


def remove_room_state(chat_id: int) -> None:
    repo.delete_room(chat_id)


def role_mark_text(role: str) -> str:
    emoji = ROLE_EMOJI.get(role, "")
    return f"{emoji} <b>{role}</b>".strip()


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

    await bot.send_message(chat_id, caption, reply_markup=reply_markup)


def skipped_turn_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Вы пропустили ход",
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
        elif player.role == "Комиссар Каттани" and room.commissar_target_id is None:
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


async def mark_skipped_night_menus(bot: Bot, room, skipped_user_ids: list[int]) -> None:
    if not skipped_user_ids:
        return

    keyboard = skipped_turn_keyboard(room.chat_id)
    for user_id in skipped_user_ids:
        message_id = get_action_menu_message_id(room.chat_id, user_id)
        if message_id is None:
            continue
        try:
            await bot.edit_message_reply_markup(
                chat_id=user_id,
                message_id=message_id,
                reply_markup=keyboard,
            )
        except Exception:
            continue


def format_killer_sources_text(sources: list[str]) -> str:
    if not sources:
        return ""

    labels: list[str] = []
    for source in sources:
        if source == "мафия":
            labels.append(role_mark_text(ROLE_DON))
        elif source == "маньяк":
            labels.append(role_mark_text(ROLE_MANIAC))
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
        return REGISTRATION_SECONDS
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

    async def worker() -> None:
        try:
            warning_mark = 30
            if seconds > warning_mark:
                await asyncio.sleep(seconds - warning_mark)
                current_room = storage.get_room(room.chat_id)
                if current_room is not None and not current_room.started and current_room.registration_open:
                    me = await bot.get_me()
                    join_link = f"https://t.me/{me.username}?start=join_{room.chat_id}"
                    await bot.send_message(
                        room.chat_id,
                        f"До окончания регистрации осталось {warning_mark} сек.",
                        reply_markup=registration_lobby_keyboard(join_link),
                    )
                await asyncio.sleep(warning_mark)
            else:
                await asyncio.sleep(seconds)
            await process_registration_timeout(bot, room.chat_id)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"[ERROR] registration_timer_worker: chat_id={room.chat_id}, error={e!r}")

    registration_timers[room.chat_id] = asyncio.create_task(worker())


async def launch_game_from_registration(bot: Bot, room, chat_id: int, chat_title: str | None) -> None:
    cancel_registration_timer(chat_id)
    room.close_registration()
    try:
        room.assign_roles()
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
            "Игра начинается!\n\n"
            "В течение нескольких секунд бот пришлёт вам личное сообщение с ролью и её описанием.",
        )
    except Exception as e:
        print(f"[ERROR] send_message(Игра начинается): {e!r}")

    async def send_role_cards() -> None:
        async def send_one_role_card(player) -> tuple[str, bool]:
            name = player_display_name(player)
            try:
                card_text = role_card_for_player(room, player, chat_title or room.chat_title or "Групповой чат")
                await asyncio.wait_for(bot.send_message(player.user_id, card_text), timeout=8)
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

    # Small pause so players can read their role card before the night UI arrives.
    await asyncio.sleep(1)

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
    try:
        room = storage.get_room(chat_id)
    except Exception as e:
        print(f"[ERROR] storage.get_room: {e!r}")
        return
    if room is None:
        print(f"[ERROR] process_registration_timeout: room is None for chat_id={chat_id}")
        return
    if room.started or not room.registration_open:
        print(f"[ERROR] process_registration_timeout: already started or registration closed for chat_id={chat_id}")
        return

    if len(room.players) < MIN_PLAYERS:
        try:
            await clear_registration_post(bot, room)
        except Exception as e:
            print(f"[ERROR] clear_registration_post (min players): {e!r}")
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
    survived = int(stats.get("survived_games", 0))
    suicide_personal_wins = int(stats.get("suicide_personal_wins", 0))
    mafia_games = int(stats.get("mafia_games", 0))
    maniac_games = int(stats.get("maniac_games", 0))
    civilian_games = int(stats.get("civilian_games", 0))
    last_role = str(stats.get("last_role", "") or "-")
    name = str(stats.get("display_name", "Игрок"))
    last_role_mark = role_mark_text(last_role) if last_role != "-" else "-"

    win_rate = (wins / games * 100.0) if games > 0 else 0.0
    survival_rate = (survived / games * 100.0) if games > 0 else 0.0

    return (
        "<b>Твоя статистика</b>\n"
        f"Игрок: {name}\n"
        f"Игр сыграно: {games}\n"
        f"Побед: {wins}\n"
        f"Поражений: {losses}\n"
        f"Винрейт: {win_rate:.1f}%\n"
        f"Выживал до конца: {survived} ({survival_rate:.1f}%)\n"
        f"Личных побед Самоубийцы: {suicide_personal_wins}\n"
        f"Партий за мафию: {mafia_games}\n"
        f"Партий за маньяка: {maniac_games}\n"
        f"Партий за мирных: {civilian_games}\n"
        f"Последняя роль: {last_role_mark}"
    )


async def safe_delete_message(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        return


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


async def registration_join_link(message: Message, chat_id: int) -> str:
    me = await message.bot.get_me()
    return f"https://t.me/{me.username}?start=join_{chat_id}"


def registration_text(room) -> str:
    lines = ["Ведётся набор в игру", ""]
    if not room.players:
        lines.append("Зарегистрировались::")
        lines.append("Пока никто не зарегистрировался.")
        lines.append("")
        lines.append("Итого 0 чел.")
        return "\n".join(lines)

    joined_names = ", ".join(player_display_name(player) for player in room.players.values())
    lines.append("Зарегистрировались::")
    lines.append(joined_names)
    lines.append("")
    lines.append(f"Итого {len(room.players)} чел.")
    return "\n".join(lines)


def registration_post_text(room) -> str:
    remaining = registration_remaining_seconds(room)
    if remaining <= 0:
        remaining = REGISTRATION_SECONDS
    return registration_text(room) + f"\n\nДо окончания регистрации осталось {remaining} сек."


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


def registration_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Создать лобби", callback_data="reg:start"),
            ],
        ]
    )


def private_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Список ролей", callback_data="pmenu:roles")],
            [InlineKeyboardButton(text="Статистика", callback_data="pmenu:stats")],
        ]
    )


def private_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="pmenu:main")]]
    )


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
        if actor.role in {"Дон", "Мафия"}:
            return room.night_votes.get(actor_user_id)
        if actor.role == "Доктор":
            return room.doctor_target_id
        if actor.role == "Комиссар Каттани":
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

    kamikaze_revenge_mode = (
        room.phase == PHASE_NIGHT
        and actor.role == ROLE_KAMIKAZE
        and room.kamikaze_pending_user_id == actor_user_id
    )
    if not actor.alive and not kamikaze_revenge_mode:
        return None

    rows: list[list[InlineKeyboardButton]] = []
    all_players_order = list(room.players.values())
    seat_positions = {p.user_id: i for i, p in enumerate(all_players_order, start=1)}
    alive_players = room.alive_players()
    alive_targets = [p for p in alive_players if p.user_id != actor_user_id]
    selected_target_id = selected_target_for_actor(room, actor_user_id)

    def mark(name: str, user_id: int) -> str:
        return f"✅ {name}" if selected_target_id == user_id else name

    def target_label(target, teammate_mark: str = "") -> str:
        position = seat_positions.get(target.user_id)
        base_name = player_display_name(target)
        if position is None:
            return f"{teammate_mark} {base_name}".strip()
        return f"{position}.{teammate_mark} {base_name}".strip()

    if room.phase == "night":
        if actor.role in {"Дон", "Мафия"}:
            mafia_teammate_ids = {p.user_id for p in alive_players if p.role in {ROLE_DON, ROLE_MAFIA}}
            for target in alive_targets:
                teammate_mark = " 🤵🏻" if target.user_id in mafia_teammate_ids else ""
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=mark(target_label(target, teammate_mark), target.user_id),
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
        for target in alive_targets:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=mark(target_label(target), target.user_id),
                        callback_data=f"act:vote:{room.chat_id}:{target.user_id}",
                    )
                ]
            )
        skip_text = "✅ Пропустить ход" if selected_target_id == 0 else "Пропустить ход"
        rows.append(
            [
                InlineKeyboardButton(
                    text=skip_text,
                    callback_data=f"act:skipvote:{room.chat_id}:0",
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
            return "<b>Кого будем проверять?</b>"
        return "Сейчас у твоей роли нет активных ночных действий."

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION:
        return "Пришло время искать виноватых!\nКого ты хочешь линчевать?"

    return "Выбери действие на текущую фазу:"


def night_status_text(room) -> str:
    alive = room.alive_players()
    seat_positions = {p.user_id: i for i, p in enumerate(room.players.values(), start=1)}
    lines = ["Живые игроки:"]
    for player in sorted(alive, key=lambda p: seat_positions.get(p.user_id, 10**9)):
        seat_no = seat_positions.get(player.user_id)
        raw_name = (player.full_name or "").strip()
        fallback_name = f"Игрок {seat_no}" if seat_no is not None else f"Игрок {player.user_id}"
        safe_name = escape(raw_name if raw_name else fallback_name)
        if seat_no is None:
            lines.append(f"<a href=\"tg://user?id={player.user_id}\">{safe_name}</a>")
        else:
            lines.append(f"{seat_no}. <a href=\"tg://user?id={player.user_id}\">{safe_name}</a>")
    lines.append(f"\nСпать осталось {NIGHT_PHASE_SECONDS} сек.")
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
        await message.answer("Сейчас у твоей роли нет доступных действий.")
        return

    prompt_text = build_action_prompt_text(room, message.from_user.id)
    sent = await message.answer(prompt_text, reply_markup=keyboard)
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
        sent = await bot.send_message(user_id, prompt_text, reply_markup=keyboard)
        track_action_menu_message(room.chat_id, user_id, sent.message_id)
    except Exception:
        await bot.send_message(
            room.chat_id,
            "Не смог отправить меню камикадзе. Пусть напишет боту /start в личке.",
        )


async def push_trial_vote_menus(bot: Bot, room, candidate_name: str) -> None:
    print(f"[TRIAL] push_trial_vote_menus: candidate={candidate_name}, chat_id={room.chat_id}")
    yes_count, no_count = room.trial_vote_counts()
    try:
        await bot.send_message(
            room.chat_id,
            f"Вы точно хотите линчевать {candidate_name}?",
            reply_markup=trial_vote_keyboard(room.chat_id, yes_count, no_count),
        )
    except Exception as e:
        print(f"[ERROR] push_trial_vote_menus: chat_id={room.chat_id}, error={e!r}")


async def send_mafia_private_update(room, bot, text: str) -> None:
    for player in room.alive_players():
        if player.role not in {ROLE_DON, ROLE_MAFIA}:
            continue
        try:
            await bot.send_message(player.user_id, text)
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
        await bot.send_message(commissar_successor_id, "Теперь ты 🕵️‍ Комиссар Каттани")
    except Exception:
        pass


async def prompt_last_words(bot: Bot, room, eliminated) -> None:
    queued_user_ids = room.queue_last_words(eliminated)
    if queued_user_ids:
        persist_room(room)
    for user_id in queued_user_ids:
        try:
            await bot.send_message(
                user_id,
                (
                    "Тебя убили. Ты можешь оставить предсмертное сообщение.\n"
                    "Отправь один текст в ответ, и бот передаст его в группу."
                ),
            )
        except Exception:
            continue


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

        reports = room.pop_night_reports()
        kill_sources = room.pop_night_kill_sources()
        for user_id, lines in reports.items():
            try:
                await bot.send_message(user_id, "\n".join(lines))
            except Exception:
                continue

        await send_phase_media(bot, chat_id, room.day_media_caption(), DAY_IMAGE_PATH)

        if don_transfer_note:
            await announce_don_transfer(room, bot, don_successor_id)
        if commissar_transfer_note:
            await announce_commissar_transfer(room, bot, commissar_successor_id)
        if mafia_alive_tonight and mafia_target_tonight is None:
            await bot.send_message(chat_id, "🚷 🤵🏻 Дон сегодня не в настроении")

        if room.last_doctor_saved_target_id is not None:
            await bot.send_message(chat_id, "👨🏼‍⚕️ Доктор спас кого-то от смерти.")
        if eliminated:
            for dead in eliminated:
                role_text = role_mark_text(dead.role)
                sources = kill_sources.get(dead.user_id, [])
                killer_text = format_killer_sources_text(sources)
                safe_name = escape((dead.full_name or "").strip() or f"Игрок {dead.user_id}")
                text = f"Сегодня был жестоко убит {role_text} {safe_name}"
                if killer_text:
                    text += f"\n{killer_text}"
                await bot.send_message(chat_id, text)
            await prompt_last_words(bot, room, eliminated)
        else:
            await bot.send_message(chat_id, "🌙 Этой ночью было тихо. Никто не погиб.")

        day_summary = (
            room.alive_players_text()
            + "\n\n"
            + room.alive_role_hints_text()
            + "\n\n"
            + "Сейчас самое время обсудить результаты ночи, разобраться в причинах и следствиях..."
        )
        await bot.send_message(chat_id, day_summary)

        if room.phase == PHASE_FINISHED:
            ensure_stats_recorded(room)
            await bot.send_message(chat_id, info)
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
                    "Пришло время определить и наказать виновного.\n"
                    "Выберите кандидата на выгон.\n"
                    f"Голосование продлится: {DAY_NOMINATION_SECONDS} сек."
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
                votes_by_target: dict[int, int] = {}
                for target_id in room.day_votes.values():
                    target = room.get_player(target_id)
                    if target is None or not target.alive:
                        continue
                    votes_by_target[target.user_id] = votes_by_target.get(target.user_id, 0) + 1

                if votes_by_target:
                    sorted_votes = sorted(votes_by_target.items(), key=lambda item: item[1], reverse=True)
                    vote_lines: list[str] = []
                    for target_id, count in sorted_votes:
                        target = room.get_player(target_id)
                        if target is None:
                            continue
                        vote_lines.append(f"- {target.full_name}: {count}")
                    if vote_lines:
                        await bot.send_message(
                            chat_id,
                            "Голоса на этапе выбора кандидата разделились поровну.\n"
                            "🗿 Жители решили никого не вешать...\n\n"
                            "Итоги голосования:\n"
                            + "\n".join(vote_lines),
                        )
                else:
                    await bot.send_message(
                        chat_id,
                        "Голосование окончено\n🗿 Жители решили никого не вешать...",
                    )

                ok_end, info_end = room.end_day_no_lynch()
                print(f"[PHASE] process_day_end: end_day_no_lynch result for chat_id={chat_id}, ok_end={ok_end}, info={info_end}")
                if ok_end:
                    await bot.send_message(chat_id, "Сегодня решили никого не вешать.")
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
            await push_trial_vote_menus(bot, room, candidate.full_name)
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
                await bot.send_message(chat_id, info)
                return

            if eliminated:
                first = eliminated[0]
                role_text = role_mark_text(first.role)
                await bot.send_message(
                    chat_id,
                    f"Результаты голосования:\n{yes_count} 👍 | {no_count} 👎\n\nВешаем {first.full_name}! :)",
                )
                await bot.send_message(chat_id, f"{first.full_name} был {role_text}")
                try:
                    if first.role == ROLE_KAMIKAZE:
                        await bot.send_message(
                            first.user_id,
                            "Тебя линчевали на дневном собрании :(\nКого заберём с собой в могилу?",
                        )
                    else:
                        await bot.send_message(
                            first.user_id,
                            "Тебя линчевали на дневном голосовании.",
                        )
                except Exception:
                    pass
                if first.role == "Самоубийца":
                    await bot.send_message(chat_id, "💀 <b>Самоубийца</b> выполнил личную цель победы.")
                if first.role == "Камикадзе" and len(eliminated) > 1:
                    second = eliminated[1]
                    second_role = role_mark_text(second.role)
                    await bot.send_message(chat_id, f"💣 Камикадзе забрал с собой {second.full_name} ({second_role}).")
            else:
                await bot.send_message(
                    chat_id,
                    "Мнения жителей разошлись\n"
                    f"({yes_count} 👍 | {no_count} 👎 )... Разошлись и сами жители, так никого и не повесив...",
                )

            if don_transfer_note:
                await announce_don_transfer(room, bot, don_successor_id)
            if commissar_transfer_note:
                await announce_commissar_transfer(room, bot, commissar_successor_id)

            if room.phase == PHASE_FINISHED:
                ensure_stats_recorded(room)
                await bot.send_message(chat_id, info)
                await bot.send_message(chat_id, room.final_report_text())
                cancel_phase_timer(chat_id)
                persist_room(room)
                return

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
            await process_night_end(bot, chat_id, timer_reason="⏱ Время ночи вышло. Фаза закрыта автоматически.")
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

    phase_duration = NIGHT_PHASE_SECONDS
    if room.phase == "night":
        phase_duration = NIGHT_PHASE_SECONDS
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
                base_duration = NIGHT_PHASE_SECONDS
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
                        timer_reason="⏱ Время ночи истекло во время перезапуска. Фаза закрыта автоматически."
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
        await process_night_end(bot, room.chat_id, timer_reason="⚡ Все ночные действия получены. Ночь завершается досрочно.")
        return

    if room.phase == "day" and room.day_stage == DAY_STAGE_NOMINATION and room.all_alive_day_voted():
        await process_day_end(bot, room.chat_id, timer_reason=None)
        return

    if room.phase == "day" and room.day_stage == DAY_STAGE_TRIAL and room.all_alive_trial_voted():
        await process_day_end(bot, room.chat_id, timer_reason="⚡ Все дневные голоса получены. День завершается досрочно.")


def mafia_allies_text(room) -> str:
    allies = [player for player in room.players.values() if player.role in {ROLE_DON, ROLE_MAFIA}]
    if not allies:
        return ""

    lines = ["", "Запомни своих союзников:"]
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

    return "\n\nЗапомни своих союзников:\n" + "\n".join(lines)


def role_card_for_player(room, player, chat_title: str) -> str:
    if player.role == ROLE_SERGEANT:
        base = (
            "Ты - 👮🏼‍♂️ Сержант!\n"
            "Помощник комиссара Каттани. Он будет информировать тебя о своих действиях "
            "и держать в курсе событий. Если комиссар погибнет - ты займёшь его место."
        )
        return base + city_power_allies_text(room, ROLE_SERGEANT)

    if player.role == ROLE_COMMISSAR:
        base = (
            "Ты - 🕵️‍ Комиссар Каттани!\n"
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
    if message.chat.type == "private":
        nickname = user_nickname(message.from_user)
        is_private_first_visit = repo.touch_private_user(message.from_user.id, nickname)

    if message.chat.type == "private" and command.args and command.args.startswith("join_"):
        if is_private_first_visit:
            await message.answer(
                (
                    "<b>Добро пожаловать в Мафию</b>\n\n"
                    f"👋 Привет, {nickname}!\n"
                    "Сейчас зарегистрирую тебя в лобби.\n\n"
                    "<b>Дальше:</b>\n"
                    "• Роль и меню ходов будут приходить автоматически в этот чат."
                )
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
        keyboard = private_main_menu_keyboard()
        if is_private_first_visit:
            text = (
                "<b>Добро пожаловать в бота Мафии</b>\n\n"
                f"👋 Привет, {nickname}!\n\n"
                "<b>В этом чате ты можешь:</b>\n"
                "• Получать роль и задания по фазам\n"
                "• Делать ходы кнопками\n"
                "• Смотреть роли и статистику\n\n"
                "Найди лобби в группе и присоединяйся."
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

@router.message(Command("roles"))
async def cmd_roles(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer("Выберите роль:", reply_markup=private_roles_keyboard())
        return
    await message.answer(all_roles_info_text())


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
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
    await message.answer("Профиль отключен. Используй меню ролей и статистики.")


@router.message(Command("action"))
async def cmd_action(message: Message) -> None:
    await message.answer("Меню хода отправляется автоматически при старте каждой фазы.")


@router.message(Command("panel"))
async def cmd_panel(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer("Панель доступна в групповом чате.")
        return

    await message.answer("Панель регистрации:", reply_markup=registration_panel())


@router.message(Command("game"))
async def cmd_create(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer("Создавай лобби в групповом чате.")
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
    elif room.started:
        await message.answer("Игра уже идет. Заверши текущую игру перед новой регистрацией.")
        return

    if room is not None and room.registration_open:
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
        await message.answer(
            "Лобби уже создано. Используй существующее сообщение регистрации "
            "и кнопку регистрации под ним."
        )
        return

    if room is None:
        await message.answer("Не удалось создать лобби. Попробуй еще раз.")
        return

    room.chat_title = message.chat.title or "Групповой чат"
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
    await start_registration_timer(room, message.bot, REGISTRATION_SECONDS)


@router.message(Command("join"))
async def cmd_join(message: Message) -> None:
    await message.answer("Вход в лобби только через inline-кнопку Зарегистрироваться под постом лобби.")


@router.message(Command("leave"))
async def cmd_leave(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    ok, info = room.remove_player(message.from_user.id)
    await message.answer(info)
    if ok:
        persist_room(room)

    if room.players:
        await message.answer(room.lobby_text())
        await refresh_registration_post(message, room)
    else:
        await clear_registration_post(message.bot, room)
        cancel_phase_timer(message.chat.id)
        cancel_registration_timer(message.chat.id)
        clear_chat_penalties(message.chat.id)
        clear_action_menu_messages(message.chat.id)
        remove_room_state(message.chat.id)
        storage.close_room(message.chat.id)
        await message.answer("Лобби пустое и закрыто.")


@router.message(Command("lobby"))
async def cmd_lobby(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await message.answer(room.lobby_text())


@router.message(Command("extend"))
async def cmd_extend(message: Message) -> None:
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
    await message.answer(
        f"Регистрация продлена на {REGISTRATION_EXTENSION_SECONDS} сек. "
        f"Осталось {new_seconds} сек. Продлений: {room.registration_extensions}."
    )
    await refresh_registration_post(message, room)


@router.message(Command("start"))
async def cmd_begin(message: Message) -> None:
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
    action = callback.data.split(":", maxsplit=1)[1]

    room = storage.get_room(chat_id)

    if action == "start":
        if is_user_blocked(chat_id, callback.from_user.id):
            await notify_registration_blocked(callback.bot, chat_id, callback.from_user.id)
            await callback.answer("Пока действует мут, создание лобби недоступно.", show_alert=True)
            return

        if room is None:
            ok, info = storage.create_room(chat_id=chat_id, host_id=callback.from_user.id)
            if not ok:
                await callback.answer(info, show_alert=True)
                return
            room = storage.get_room(chat_id)

        room.chat_title = callback.message.chat.title or "Групповой чат"
        room.open_registration()
        persist_room(room)
        await start_registration_timer(room, callback.bot, REGISTRATION_SECONDS)
        join_link = await registration_join_link(callback.message, chat_id)
        sent = await callback.message.answer(
            registration_post_text(room),
            reply_markup=registration_lobby_keyboard(join_link),
        )
        room.registration_message_id = sent.message_id
        persist_room(room)
        await pin_registration_post(callback.bot, room)
        await callback.message.answer("Лобби создано. Игроки могут входить через кнопку ниже")
        await callback.answer("Готово")
        return

    if action == "join":
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
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await message.answer(room.status_text())


@router.message(Command("kill"))
async def cmd_kill(message: Message) -> None:
    await send_action_menu(message)


@router.message(Command("heal"))
async def cmd_heal(message: Message) -> None:
    await send_action_menu(message)


@router.message(Command("check"))
async def cmd_check(message: Message) -> None:
    await send_action_menu(message)


@router.message(Command("night_end"))
async def cmd_night_end(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await process_night_end(message.bot, message.chat.id)


@router.message(Command("vote"))
async def cmd_vote(message: Message) -> None:
    await send_action_menu(message)


@router.message(Command("day_end"))
async def cmd_day_end(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await process_day_end(message.bot, message.chat.id)


@router.callback_query(F.data.startswith("trial:"))
async def on_trial_callback(callback: CallbackQuery) -> None:
    if callback.message is None or callback.from_user is None:
        return

    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректное голосование.", show_alert=True)
        return

    _, raw_vote, raw_chat_id = parts
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
        await callback.answer("Ты не участвуешь в этом голосовании.", show_alert=True)
        return

    approve = raw_vote == "yes"
    ok, info = room.set_trial_vote(callback.from_user.id, approve)
    await callback.answer(info, show_alert=not ok)
    if not ok:
        return
    persist_room(room)

    # Update shared group vote message after vote
    yes_count, no_count = room.trial_vote_counts()
    candidate = room.get_player(room.trial_candidate_id) if room.trial_candidate_id is not None else None
    candidate_name = candidate.full_name if candidate is not None else "кандидат"
    choice_text = "ЗА" if approve else "ПРОТИВ"
    try:
        await callback.message.edit_text(
            f"Вы точно хотите линчевать {candidate_name}?",
            reply_markup=trial_vote_keyboard(chat_id, yes_count, no_count),
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=trial_vote_keyboard(chat_id, yes_count, no_count))
        except Exception:
            pass

    # Send group message about the vote
    voter = room.get_player(callback.from_user.id)
    if voter and candidate:
        vote_text = "ЗА" if approve else "ПРОТИВ"
        await callback.bot.send_message(
            room.chat_id,
            f"🗳️ {voter.full_name} проголосовал {vote_text} казни {candidate.full_name}"
        )

    # If all have voted, proceed to next phase
    if room.all_alive_trial_voted():
        await process_day_end(callback.bot, room.chat_id, timer_reason="⚡ Все дневные голоса получены. День завершается досрочно.")
    else:
        await maybe_finish_phase_early(callback.bot, room)


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
        await callback.answer("Ты не участвуешь в этой фазе.", show_alert=True)
        return
    if not actor.alive:
        is_kamikaze_revenge = (
            room.phase == PHASE_NIGHT
            and actor.role == ROLE_KAMIKAZE
            and room.kamikaze_pending_user_id == actor.user_id
        )
        if not is_kamikaze_revenge:
            await callback.answer("Ты не участвуешь в этой фазе.", show_alert=True)
            return

    async def announce_night_role_once(role_name: str) -> None:
        if room.phase != "night":
            return
        if not room.mark_night_role_announced(role_name):
            return
        role_announcement = {
            ROLE_COMMISSAR: "🕵️ Комиссар Каттани ушёл искать злодеев...",
            ROLE_BUM: "🧙🏼‍♂️ Бомж пошёл к кому-то за бутылкой...",
            ROLE_MANIAC: "🔪 Маньяк спрятался глубоко в кустах...",
            ROLE_ADVOCATE: "👨🏼‍💼 Адвокат ищет мафию для защиты...",
            ROLE_MISTRESS: "💃🏼 Любовница уже ждёт кого-то в гости...",
            ROLE_DOCTOR: "👨🏼‍⚕️ Доктор вышел на ночное дежурство...",
            ROLE_DON: "🤵🏻 Мафия выбрала жертву...",
            ROLE_MAFIA: "🤵🏻 Мафия выбрала жертву...",
        }
        announcement_text = role_announcement.get(role_name)
        if announcement_text is None:
            role_mark = role_mark_text(role_name)
            announcement_text = f"{role_mark} сделал ночной ход."
        await callback.bot.send_message(room.chat_id, announcement_text)

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
                if not room.mafia_target_announced:
                    room.mafia_target_announced = True
                    await callback.bot.send_message(
                        room.chat_id,
                        "🤵🏻 Мафия определилась с общей целью.",
                    )
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

    if action == "heal":
        if room.doctor_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.set_doctor_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role)
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

    if action == "check":
        if room.commissar_target_id is not None:
            await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
            return
        ok, info = room.check_player_role(callback.from_user.id, target_id)
        await callback.answer("Проверка принята." if ok else info, show_alert=not ok)
        if ok:
            target = room.get_player(target_id)
            await announce_night_role_once(actor.role)
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
            await announce_night_role_once(actor.role)
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
        if callback.from_user.id in room.day_votes:
            await callback.answer("Выбор уже зафиксирован до конца голосования.", show_alert=True)
            return

        ok, info = room.set_day_vote(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
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
                reply_markup=locked_choice_keyboard(selected_name),
            )

            if voter is not None and target is not None:
                await callback.bot.send_message(
                    room.chat_id,
                    f"{player_display_name(voter)} проголосовал за {player_display_name(target)}",
                )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    if action == "skipvote":
        if room.phase != PHASE_DAY or room.day_stage != DAY_STAGE_NOMINATION:
            await callback.answer("Сейчас не этап выбора кандидата.", show_alert=True)
            return

        if callback.from_user.id in room.day_votes:
            await callback.answer("Выбор уже зафиксирован до конца голосования.", show_alert=True)
            return

        voter = room.get_player(callback.from_user.id)
        if voter is None or not voter.alive:
            await callback.answer("Ты не можешь голосовать на этом этапе.", show_alert=True)
            return

        room.day_votes[callback.from_user.id] = 0
        await callback.answer("Пропуск голосования принят.")
        await callback.message.edit_text(
            build_action_prompt_text(room, callback.from_user.id) + "\n\nТы выбрал пропуск",
            reply_markup=locked_choice_keyboard("пропуск"),
        )
        await callback.bot.send_message(
            room.chat_id,
            f"{player_display_name(voter)} пропускает голосование.",
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
            await announce_night_role_once(actor.role)
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
            await announce_night_role_once(actor.role)
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
            await announce_night_role_once(actor.role)
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
    await callback.answer("Этап уже закрыт. Вы пропустили ход.", show_alert=True)


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
        await show_menu_screen("Профиль отключен. Используй меню ролей и статистики.", private_back_to_menu_keyboard())
        await safe_answer()
        return
    await safe_answer("Неизвестный пункт меню.", show_alert=True)


@router.message(F.chat.type == "private", F.text)
async def on_private_text(message: Message) -> None:
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    last_word_room = get_pending_last_word_room(message.from_user.id)
    if last_word_room is not None:
        ok, payload = last_word_room.consume_last_word(message.from_user.id, text)
        if not ok:
            await message.answer(payload)
            return
        persist_room(last_word_room)

        player = last_word_room.get_player(message.from_user.id)
        player_name = player.full_name if player is not None else f"Игрок {message.from_user.id}"
        await message.answer("Предсмертное сообщение принято.")
        await message.bot.send_message(
            last_word_room.chat_id,
            f"🕯 Предсмертное слово {player_name}:\n{payload}",
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
            await message.bot.send_message(teammate.user_id, relay_text)
        except Exception:
            continue


@router.message(F.chat.type.in_({"group", "supergroup"}), ~F.text.startswith("/"))
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
    has_forbidden_media = bool(message.photo) or message.video is not None

    if has_forbidden_media:
        await safe_delete_message(message)
        await process_rule_violation(message)
        return

    if room.phase == PHASE_NIGHT:
        # At night, participants are muted without penalties to avoid blocking gameplay.
        if is_participant:
            if not (is_alive_player and is_command):
                await safe_delete_message(message)
            return

        # Non-participants still receive regular penalties.
        if not (is_alive_player and is_command):
            await safe_delete_message(message)
            await process_rule_violation(message)
        return

    if room.phase == PHASE_DAY:
        # Silenced by mistress effect: messages are deleted for this day without penalty escalation.
        if room.day_silenced_user_id is not None and message.from_user.id == room.day_silenced_user_id:
            await safe_delete_message(message)
            return

        # At day, only alive players can speak in the group chat.
        if not is_alive_player:
            await safe_delete_message(message)
            await process_rule_violation(message)
        return


@router.message(Command("stop"))
async def cmd_close(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    if room.started:
        closing_text = "Игра остановлена."
    elif room.registration_open:
        closing_text = "Регистрация отменена."
    else:
        closing_text = "Лобби закрыто."

    await clear_registration_post(message.bot, room)
    cancel_phase_timer(message.chat.id)
    cancel_registration_timer(message.chat.id)
    clear_chat_penalties(message.chat.id)
    clear_action_menu_messages(message.chat.id)
    remove_room_state(message.chat.id)
    storage.close_room(message.chat.id)
    await message.answer(closing_text)
