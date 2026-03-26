import asyncio
import os
import time

from aiogram import F, Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, User
from aiogram import Bot

from mafia_bot.game import (
    MIN_PLAYERS,
    PHASE_DAY,
    PHASE_FINISHED,
    PHASE_NIGHT,
    ROLE_DON,
    ROLE_EMOJI,
    ROLE_MAFIA,
    GameStorage,
    all_roles_info_text,
    role_card_text,
)

router = Router()
storage = GameStorage()


def read_phase_seconds(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


NIGHT_PHASE_SECONDS = read_phase_seconds("NIGHT_PHASE_SECONDS", 90)
DAY_PHASE_SECONDS = read_phase_seconds("DAY_PHASE_SECONDS", 150)

phase_timers: dict[int, asyncio.Task] = {}
phase_locks: dict[int, asyncio.Lock] = {}
chat_penalties: dict[int, dict[int, dict[str, float | int | bool]]] = {}


def get_phase_lock(chat_id: int) -> asyncio.Lock:
    lock = phase_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        phase_locks[chat_id] = lock
    return lock


def cancel_phase_timer(chat_id: int) -> None:
    timer = phase_timers.pop(chat_id, None)
    if timer is not None:
        timer.cancel()


def clear_chat_penalties(chat_id: int) -> None:
    chat_penalties.pop(chat_id, None)


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
    if user.username:
        return f"@{user.username}"
    return user.full_name


def player_display_name(player) -> str:
    name = (player.full_name or "").strip()
    if name:
        return name
    return f"Игрок {player.user_id}"


async def registration_join_link(message: Message, chat_id: int) -> str:
    me = await message.bot.get_me()
    return f"https://t.me/{me.username}?start=join_{chat_id}"


def registration_text(room) -> str:
    title = room.chat_title or "Этот чат"
    return (
        f"<b>Регистрация в игру Мафия</b>\n"
        f"Чат: {title}\n\n"
        f"{room.lobby_text()}"
    )


async def refresh_registration_post(message: Message, room) -> None:
    if room.registration_message_id is None:
        return

    link = await registration_join_link(message, room.chat_id)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Присоединиться", url=link)],
        ]
    )
    try:
        await message.bot.edit_message_text(
            chat_id=room.chat_id,
            message_id=room.registration_message_id,
            text=registration_text(room),
            reply_markup=keyboard,
        )
    except Exception:
        return


def registration_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Запустить регистрацию", callback_data="reg:start"),
                InlineKeyboardButton(text="Продлить регистрацию", callback_data="reg:extend"),
            ],
            [
                InlineKeyboardButton(text="Завершить регистрацию и начать", callback_data="reg:finish_start"),
            ],
            [
                InlineKeyboardButton(text="Отменить регистрацию", callback_data="reg:cancel"),
            ],
        ]
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
            return None
        if actor.role == "Маньяк":
            return room.maniac_target_id
        if actor.role == "Любовница":
            return room.mistress_target_id
        if actor.role == "Бомж":
            return room.bum_target_id

    if room.phase == "day":
        return room.day_votes.get(actor_user_id)

    return None


def build_action_keyboard(room, actor_user_id: int) -> InlineKeyboardMarkup | None:
    actor = room.get_player(actor_user_id)
    if actor is None or not actor.alive:
        return None

    rows: list[list[InlineKeyboardButton]] = []
    alive_targets = [p for p in room.alive_players() if p.user_id != actor_user_id]
    selected_target_id = selected_target_for_actor(room, actor_user_id)

    def mark(name: str, user_id: int) -> str:
        return f"✅ {name}" if selected_target_id == user_id else name

    if room.phase == "night":
        if actor.role in {"Дон", "Мафия"}:
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=f"Устранить: {mark(target.full_name, target.user_id)}",
                            callback_data=f"act:kill:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Доктор":
            for target in room.alive_players():
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=f"Лечить: {mark(target.full_name, target.user_id)}",
                            callback_data=f"act:heal:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Комиссар Каттани":
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=f"Проверить: {target.full_name}",
                            callback_data=f"act:check:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Маньяк":
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=f"Маньяк: {mark(target.full_name, target.user_id)}",
                            callback_data=f"act:maniac:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Любовница":
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=f"Отвлечь: {mark(target.full_name, target.user_id)}",
                            callback_data=f"act:mistress:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )
        if actor.role == "Бомж":
            for target in alive_targets:
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=f"Наблюдать: {mark(target.full_name, target.user_id)}",
                            callback_data=f"act:bum:{room.chat_id}:{target.user_id}",
                        )
                    ]
                )

    if room.phase == "day":
        for target in alive_targets:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"Голос за: {mark(target.full_name, target.user_id)}",
                        callback_data=f"act:vote:{room.chat_id}:{target.user_id}",
                    )
                ]
            )

    if not rows:
        return None

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_action_menu(message: Message) -> None:
    if message.chat.type != "private":
        await message.answer("Действия игроков выполняются только в личке с ботом.")
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

    await message.answer("Выбери действие на текущую фазу:", reply_markup=keyboard)


async def push_phase_action_menus(bot: Bot, room) -> None:
    for player in room.alive_players():
        keyboard = build_action_keyboard(room, player.user_id)
        if keyboard is None:
            continue
        try:
            await bot.send_message(
                player.user_id,
                f"Фаза: {room.phase}. Выбери действие:",
                reply_markup=keyboard,
            )
        except Exception:
            await bot.send_message(
                room.chat_id,
                f"Не смог отправить меню хода игроку {player.full_name}."
                " Пусть напишет боту /start в личке."
            )


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
    await bot.send_message(room.chat_id, "🤵🏻 В мафиозной семье выбран новый Дон.")
    await send_mafia_private_update(
        room,
        bot,
        (
            f"👑 Власть семьи переходит к {don_name}.\n"
            "Теперь он - 🤵🏻 Дон и принимает решающее слово мафии."
        ),
    )


async def process_night_end(bot: Bot, chat_id: int, timer_reason: str | None = None) -> None:
    lock = get_phase_lock(chat_id)
    async with lock:
        room = storage.get_room(chat_id)
        if room is None or room.phase != "night":
            return

        cancel_phase_timer(chat_id)
        ok, info, eliminated, don_transfer_note, don_successor_id = room.resolve_night()
        if not ok:
            await bot.send_message(chat_id, info)
            return

        if timer_reason:
            await bot.send_message(chat_id, timer_reason)

        reports = room.pop_night_reports()
        for user_id, lines in reports.items():
            try:
                await bot.send_message(user_id, "\n".join(lines))
            except Exception:
                continue

        await bot.send_message(chat_id, room.day_intro_text())
        if don_transfer_note:
            await announce_don_transfer(room, bot, don_successor_id)

        if eliminated:
            for dead in eliminated:
                role_text = f"{ROLE_EMOJI.get(dead.role, '')} {dead.role}".strip()
                await bot.send_message(chat_id, f"Сегодня был жестоко убит {role_text} {dead.full_name}...")
        else:
            await bot.send_message(chat_id, "🤷 Удивительно, но этой ночью все выжили")

        await bot.send_message(chat_id, room.alive_players_text())
        await bot.send_message(chat_id, room.alive_role_hints_text())

        if room.phase == PHASE_FINISHED:
            await bot.send_message(chat_id, info)
            await bot.send_message(chat_id, room.final_report_text())
            cancel_phase_timer(chat_id)
            return

        await bot.send_message(
            chat_id,
            (
                "Сейчас самое время обсудить результаты ночи.\n"
                "Пришло время определить и наказать виноватых.\n"
                "Голосование проводится в личке бота.\n"
                f"У вас {DAY_PHASE_SECONDS} сек. на дневную фазу."
            ),
        )
        await push_phase_action_menus(bot, room)
        await start_phase_timer(room, bot)


async def process_day_end(bot: Bot, chat_id: int, timer_reason: str | None = None) -> None:
    lock = get_phase_lock(chat_id)
    async with lock:
        room = storage.get_room(chat_id)
        if room is None or room.phase != "day":
            return

        cancel_phase_timer(chat_id)

        if timer_reason and not room.day_votes:
            ok, info = room.end_day_without_votes()
            eliminated: list = []
            don_transfer_note = None
            don_successor_id = None
        else:
            ok, info, eliminated, don_transfer_note, don_successor_id = room.resolve_day()

        if not ok:
            await bot.send_message(chat_id, info)
            return

        if timer_reason:
            await bot.send_message(chat_id, timer_reason)

        if eliminated:
            first = eliminated[0]
            role_text = f"{ROLE_EMOJI.get(first.role, '')} {first.role}".strip()
            await bot.send_message(chat_id, f"По итогам голосования выбыл: {first.full_name} ({role_text}).")
            if first.role == "Самоубийца":
                await bot.send_message(chat_id, "Самоубийца выполнил личную цель победы.")
            if first.role == "Камикадзе" and len(eliminated) > 1:
                second = eliminated[1]
                second_role = f"{ROLE_EMOJI.get(second.role, '')} {second.role}".strip()
                await bot.send_message(chat_id, f"Камикадзе забрал с собой {second.full_name} ({second_role}).")
        else:
            await bot.send_message(chat_id, "Голосование завершено: никто не выбыл.")

        if don_transfer_note:
            await announce_don_transfer(room, bot, don_successor_id)

        if room.phase == PHASE_FINISHED:
            await bot.send_message(chat_id, info)
            await bot.send_message(chat_id, room.final_report_text())
            cancel_phase_timer(chat_id)
            return

        await bot.send_message(chat_id, "День завершен. Наступает ночь.")
        await bot.send_message(chat_id, room.night_intro_text())
        await bot.send_message(
            chat_id,
            f"Игроки делают ночные действия в личке бота. На ночь {NIGHT_PHASE_SECONDS} сек. Меню разослано автоматически.",
        )
        await push_phase_action_menus(bot, room)
        await start_phase_timer(room, bot)


async def phase_timer_worker(bot: Bot, chat_id: int, phase: str, duration_sec: int) -> None:
    try:
        await asyncio.sleep(duration_sec)
        room = storage.get_room(chat_id)
        if room is None or room.phase != phase:
            return

        if phase == "night":
            await process_night_end(bot, chat_id, timer_reason="⏱ Время ночи вышло. Фаза закрыта автоматически.")
        elif phase == "day":
            await process_day_end(bot, chat_id, timer_reason="⏱ Время дня вышло. Фаза закрыта автоматически.")
    except asyncio.CancelledError:
        return


async def start_phase_timer(room, bot: Bot) -> None:
    cancel_phase_timer(room.chat_id)

    if room.phase == "night":
        phase_timers[room.chat_id] = asyncio.create_task(
            phase_timer_worker(bot, room.chat_id, room.phase, NIGHT_PHASE_SECONDS)
        )
    elif room.phase == "day":
        phase_timers[room.chat_id] = asyncio.create_task(
            phase_timer_worker(bot, room.chat_id, room.phase, DAY_PHASE_SECONDS)
        )


async def maybe_finish_phase_early(bot: Bot, room) -> None:
    if room.phase == "night" and room.all_required_night_actions_done():
        await process_night_end(bot, room.chat_id, timer_reason="⚡ Все ночные действия получены. Ночь завершается досрочно.")
        return

    if room.phase == "day" and room.all_alive_day_voted():
        await process_day_end(bot, room.chat_id, timer_reason="⚡ Все дневные голоса получены. День завершается досрочно.")


def mafia_allies_text(room) -> str:
    allies = [player for player in room.players.values() if player.role in {ROLE_DON, ROLE_MAFIA}]
    if not allies:
        return ""

    lines = ["", "Запомни своих союзников:"]
    for ally in allies:
        role_mark = f"{ROLE_EMOJI.get(ally.role, '')} {ally.role}".strip()
        lines.append(f"  {ally.full_name} - {role_mark}")
    return "\n".join(lines)


@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject) -> None:
    if message.chat.type == "private" and command.args and command.args.startswith("join_"):
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

        nickname = user_nickname(message.from_user)
        ok, info = room.add_player(message.from_user.id, nickname)
        if not ok:
            await message.answer(info)
            return

        await message.answer(
            f"Ты присоединился к игре в чате: {room.chat_title or room.chat_id}.\n"
            f"Твой ник в регистрации: {nickname}."
        )
        await message.bot.send_message(
            room.chat_id,
            f"{nickname} присоединился к регистрации.",
        )
        await refresh_registration_post(message, room)
        return

    text = (
        "Привет. Это бот для игры в Мафию.\n\n"
        "Команды:\n"
        "/panel - кнопки управления регистрацией\n"
        "/create - создать лобби\n"
        "/join - войти в лобби\n"
        "/leave - выйти из лобби\n"
        "/lobby - показать игроков\n"
        "/begin - начать игру\n"
        "/roles - список ролей и описаний\n"
        "/action - сделать ход (в личке с ботом)\n"
        "/status - статус текущей игры\n"
        "/kill (reply) - ночной выбор мафии\n"
        "/heal (reply) - ход доктора ночью\n"
        "/check (reply) - проверка комиссара ночью\n"
        "/night_end - завершить ночь\n"
        "/vote (reply) - дневное голосование\n"
        "/day_end - завершить день\n"
        "/close - закрыть лобби"
    )
    await message.answer(text)


@router.message(Command("roles"))
async def cmd_roles(message: Message) -> None:
    await message.answer(all_roles_info_text())


@router.message(Command("action"))
async def cmd_action(message: Message) -> None:
    await send_action_menu(message)


@router.message(Command("panel"))
async def cmd_panel(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer("Панель доступна в групповом чате.")
        return

    await message.answer("Панель регистрации:", reply_markup=registration_panel())


@router.message(Command("create"))
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

    room.chat_title = message.chat.title or "Групповой чат"
    room.players.clear()
    room.open_registration()
    room.add_player(message.from_user.id, user_nickname(message.from_user))

    join_link = await registration_join_link(message, message.chat.id)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Присоединиться", url=join_link)],
        ]
    )
    sent = await message.answer(registration_text(room), reply_markup=keyboard)
    room.registration_message_id = sent.message_id
    await message.answer("Регистрация запущена через команду. Игроки входят кнопкой ниже.")


@router.message(Command("join"))
async def cmd_join(message: Message) -> None:
    if is_user_blocked(message.chat.id, message.from_user.id):
        await notify_registration_blocked(message.bot, message.chat.id, message.from_user.id)
        await message.answer("Ты не можешь войти в лобби, пока действует мут. Проверь ЛС бота.")
        return

    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Сначала создай лобби: /create")
        return

    if not room.registration_open:
        await message.answer("Регистрация закрыта.")
        return

    ok, info = room.add_player(message.from_user.id, user_nickname(message.from_user))
    if not ok:
        await message.answer(info)
        return

    await message.answer("Ты вошел в лобби.")
    await message.answer(room.lobby_text())
    await refresh_registration_post(message, room)


@router.message(Command("leave"))
async def cmd_leave(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    ok, info = room.remove_player(message.from_user.id)
    await message.answer(info)

    if room.players:
        await message.answer(room.lobby_text())
        await refresh_registration_post(message, room)
    else:
        cancel_phase_timer(message.chat.id)
        clear_chat_penalties(message.chat.id)
        storage.close_room(message.chat.id)
        await message.answer("Лобби пустое и закрыто.")


@router.message(Command("lobby"))
async def cmd_lobby(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    await message.answer(room.lobby_text())


@router.message(Command("begin"))
async def cmd_begin(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    if not room.registration_open:
        await message.answer("Сначала открой регистрацию или используй кнопки из /panel.")
        return

    if len(room.players) < MIN_PLAYERS:
        await message.answer(f"Нужно минимум {MIN_PLAYERS} игрока(ов).")
        return

    room.close_registration()
    room.assign_roles()
    await message.answer(
        "Игра начинается!\n\n"
        "В течение нескольких секунд бот пришлет вам личное сообщение с ролью и ее описанием."
    )

    for player in room.players.values():
        try:
            card_text = role_card_text(player.role, message.chat.title or "Групповой чат")
            if player.role in {ROLE_DON, ROLE_MAFIA}:
                card_text += mafia_allies_text(room)
            await message.bot.send_message(
                player.user_id,
                card_text,
            )
        except Exception:
            await message.answer(
                f"Не смог отправить роль {player.full_name}."
                " Пусть напишет боту /start в личке."
            )

    await message.answer(room.night_intro_text())
    await message.answer(
        f"Игроки делают ночные действия в личке бота. На ночь {NIGHT_PHASE_SECONDS} сек. Меню разослано автоматически."
    )
    await push_phase_action_menus(message.bot, room)
    await start_phase_timer(room, message.bot)


@router.callback_query(F.data.startswith("reg:"))
async def on_registration_action(callback: CallbackQuery) -> None:
    if callback.message is None or callback.from_user is None:
        return

    chat_id = callback.message.chat.id
    action = callback.data.split(":", maxsplit=1)[1]

    if is_user_blocked(chat_id, callback.from_user.id):
        await notify_registration_blocked(callback.bot, chat_id, callback.from_user.id)
        await callback.answer("Пока действует мут, регистрация недоступна. Проверь ЛС бота.", show_alert=True)
        return

    room = storage.get_room(chat_id)

    if action == "start":
        if room is None:
            ok, info = storage.create_room(chat_id=chat_id, host_id=callback.from_user.id)
            if not ok:
                await callback.answer(info, show_alert=True)
                return
            room = storage.get_room(chat_id)

        room.open_registration()
        room.add_player(callback.from_user.id, user_nickname(callback.from_user))
        await callback.message.answer("Регистрация запущена. Игроки могут входить через /join")
        await callback.message.answer(room.lobby_text())
        await callback.answer("Готово")
        return

    if room is None:
        await callback.answer("Сначала запусти регистрацию.", show_alert=True)
        return

    if action == "extend":
        if room.started:
            await callback.answer("Игра уже началась.", show_alert=True)
            return
        room.extend_registration()
        await callback.message.answer(
            f"Регистрация продлена. Продлений: {room.registration_extensions}."
        )
        await callback.message.answer(room.lobby_text())
        await callback.answer("Продлено")
        return

    if action == "finish_start":
        if room.started:
            await callback.answer("Игра уже началась.", show_alert=True)
            return
        if len(room.players) < MIN_PLAYERS:
            await callback.answer(f"Нужно минимум {MIN_PLAYERS} игрока(ов).", show_alert=True)
            return

        room.close_registration()
        room.assign_roles()
        await callback.message.answer(
            "Игра начинается!\n\n"
            "В течение нескольких секунд бот пришлет вам личное сообщение с ролью и ее описанием."
        )

        for player in room.players.values():
            try:
                card_text = role_card_text(player.role, callback.message.chat.title or "Групповой чат")
                if player.role in {ROLE_DON, ROLE_MAFIA}:
                    card_text += mafia_allies_text(room)
                await callback.bot.send_message(
                    player.user_id,
                    card_text,
                )
            except Exception:
                await callback.message.answer(
                    f"Не смог отправить роль {player.full_name}."
                    " Пусть напишет боту /start в личке."
                )

        await callback.message.answer(room.night_intro_text())
        await callback.message.answer(
            f"Игроки делают ночные действия в личке бота. На ночь {NIGHT_PHASE_SECONDS} сек. Меню разослано автоматически."
        )
        await push_phase_action_menus(callback.bot, room)
        await start_phase_timer(room, callback.bot)
        await callback.answer("Игра начата")
        return

    if action == "cancel":
        cancel_phase_timer(chat_id)
        clear_chat_penalties(chat_id)
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


@router.callback_query(F.data.startswith("act:"))
async def on_action_callback(callback: CallbackQuery) -> None:
    if callback.message is None or callback.from_user is None:
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
    if actor is None or not actor.alive:
        await callback.answer("Ты не участвуешь в этой фазе.", show_alert=True)
        return

    async def announce_night_role_once(role_name: str) -> None:
        if room.phase != "night":
            return
        if not room.mark_night_role_announced(role_name):
            return
        role_mark = f"{ROLE_EMOJI.get(role_name, '')} {role_name}".strip()
        await callback.bot.send_message(room.chat_id, f"{role_mark} сделал ночной ход.")

    if action == "kill":
        ok, info = room.set_night_vote(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            await callback.message.answer("Ночной выбор мафии принят.")
            actor = room.get_player(callback.from_user.id)
            target = room.get_player(target_id)
            if actor is not None and target is not None:
                role_mark = f"{ROLE_EMOJI.get(actor.role, '')} {actor.role}".strip()
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
            keyboard = build_action_keyboard(room, callback.from_user.id)
            if keyboard is not None:
                await callback.message.edit_reply_markup(reply_markup=keyboard)
            await maybe_finish_phase_early(callback.bot, room)
        return

    if action == "heal":
        ok, info = room.set_doctor_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            await callback.message.answer("Выбор доктора принят.")
            await announce_night_role_once(actor.role)
            keyboard = build_action_keyboard(room, callback.from_user.id)
            if keyboard is not None:
                await callback.message.edit_reply_markup(reply_markup=keyboard)
            await maybe_finish_phase_early(callback.bot, room)
        return

    if action == "check":
        ok, info = room.check_player_role(callback.from_user.id, target_id)
        await callback.answer("Проверка выполнена." if ok else info, show_alert=not ok)
        if ok:
            await callback.message.answer(info)
            await announce_night_role_once(actor.role)
            await maybe_finish_phase_early(callback.bot, room)
        return

    if action == "vote":
        ok, info = room.set_day_vote(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            await callback.message.answer("Твой голос принят.")
            keyboard = build_action_keyboard(room, callback.from_user.id)
            if keyboard is not None:
                await callback.message.edit_reply_markup(reply_markup=keyboard)
            await maybe_finish_phase_early(callback.bot, room)
        return

    if action == "maniac":
        ok, info = room.set_maniac_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            await callback.message.answer("Маньяк выбрал цель.")
            await announce_night_role_once(actor.role)
            keyboard = build_action_keyboard(room, callback.from_user.id)
            if keyboard is not None:
                await callback.message.edit_reply_markup(reply_markup=keyboard)
            await maybe_finish_phase_early(callback.bot, room)
        return

    if action == "mistress":
        ok, info = room.set_mistress_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            await callback.message.answer("Любовница сделала ход.")
            await announce_night_role_once(actor.role)
            keyboard = build_action_keyboard(room, callback.from_user.id)
            if keyboard is not None:
                await callback.message.edit_reply_markup(reply_markup=keyboard)
            await maybe_finish_phase_early(callback.bot, room)
        return

    if action == "bum":
        ok, info = room.set_bum_target(callback.from_user.id, target_id)
        await callback.answer(info, show_alert=not ok)
        if ok:
            await callback.message.answer("Бомж отправился наблюдать.")
            await announce_night_role_once(actor.role)
            keyboard = build_action_keyboard(room, callback.from_user.id)
            if keyboard is not None:
                await callback.message.edit_reply_markup(reply_markup=keyboard)
            await maybe_finish_phase_early(callback.bot, room)
        return

    await callback.answer("Неизвестный тип действия.", show_alert=True)


@router.message(F.chat.type == "private", F.text)
async def on_private_text(message: Message) -> None:
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    room = get_private_action_room(message.from_user.id)
    if room is None:
        return

    actor = room.get_player(message.from_user.id)
    if actor is None or not actor.alive:
        return

    if room.phase != "night":
        return

    if actor.role not in {ROLE_DON, ROLE_MAFIA}:
        return

    relay_text = f"{actor.full_name}:\n{text}"
    for teammate in room.alive_players():
        if teammate.role not in {ROLE_DON, ROLE_MAFIA}:
            continue
        if teammate.user_id == actor.user_id:
            continue
        try:
            await message.bot.send_message(teammate.user_id, relay_text)
        except Exception:
            continue


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def enforce_group_game_rules(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None or not room.started or room.phase == PHASE_FINISHED:
        return

    if message.from_user is None or message.from_user.is_bot:
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
        # At day, only alive players can speak in the group chat.
        if not is_alive_player:
            await safe_delete_message(message)
            await process_rule_violation(message)
        return


@router.message(Command("close"))
async def cmd_close(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    cancel_phase_timer(message.chat.id)
    clear_chat_penalties(message.chat.id)
    storage.close_room(message.chat.id)
    await message.answer("Лобби закрыто.")
