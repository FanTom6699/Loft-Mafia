# Extracted from the original mafia_bot/handlers.py.
# Stage 1 refactor: gameplay behavior is intentionally unchanged.
from ._context import *  # noqa: F401,F403

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
    active_role_triggered_user_ids: set[int] = set()
    try:
        room.assign_roles()
        active_role_triggered_user_ids = apply_room_active_role_buffs(room)
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

        async def notify_active_role_trigger(player) -> None:
            if player.user_id not in active_role_triggered_user_ids:
                return
            try:
                await asyncio.wait_for(
                    bot.send_message(
                        player.user_id,
                        "🎎 Твой баф «Активная роль» сработал в этой партии.",
                        **private_game_send_kwargs(room),
                    ),
                    timeout=8,
                )
            except Exception as e:
                print(f"[ERROR] notify_active_role_trigger({player_display_name(player)}): {e!r}")

        async def notify_active_role_failure(player) -> None:
            if player.user_id not in room.active_role_failed_user_ids:
                return
            try:
                await asyncio.wait_for(
                    bot.send_message(
                        player.user_id,
                        "🎲 Твой баф «Активная роль» в этой партии не сработал.",
                        **private_game_send_kwargs(room),
                    ),
                    timeout=8,
                )
            except Exception as e:
                print(f"[ERROR] notify_active_role_failure({player_display_name(player)}): {e!r}")

        try:
            results = await asyncio.gather(
                *(send_one_role_card(player) for player in room.players.values()),
                return_exceptions=False,
            )
        except Exception as e:
            print(f"[ERROR] send_role_cards gather: {e!r}")
            results = []

        await asyncio.gather(
            *(notify_active_role_trigger(player) for player in room.players.values()),
            return_exceptions=True,
        )

        await asyncio.gather(
            *(notify_active_role_failure(player) for player in room.players.values()),
            return_exceptions=True,
        )

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


async def maybe_launch_full_lobby(bot: Bot, room, chat_id: int, chat_title: str | None) -> bool:
    if room.started or not room.registration_open:
        return False
    if len(room.players) < MAX_PLAYERS:
        return False
    await launch_game_from_registration(bot, room, chat_id, chat_title)
    return True


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
        try:
            await notify_room_private_cancellation(
                bot,
                room,
                f"<b>Регистрация отменена.</b>\nНедостаточно игроков. Нужно минимум {MIN_PLAYERS}.",
            )
        except Exception as e:
            print(f"[ERROR] notify_room_private_cancellation(min players): {e!r}")
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
            parse_mode="HTML",
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


async def cmd_start(message: Message, command: CommandObject) -> None:
    is_private_first_visit = False
    nickname = ""
    keyboard = private_main_menu_keyboard(message.from_user.id if message.from_user is not None else None)
    if message.chat.type == "private":
        nickname = user_nickname(message.from_user)
        is_private_first_visit = repo.touch_private_user(
            message.from_user.id,
            nickname,
            message.from_user.username,
        )

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
            await message.answer(MSG_REGISTRATION_CLOSED)
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
        if await maybe_launch_full_lobby(message.bot, room, chat_id, room.chat_title):
            return
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
        try:
            await message.answer(
                text,
                reply_markup=keyboard,
            )
        except TelegramForbiddenError:
            return
        return


async def on_new_chat_members(message: Message) -> None:
    members = [member for member in (message.new_chat_members or []) if not member.is_bot]
    if not members:
        return
    for member in members:
        await send_group_welcome(message.bot, message.chat.id, member)


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


async def cmd_roles(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type == "private":
        await message.answer("Выберите роль:", reply_markup=private_roles_keyboard())
        return
    await message.answer(all_roles_info_text())


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
        await message.answer(MSG_GAME_ALREADY_RUNNING)
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
                parse_mode="HTML",
            )
            room.registration_message_id = sent.message_id
            persist_room(room)
            await pin_registration_post(message.bot, room)
        else:
            await refresh_registration_post(message, room)
            await pin_registration_post(message.bot, room)
        info_message = await message.answer(
            "Лобби уже создано. Регистрируйся в текущем сообщении."
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
        parse_mode="HTML",
    )
    room.registration_message_id = sent.message_id
    persist_room(room)
    await pin_registration_post(message.bot, room)
    await start_registration_timer(room, message.bot, int(room_chat_settings(room)["timings"]["registration"]))


async def cmd_join(message: Message) -> None:
    await cleanup_group_command_message(message)
    await message.answer("Вход в лобби только через inline-кнопку Зарегистрироваться под постом лобби.")


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

        leave_text = f"{room_player_mark(room, player)} не выдержал гнетущей атмосферы этого города и повесился."
        if show_roles_enabled(room):
            leave_text += f"\nОн был {role_mark_text(player.role)}"
        await message.answer(leave_text, parse_mode="HTML")

        try:
            await message.bot.send_message(player.user_id, "Ты вышел из игры", **private_game_send_kwargs(room))
        except Exception:
            pass

        if room.phase == PHASE_FINISHED:
            stats_already_recorded = room.stats_recorded
            ensure_stats_recorded(room)
            if not stats_already_recorded:
                await send_endgame_currency_summaries(message.bot, room)
            await message.answer(room.final_report_text(), parse_mode="HTML")
            cancel_phase_timer(message.chat.id)

        persist_room(room)
        return


async def cmd_lobby(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer(MSG_LOBBY_NOT_FOUND)
        return

    await message.answer(room.lobby_text(), parse_mode="HTML")


async def cmd_extend(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer(MSG_LOBBY_NOT_FOUND)
        return
    if room.started or not room.registration_open:
        await message.answer(MSG_REGISTRATION_CLOSED)
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


async def cmd_stop(message: Message) -> None:
    await cleanup_group_command_message(message)
    if message.chat.type == "private":
        await message.answer("Останавливать игру нужно в групповом чате.")
        return
    if message.from_user is None:
        return
    if not await is_group_settings_admin(message.bot, message.chat.id, message.from_user.id):
        await message.answer(MSG_ADMIN_REQUIRED)
        return

    room = storage.get_room(message.chat.id)
    if room is None:
        return

    if room.registration_open and not room.started:
        await notify_room_private_cancellation(
            message.bot,
            room,
            MSG_REGISTRATION_CANCELLED_ADMIN,
        )
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
        await message.answer(MSG_REGISTRATION_CANCELLED_ADMIN)
        return

    await notify_room_private_cancellation(
        message.bot,
        room,
        MSG_GAME_STOPPED_ADMIN,
    )
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
    await message.answer(MSG_GAME_STOPPED_ADMIN)


async def cmd_begin(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer(MSG_LOBBY_NOT_FOUND)
        return

    if not room.registration_open:
        await message.answer(MSG_REGISTRATION_CLOSED)
        return

    if len(room.players) < MIN_PLAYERS:
        await message.answer(f"Нужно минимум {MIN_PLAYERS} игрока(ов).")
        return

    await launch_game_from_registration(message.bot, room, message.chat.id, message.chat.title)


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
            parse_mode="HTML",
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
            await callback.answer("Сначала создай лобби через /game.", show_alert=True)
            return
        if not room.registration_open or room.started:
            await callback.answer(MSG_REGISTRATION_CLOSED, show_alert=True)
            return

        ok, info = room.add_player(callback.from_user.id, user_nickname(callback.from_user))
        if not ok:
            if info == "Ты уже в лобби.":
                persist_room(room)
                await refresh_registration_post(callback.message, room)
            await callback.answer(info, show_alert=True)
            return
        persist_room(room)
        await callback.answer("Ты зарегистрирован")
        if await maybe_launch_full_lobby(callback.bot, room, chat_id, callback.message.chat.title):
            return
        await refresh_registration_post(callback.message, room)
        return

    if action == "leave":
        if room is None:
            await callback.answer(MSG_LOBBY_NOT_FOUND, show_alert=True)
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
        if not await is_group_settings_admin(callback.bot, chat_id, callback.from_user.id):
            await callback.answer(
                MSG_ADMIN_REQUIRED,
                show_alert=True,
            )
            return
        if room.started:
            await callback.answer("Игра уже началась.", show_alert=True)
            return
        if room.registration_open:
            room.close_registration()
            persist_room(room)
        await notify_room_private_cancellation(
            callback.bot,
            room,
            MSG_GAME_CANCELLED_ADMIN,
        )
        await clear_registration_post(callback.bot, room)
        cancel_phase_timer(chat_id)
        cancel_registration_timer(chat_id)
        clear_chat_penalties(chat_id)
        clear_action_menu_messages(chat_id)
        remove_room_state(chat_id)
        storage.close_room(chat_id)
        await callback.message.answer(MSG_GAME_CANCELLED_ADMIN)
        await callback.answer("Игра отменена")
        return

    if action == "cancel":
        if not await is_group_settings_admin(callback.bot, chat_id, callback.from_user.id):
            await callback.answer(
                MSG_ADMIN_REQUIRED,
                show_alert=True,
            )
            return
        if room.registration_open:
            room.close_registration()
            persist_room(room)
        await notify_room_private_cancellation(
            callback.bot,
            room,
            MSG_REGISTRATION_CANCELLED_ADMIN,
        )
        await clear_registration_post(callback.bot, room)
        cancel_phase_timer(chat_id)
        cancel_registration_timer(chat_id)
        clear_chat_penalties(chat_id)
        clear_action_menu_messages(chat_id)
        remove_room_state(chat_id)
        storage.close_room(chat_id)
        await callback.message.answer(MSG_REGISTRATION_CANCELLED_ADMIN)
        await callback.answer("Отменено")
        return

    await callback.answer("Неизвестное действие", show_alert=True)


async def cmd_status(message: Message) -> None:
    await cleanup_group_command_message(message)
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer(MSG_LOBBY_NOT_FOUND)
        return

    await message.answer(room.status_text(), parse_mode="HTML")


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

