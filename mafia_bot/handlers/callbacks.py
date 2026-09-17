# Extracted from mafia_bot/handlers.py.
# Refactor only: gameplay behavior is unchanged.
from ._context import *  # noqa: F401,F403

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
            await callback.answer("🚷 Некорректный пропуск хода.", show_alert=True)
            return
        room = storage.get_room(chat_id)
        if room is None or room.phase != PHASE_NIGHT:
            await callback.answer("🚷 Сейчас нельзя пропустить ход.", show_alert=True)
            return
        if callback.message.chat.type != "private":
            await callback.answer("🚷 Пропуск ночного хода доступен только в ЛС бота.", show_alert=True)
            return
        if not night_action_skip_enabled(room):
            await callback.answer("🚷 Пропуск ночного хода отключен в настройках.", show_alert=True)
            return
        ok, info = room.set_night_skip(callback.from_user.id)
        await callback.answer(info, show_alert=not ok)
        if not ok:
            return
        try:
            await callback.message.edit_text(
                build_action_prompt_text(room, callback.from_user.id) + f"\n\n{skip_turn_selected_text()}",
                reply_markup=None,
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
            private_main_menu_keyboard(callback.from_user.id),
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
        shop_text = format_buffs_shop_text()
        room = get_player_profile_room(callback.from_user.id)
        if room is not None and not buffs_enabled(room):
            shop_text += "\n\n🚫 В текущей игре бафы выключены. Купленные сейчас бафы сработают только в новой партии после включения бафов."
        await show_menu_screen(shop_text, private_buffs_shop_keyboard())
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
            room = get_player_profile_room(callback.from_user.id)
            success_text = f"Приобретено: {item['success_name']}"
            if room is not None and room.get_player(callback.from_user.id) is not None:
                if str(item["inventory_key"]) == "buff_active_role":
                    room.active_role_queued_user_ids.add(callback.from_user.id)
                    persist_room(room)
                    success_text = "Приобретено: Активная роль. В текущей игре баф сработает только в следующей партии."
            await show_menu_screen(
                format_private_profile_text(user_nickname(callback.from_user), stats),
                private_profile_keyboard(),
            )
            await safe_answer(success_text)
            return
        await safe_answer(info, show_alert=True)
        return
    await safe_answer("Неизвестный пункт меню.", show_alert=True)


@router.callback_query(F.data.startswith("top:"))
async def on_top_callback(callback: CallbackQuery) -> None:
    if callback.message is None:
        return

    parts = callback.data.split(":")
    if len(parts) == 2:
        metric = "wins"
        period = parts[1]
        owner_user_id = None
    elif len(parts) == 3:
        _, metric, period = parts
        owner_user_id = None
    elif len(parts) == 4:
        _, metric, period, owner_raw = parts
        try:
            owner_user_id = int(owner_raw)
        except ValueError:
            await callback.answer("Неизвестный топ.", show_alert=True)
            return
    else:
        await callback.answer("Неизвестный топ.", show_alert=True)
        return

    if owner_user_id is not None and callback.from_user is not None and callback.from_user.id != owner_user_id:
        await callback.answer("Это меню не для тебя.")
        return

    if callback.message.chat.type in {"group", "supergroup"} and callback.from_user is not None:
        room = storage.get_room(callback.message.chat.id)
        if room is not None and room.started and room.phase != PHASE_FINISHED:
            sent = await send_top_to_private(callback.message.bot, callback.from_user, metric="wins", period="all")
            if sent:
                await callback.answer("Во время игры топ отправлен в ЛС бота.", show_alert=True)
            else:
                await callback.answer("Во время игры топ доступен только в ЛС бота. Напиши боту /start.", show_alert=True)
            return

    if metric not in {"wins", "tickets"}:
        await callback.answer("Неизвестный тип топа.", show_alert=True)
        return
    if period not in TOP_PERIOD_LABELS:
        await callback.answer("Неизвестный период.", show_alert=True)
        return

    if metric == "tickets":
        period = "all"

    rows = repo.get_top_players(period=period, limit=10, metric=metric)
    try:
        keyboard_owner_id = owner_user_id
        if keyboard_owner_id is None and callback.from_user is not None:
            keyboard_owner_id = callback.from_user.id
        await callback.message.edit_text(
            format_top_text(metric, period, rows),
            reply_markup=top_period_keyboard(metric, period, owner_user_id=keyboard_owner_id),
            parse_mode="HTML",
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    await callback.answer()


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
        safe_name = escape(normalize_link_display_name(raw_name or "", f"Игрок {message.from_user.id}"))
        player_mark = f"<a href=\"tg://user?id={message.from_user.id}\">{safe_name}</a>"
        safe_payload = escape(payload)
        await message.answer("Предсмертное сообщение принято.", **private_game_send_kwargs(last_word_room))
        public_prefix = f"Кто-то из жителей слышал, как {room_player_mark(last_word_room, player) if player is not None else player_mark} кричал перед смертью:\n"
        await message.bot.send_message(
            last_word_room.chat_id,
            f"{public_prefix}<b>{safe_payload}</b>",
            parse_mode="HTML",
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

    relay_author = room_player_mark(room, actor)
    relay_text = f"{relay_author}:\n{text}"
    for teammate in room.alive_players():
        if teammate.role not in teammate_roles:
            continue
        if teammate.user_id == actor.user_id:
            continue
        try:
            await message.bot.send_message(
                teammate.user_id,
                relay_text,
                **private_game_send_kwargs(room),
            )
        except Exception:
            continue
