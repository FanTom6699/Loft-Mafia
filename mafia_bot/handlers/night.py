# Extracted from mafia_bot/handlers.py.
# Refactor only: gameplay behavior is unchanged.
from ._context import *  # noqa: F401,F403

async def night_action_keyboard(bot: Bot) -> InlineKeyboardMarkup:
    link = await private_bot_link(bot)
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Посмотреть роль", url=link)]],
    )


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
        return None

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
        base_name = room_player_label(room, target)
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
            skip_text = skip_turn_button_text(selected_target_id == 0)
            rows.append(
                [
                    InlineKeyboardButton(
                        text=skip_text,
                        callback_data=f"act:skipvote:{room.chat_id}:0",
                    )
                ]
            )

    if room.phase == "night" and rows and night_action_skip_enabled(room):
        skip_text = skip_turn_button_text(selected_target_id == 0)
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
            return DAY_SILENCED_VOTE_TEXT
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
    if invisible_mode_enabled(room):
        for player in sorted(alive, key=lambda p: seat_positions.get(p.user_id, 10**9)):
            seat_no = seat_positions.get(player.user_id)
            if seat_no is None:
                lines.append(room.anonymous_player_label(player))
            else:
                lines.append(f"{seat_no}. {room.anonymous_player_label(player)}")

        lines.append(f"\n<b>Спать осталось {format_sleep_left(int(room_chat_settings(room)['timings']['night']))}</b>")
        return "\n".join(lines)

    for player in sorted(alive, key=lambda p: seat_positions.get(p.user_id, 10**9)):
        seat_no = seat_positions.get(player.user_id)
        raw_name = (player.full_name or "").strip()
        fallback_name = f"Игрок {seat_no}" if seat_no is not None else f"Игрок {player.user_id}"
        normalized_name = normalize_link_display_name(raw_name, fallback_name)
        safe_name = escape(normalized_name)
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
        actor = room.get_player(message.from_user.id)
        if (
            actor is not None
            and actor.alive
            and room.phase == PHASE_DAY
            and room.day_stage == DAY_STAGE_NOMINATION
            and room.day_silenced_user_id == actor.user_id
        ):
            await message.answer(DAY_SILENCED_VOTE_TEXT, **private_game_send_kwargs(room))
            return
        await message.answer("Сейчас у твоей роли нет доступных действий.", **private_game_send_kwargs(room))
        return

    prompt_text = build_action_prompt_text(room, message.from_user.id)
    sent = await message.answer(prompt_text, reply_markup=keyboard, **private_game_send_kwargs(room))
    track_action_menu_message(room.chat_id, message.from_user.id, sent.message_id)


async def push_phase_action_menus(bot: Bot, room) -> None:
    for player in room.alive_players():
        keyboard = build_action_keyboard(room, player.user_id)
        if keyboard is None:
            if (
                room.phase == PHASE_DAY
                and room.day_stage == DAY_STAGE_NOMINATION
                and room.day_silenced_user_id == player.user_id
            ):
                try:
                    await bot.send_message(
                        player.user_id,
                        DAY_SILENCED_VOTE_TEXT,
                        **private_game_send_kwargs(room),
                    )
                except Exception:
                    pass
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
            failure_mark = room_player_mark(room, player)
            await bot.send_message(
                room.chat_id,
                f"Не смог отправить меню хода игроку {failure_mark}."
                " Пусть напишет боту /start в личке.",
                parse_mode="HTML",
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


async def send_mafia_private_update(room, bot, text: str) -> None:
    if len(room.alive_mafia()) <= 1:
        return

    for player in room.alive_players():
        if player.role not in {ROLE_DON, ROLE_MAFIA}:
            continue
        try:
            await bot.send_message(
                player.user_id,
                text,
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


def sergeant_commissar_check_text(room, target_user_id: int, result_role: str) -> str:
    checked_player = room.get_player(target_user_id)
    if checked_player is None:
        checked_mark = f"Игрок {target_user_id}"
    else:
        checked_mark = room_player_mark(room, checked_player)
    return f"🕵️‍ Комиссар Каттани проверил {checked_mark}. Он - {role_mark_text(result_role)}"


def mafia_allies_text(room) -> str:
    allies = [player for player in room.players.values() if player.role in {ROLE_DON, ROLE_MAFIA}]
    if len(allies) <= 1:
        return ""

    lines = ["", "<b>Запомни своих союзников:</b>"]
    for ally in allies:
        role_mark = role_mark_text(ally.role)
        lines.append(f"  {room_player_mark(room, ally)} - {role_mark}")
    return "\n".join(lines)


def city_power_allies_text(room, role: str) -> str:
    lines: list[str] = []
    if role == ROLE_SERGEANT:
        commissar = next((player for player in room.players.values() if player.role == ROLE_COMMISSAR), None)
        if commissar is not None:
            lines.append(f"     {room_player_mark(room, commissar)} - 🕵️‍ Комиссар Каттани")
    elif role == ROLE_COMMISSAR:
        sergeant = next((player for player in room.players.values() if player.role == ROLE_SERGEANT), None)
        if sergeant is not None:
            lines.append(f"     {room_player_mark(room, sergeant)} - 👮🏼‍♂️ Сержант")

    if not lines:
        return ""

    return "\n\n<b>Запомни своих союзников:</b>\n" + "\n".join(lines)


@router.message(Command("action"))
async def cmd_action(message: Message) -> None:
    await cleanup_group_command_message(message)
    await message.answer("Меню хода отправляется автоматически при старте каждой фазы.")


@router.callback_query(F.data.startswith("act:"))
async def on_action_callback(callback: CallbackQuery) -> None:
    if callback.message is None or callback.from_user is None:
        return

    if callback.message.chat.type != "private":
        await callback.answer("Игровые действия доступны только в ЛС бота.", show_alert=True)
        return

    parts = callback.data.split(":")
    if len(parts) != 4:
        await callback.answer(MSG_INVALID_ACTION, show_alert=True)
        return

    _, action, raw_chat_id, raw_target_id = parts
    try:
        chat_id = int(raw_chat_id)
        target_id = int(raw_target_id)
    except ValueError:
        await callback.answer("Некорректные параметры. Проверь и попробуй снова.", show_alert=True)
        return

    room = storage.get_room(chat_id)
    if room is None:
        await callback.answer("Игра не найдена. Дождись следующей партии.", show_alert=True)
        return

    actor = room.get_player(callback.from_user.id)
    if actor is None:
        await callback.answer(MSG_NOT_IN_GAME)
        return
    if not actor.alive:
        is_kamikaze_revenge = (
            room.phase == PHASE_NIGHT
            and actor.role == ROLE_KAMIKAZE
            and room.kamikaze_pending_user_id == actor.user_id
        )
        if not is_kamikaze_revenge:
            await callback.answer(MSG_NOT_IN_GAME)
            return

    if room.phase == PHASE_NIGHT and callback.from_user.id in getattr(room, "night_skipped_user_ids", set()):
        await callback.answer("Выбор уже зафиксирован до конца ночи.", show_alert=True)
        return

    async def announce_night_role_once(role_name: str, target=None, *, variant: str = "default") -> None:
        if room.phase != "night":
            return
        announcement_role = ROLE_MAFIA if role_name in MAFIA_ROLES else role_name
        if announcement_role == ROLE_MAFIA and room.mafia_target_announced:
            return
        if not room.mark_night_role_announced(announcement_role):
            return
        if announcement_role == ROLE_MAFIA:
            room.mafia_target_announced = True
        announcement_text = night_role_announcement_text(room, announcement_role, target, variant=variant)
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
            await announce_night_role_once(ROLE_MAFIA)
            if actor is not None and target is not None:
                role_mark = role_mark_text(actor.role)
                await send_mafia_private_update(
                    room,
                    callback.bot,
                    f"{role_mark} {room_player_mark(room, actor)} проголосовал за {room_player_mark(room, target)}",
                )

            if room.mafia_vote_locked:
                final_target_id = room.current_mafia_target_id()
                final_target = room.get_player(final_target_id) if final_target_id is not None else None
                final_name = room_player_mark(room, final_target) if final_target is not None else "цель"
                await send_mafia_private_update(
                    room,
                    callback.bot,
                    "Голосование мафии завершено\n"
                    f"Мафия принесла в жертву {final_name}.",
                )
            if target is not None:
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
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
                selected_name = room_player_label(room, target)
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
                selected_name = room_player_label(room, target)
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "кандидата"
                selected_user_id = None

            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
            )

            if voter is not None and target is not None:
                if not is_secret_voting_enabled(room):
                    await callback.bot.send_message(
                        room.chat_id,
                        f"{room_player_mark(room, voter)} проголосовал за {room_player_mark(room, target)}",
                        parse_mode="HTML",
                    )
                else:
                    await callback.bot.send_message(
                        room.chat_id,
                        f"🗳 {room_player_mark(room, voter)} проголосовал.",
                        parse_mode="HTML",
                    )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        else:
            if info == MISTRESS_DAY_BLOCK_TOAST:
                await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
                return
            await callback.answer(info, show_alert=True)
        return

    if action == "skipvote":
        if room.phase == PHASE_DAY and room.day_stage == DAY_STAGE_TRIAL:
            await callback.answer("Сейчас идет повешение: можно только поставить 👍 или 👎 в чате.", show_alert=True)
            return
        if room.phase != PHASE_DAY or room.day_stage != DAY_STAGE_NOMINATION:
            await callback.answer("Сейчас не этап выбора кандидата. Дождись начала голосования.", show_alert=True)
            return
        if not day_vote_skip_enabled(room):
            await callback.answer("Пропуск дневного голосования отключен в настройках.", show_alert=True)
            return

        if callback.from_user.id in room.day_votes:
            await callback.answer("Выбор уже зафиксирован до конца голосования.", show_alert=True)
            return

        voter = room.get_player(callback.from_user.id)
        if voter is None or not voter.alive:
            await callback.answer(MSG_NOT_IN_GAME)
            return
        if room.day_silenced_user_id is not None and voter.user_id == room.day_silenced_user_id:
            await callback.answer(MISTRESS_DAY_BLOCK_TOAST)
            return

        room.day_votes[callback.from_user.id] = 0
        await callback.answer("🚷 Пропуск голосования принят.")
        await callback.message.edit_text(
            build_action_prompt_text(room, callback.from_user.id) + f"\n\n{skip_turn_selected_text()}",
            reply_markup=None,
        )
        if not is_secret_voting_enabled(room):
            await callback.bot.send_message(
                room.chat_id,
                f"🚷 {room_player_mark(room, voter)} пропускает голосование.",
                parse_mode="HTML",
            )
        else:
            await callback.bot.send_message(
                room.chat_id,
                f"🚷 {room_player_mark(room, voter)} пропускает голосование.",
                parse_mode="HTML",
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
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
                selected_name = room_player_label(room, target)
                selected_user_id = target.user_id
            else:
                selected_name = "цель"
                selected_user_id = None
            await callback.message.edit_text(
                locked_choice_text(room, callback.from_user.id, selected_name, selected_user_id),
                parse_mode="HTML",
                reply_markup=None,
            )
            await maybe_finish_phase_early(callback.bot, room)
            persist_room(room)
        return

    await callback.answer("Неизвестный тип действия.", show_alert=True)
