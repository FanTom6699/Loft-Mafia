# Extracted from mafia_bot/handlers.py.
# Refactor only: gameplay behavior is unchanged.
from ._context import *  # noqa: F401,F403

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


def room_player_mark(room, player, *, clickable: bool = True) -> str:
    if invisible_mode_enabled(room):
        return escape(room.anonymous_player_label(player))
    if clickable:
        return player_profile_link(player)
    return escape(player_display_name(player))


def room_player_label(room, player) -> str:
    if invisible_mode_enabled(room):
        return room.anonymous_player_label(player)
    return player_display_name(player)


def private_game_send_kwargs(room) -> dict:
    return {"parse_mode": "HTML"}


def night_role_announcement_text(room, role_name: str, target=None, *, variant: str = "default") -> str:
    if show_targets_enabled(room) and target is not None:
        target_mark = room_player_mark(room, target)
        targeted_announcements = {
            ROLE_COMMISSAR: {
                "default": f"<b>🕵️ Комиссар Каттани</b> проверяет {target_mark}.",
                "shoot": f"<b>🕵️ Комиссар Каттани</b> стреляет в {target_mark}.",
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
                    text=skip_turn_button_text(),
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


def skip_turn_button_text(selected: bool = False) -> str:
    return "✅ 🚷 Пропустить ход" if selected else "🚷 Пропустить ход"


def skip_turn_selected_text() -> str:
    return "Ты выбрал 🚷 Пропуск хода"


def locked_choice_text(room, actor_user_id: int, selected_name: str, selected_user_id: int | None = None) -> str:
    prompt = build_action_prompt_text(room, actor_user_id)
    normalized_name = normalize_link_display_name(selected_name, "цель")
    safe_name = escape(normalized_name)
    selected_mark = safe_name
    if selected_user_id is not None and not invisible_mode_enabled(room):
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


async def mark_skipped_night_menus(bot: Bot, room, skipped_user_ids: list[int]) -> None:
    if not skipped_user_ids:
        return

    keyboard = skipped_turn_keyboard(room.chat_id)
    for user_id in skipped_user_ids:
        message_id = get_action_menu_message_id(room.chat_id, user_id)
        if message_id is None:
            continue
        skipped_text = f"{build_action_prompt_text(room, user_id)}\n\nВремя вышло, ты опоздал с ходом."
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


def ensure_stats_recorded(room) -> None:
    if room.phase != PHASE_FINISHED:
        return
    if room.stats_recorded:
        return
    repo.record_finished_game_stats(room)
    room.stats_recorded = True
    persist_room(room)


def prime_room_documents(room) -> None:
    room.documented_user_ids.clear()
    room.spent_documents_user_ids.clear()
    if not buffs_enabled(room):
        return
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None:
            continue
        if int(stats.get("buff_documents", 0)) > 0:
            room.arm_documents(player.user_id)


def prime_room_shields(room) -> None:
    room.shielded_user_ids.clear()
    room.spent_shield_user_ids.clear()
    if not buffs_enabled(room):
        return
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None:
            continue
        if int(stats.get("buff_shield", 0)) > 0:
            room.arm_shield(player.user_id)


def apply_room_active_role_buffs(room) -> set[int]:
    room.active_role_queued_user_ids.clear()
    room.active_role_triggered_user_ids.clear()
    room.active_role_failed_user_ids.clear()

    if not buffs_enabled(room):
        return set()

    buff_user_ids: set[int] = set()
    designated_players: list = []
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None or int(stats.get("buff_active_role", 0)) <= 0:
            continue

        buff_user_ids.add(player.user_id)

        if random.random() < 0.99:
            designated_players.append(player)

    room.active_role_queued_user_ids = set(buff_user_ids)

    if not designated_players:
        room.active_role_failed_user_ids = set(buff_user_ids)
        return set()

    available_active_slots = sum(1 for player in room.players.values() if player.role != ROLE_CITIZEN)
    random.shuffle(designated_players)
    winners = designated_players[:available_active_slots]
    winner_user_ids = {player.user_id for player in winners}

    satisfied_user_ids = {
        player.user_id
        for player in room.players.values()
        if player.user_id in winner_user_ids and player.role != ROLE_CITIZEN
    }

    receivers = [
        player
        for player in room.players.values()
        if player.user_id in winner_user_ids and player.role == ROLE_CITIZEN
    ]
    donors = [
        player
        for player in room.players.values()
        if player.user_id not in winner_user_ids and player.role != ROLE_CITIZEN
    ]

    random.shuffle(receivers)
    random.shuffle(donors)

    for receiver, donor in zip(receivers, donors):
        receiver.role, donor.role = donor.role, receiver.role
        satisfied_user_ids.add(receiver.user_id)

    for user_id in satisfied_user_ids:
        repo.consume_buff(user_id, inventory_column="buff_active_role")

    room.active_role_triggered_user_ids = set(satisfied_user_ids)
    room.active_role_failed_user_ids = set(buff_user_ids) - set(satisfied_user_ids)

    return satisfied_user_ids


async def send_endgame_currency_summaries(bot: Bot, room) -> None:
    for player in room.players.values():
        stats = repo.get_player_stats(player.user_id)
        if stats is None:
            continue
        won = False
        if room.winner_team == "Мафия":
            won = player.role in MAFIA_ROLES or player.role == ROLE_ADVOCATE
        elif room.winner_team == "Маньяк":
            won = player.role == ROLE_MANIAC
        elif room.winner_team == "Мирные жители":
            won = player.role not in MAFIA_ROLES and player.role != ROLE_MANIAC and player.role != ROLE_ADVOCATE
        reward_awarded = won and player.alive
        try:
            await bot.send_message(
                player.user_id,
                format_endgame_currency_text(player, stats, reward_awarded),
                reply_markup=private_profile_keyboard(),
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
    if user_id == OWNER_USER_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in {"administrator", "creator"}


async def is_group_settings_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    if user_id == OWNER_USER_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False

    if member.status == "creator":
        return True
    if member.status != "administrator":
        return False
    return bool(getattr(member, "can_change_info", False))


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


def normalize_link_display_name(name: str, fallback: str) -> str:
    normalized = str(name or "")
    normalized = normalized.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    # Remove control/format characters that may break Telegram HTML links.
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch)[0] != "C")
    normalized = " ".join(normalized.split())
    return normalized or fallback


def player_profile_link(player) -> str:
    display_name = normalize_link_display_name(player_display_name(player), f"Игрок {player.user_id}")
    safe_name = escape(display_name)
    return f"<a href=\"tg://user?id={player.user_id}\">{safe_name}</a>"


def user_profile_link_by_id(user_id: int, display_name: str) -> str:
    normalized_name = normalize_link_display_name(display_name, f"Игрок {user_id}")
    safe_name = escape(normalized_name)
    return f"<a href=\"tg://user?id={user_id}\">{safe_name}</a>"


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


async def notify_room_private_cancellation(bot: Bot, room, text: str) -> None:
    for player in room.players.values():
        try:
            await bot.send_message(
                player.user_id,
                text,
                **private_game_send_kwargs(room),
            )
        except Exception:
            continue


async def announce_don_transfer(room, bot: Bot, don_successor_id: int | None) -> None:
    if don_successor_id is None:
        return

    new_don = room.get_player(don_successor_id)
    if new_don is None:
        return

    don_name = room_player_mark(room, new_don)
    await bot.send_message(
        room.chat_id,
        "Мафия унаследовала роль <b>🤵🏻 Дон</b>",
        parse_mode="HTML",
    )
    try:
        await bot.send_message(
            don_successor_id,
            "Теперь ты 🤵🏻 Дон",
            **private_game_send_kwargs(room),
        )
    except Exception:
        pass
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
        display_name = escape(normalize_link_display_name(owner_chat.full_name or "", display_name))
    except Exception:
        pass

    dev_link = f"<a href=\"tg://user?id={OWNER_USER_ID}\">{display_name}</a>"
    await message.reply(f"Это {dev_link}", parse_mode="HTML")


@router.message(
    F.chat.type.in_({"group", "supergroup"}),
    ~F.text.startswith("/"),
)
async def enforce_group_game_rules(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None or not room.started or room.phase == PHASE_FINISHED:
        return

    if message.from_user is None or message.from_user.is_bot:
        return

    lowered_text = (message.text or "").strip().lower()
    if lowered_text.startswith("!передать") or lowered_text.startswith("!забрать") or lowered_text.startswith("!начислить"):
        return

    if message.text and message.text.startswith("!"):
        if await is_group_admin(message.bot, message.chat.id, message.from_user.id) or is_ticket_manager_user_id(message.from_user.id):
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


# Unclassified handlers retained here for compatibility.

def read_phase_seconds(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


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


def registration_panel(panel_message_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Создать лобби", callback_data=f"reg:start:{panel_message_id}"),
            ],
        ]
    )

