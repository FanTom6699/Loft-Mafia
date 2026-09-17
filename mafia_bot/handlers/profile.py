# Extracted from mafia_bot/handlers.py.
# Refactor only: gameplay behavior is unchanged.
from ._context import *  # noqa: F401,F403

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
    first_seen_at = str(stats.get("first_seen_at", "") or "")
    total_special_games = mafia_games + maniac_games + civilian_games
    registered_at_text = "-"
    if first_seen_at:
        try:
            registered_at_text = datetime.fromisoformat(first_seen_at).strftime("%d.%m.%Y %H:%M")
        except ValueError:
            registered_at_text = first_seen_at

    return (
        "<b>Твоя статистика</b>\n\n"
        f"👤 Игрок: <b>{escape(name)}</b>\n\n"
        f"🕒 Первая регистрация в боте: <b>{registered_at_text}</b>\n\n"
        f"🎮 Всего партий: <b>{games}</b>\n"
        f"🏆 Побед: <b>{wins}</b>\n"
        f"💀 Поражений: <b>{losses}</b>\n\n"
        f"🕴 За мафию: <b>{mafia_games}</b>\n"
        f"🔪 За маньяка: <b>{maniac_games}</b>\n"
        f"🙂 За мирных: <b>{civilian_games}</b>\n\n"
        f"📚 Учтено партий по ролям: <b>{total_special_games}</b>"
    )


def top_period_keyboard(selected_metric: str, selected_period: str, owner_user_id: int | None = None) -> InlineKeyboardMarkup:
    def callback_value(metric: str, period: str) -> str:
        if owner_user_id is None:
            return f"top:{metric}:{period}"
        return f"top:{metric}:{period}:{owner_user_id}"

    def button(period: str) -> InlineKeyboardButton:
        prefix = "✅ " if selected_metric == "wins" and period == selected_period else ""
        metric_for_period = "wins" if selected_metric == "tickets" else selected_metric
        return InlineKeyboardButton(
            text=f"{prefix}{TOP_PERIOD_LABELS[period]}",
            callback_data=callback_value(metric_for_period, period),
        )
    ticket_prefix = "✅ " if selected_metric == "tickets" else ""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [button("day"), button("week")],
            [button("month"), button("all")],
            [
                InlineKeyboardButton(
                    text=f"{ticket_prefix}По билетикам",
                    callback_data=callback_value("tickets", "all"),
                )
            ],
        ]
    )


def format_top_text(metric: str, period: str, rows: list[dict]) -> str:
    period_name = TOP_PERIOD_LABELS.get(period, TOP_PERIOD_LABELS["all"])
    metric_name = "По билетикам" if metric == "tickets" else "По победам"
    lines = ["<b>🏆 Топ-10 игроков</b>", "", f"Рейтинг: <b>{metric_name}</b>", f"Период: <b>{period_name}</b>"]
    if not rows:
        lines.extend(["", "Пока нет данных для этого топа."])
        return "\n".join(lines)

    lines.append("")
    for index, row in enumerate(rows, start=1):
        user_id = int(row.get("user_id", 0))
        display_name = str(row.get("display_name", "") or f"Игрок {user_id}")
        player_mark = user_profile_link_by_id(user_id, display_name)
        wins = int(row.get("wins", 0))
        tickets = int(row.get("tickets", 0))
        if metric == "tickets":
            lines.append(f"{index}. {player_mark} — <b>{tickets}</b> 🎟")
        else:
            lines.append(f"{index}. {player_mark} — <b>{wins}</b> побед")

    return "\n".join(lines)


def format_endgame_currency_text(player, stats: dict, reward_awarded: bool) -> str:
    name = escape((player.full_name or "").strip() or f"Игрок {player.user_id}")
    money = int(stats.get("money", 0))
    tickets = int(stats.get("tickets", 0))
    buff_documents = int(stats.get("buff_documents", 0))
    buff_shield = int(stats.get("buff_shield", 0))
    buff_active_role = int(stats.get("buff_active_role", 0))
    if reward_awarded:
        return (
            "<b>Игра завершена</b>\n"
            f'За победу в роли "{escape(player.role)}" тебе начислили 💵 10!\n\n'
            f"👤 {name}\n\n"
            f"💵 Деньги: {money}\n"
            f"🎟 Билетики: {tickets}\n\n"
            f"🛡 Защита: {buff_shield}\n"
            f"📁 Документы: {buff_documents}\n"
            f"🎎 Активная роль: {buff_active_role}"
        )
    return (
        "<b>Игра завершена</b>\n\n"
        f"👤 {name}\n\n"
        f"💵 Деньги: {money}\n"
        f"🎟 Билетики: {tickets}\n\n"
        f"🛡 Защита: {buff_shield}\n"
        f"📁 Документы: {buff_documents}\n"
            f"🎎 Активная роль: {buff_active_role}"
    )


def format_private_profile_text(display_name: str, stats: dict | None) -> str:
    safe_name = escape((display_name or "").strip() or "Игрок")
    money = int((stats or {}).get("money", 0))
    tickets = int((stats or {}).get("tickets", 0))
    buff_documents = int((stats or {}).get("buff_documents", 0))
    buff_shield = int((stats or {}).get("buff_shield", 0))
    buff_active_role = int((stats or {}).get("buff_active_role", 0))

    return (
        "<b>Игровой профиль</b>\n\n"
        f"👤 <b>{safe_name}</b>\n\n"
        f"💵 Деньги: <b>{money}</b>\n"
        f"🎟 Билетики: <b>{tickets}</b>\n\n"
        "<b>🚀 Бафы</b>\n"
        f"📂 Документы: <b>{buff_documents}</b>\n"
        f"🛡 Защита: <b>{buff_shield}</b>\n"
        f"🎎 Активная роль: <b>{buff_active_role}</b>"
    )


def format_buffs_shop_text() -> str:
    return (
        "<b>Что будем покупать?</b>\n\n"
        "📂 <b>Документы</b>\n"
        f"{BUFF_CATALOG['documents']['description']}\n\n"
        "🛡 <b>Защита</b>\n"
        f"{BUFF_CATALOG['shield']['description']}\n\n"
        "🎎 <b>Активная роль</b>\n"
        f"{BUFF_CATALOG['active_role']['description']}"
    )


def format_buff_details_text(key: str, stats: dict | None) -> str:
    item = BUFF_CATALOG[key]
    inventory_key = str(item["inventory_key"])
    owned = int((stats or {}).get(inventory_key, 0))
    status_line = ""
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
        elif key == "documents":
            if user_id in room.spent_documents_user_ids:
                status_line = "📂 На эту игру документы уже были использованы.\n\n"
            elif user_id in room.documented_user_ids:
                status_line = "✨ Документы активны в текущей игре.\n\n"
            elif owned > 0:
                status_line = "⏳ Купленные во время этой игры документы сработают только в следующей партии.\n\n"
        elif key == "active_role":
            if not buffs_enabled(room):
                status_line = "🚫 В этой игре бафы выключены, поэтому Активная роль не сработает.\n\n"
            elif user_id in room.active_role_triggered_user_ids:
                status_line = "✨ Активная роль уже сработала в текущей игре.\n\n"
            elif user_id in room.active_role_failed_user_ids:
                status_line = "🎲 В этой игре баф не сработал. Если он не был потрачен, то остался в инвентаре.\n\n"
            elif user_id in room.active_role_queued_user_ids:
                status_line = "⏳ Купленная во время этой игры Активная роль сработает только в следующей партии.\n\n"
    elif key == "active_role" and owned > 0:
        status_line = "🎟 Баф ждёт следующую новую партию.\n\n"
    return (
        f"<b>{item['title']}</b>\n\n"
        f"{item['description']}\n\n"
        f"💰 Цена: <b>{item['price']}</b>\n"
        f"🎒 В инвентаре: <b>{owned}</b>\n\n"
        f"{status_line}"
        f"{item['details']}"
    )


def private_main_menu_keyboard(user_id: int | None = None) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="👤 Игровой профиль", callback_data="pmenu:profile"),
            InlineKeyboardButton(text="🎭 Роли", callback_data="pmenu:roles"),
        ],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="pmenu:stats")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
            [InlineKeyboardButton(text="🎎 Активная роль - 🎟10", callback_data="pmenu:buff:active_role")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="pmenu:profile")],
        ]
    )


def private_buff_details_keyboard(key: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    item = BUFF_CATALOG[key]
    rows.append([InlineKeyboardButton(text=f"Купить за {item['price']}", callback_data=f"pmenu:buy:{key}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="pmenu:buffs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


async def send_top_to_private(bot: Bot, user: User, metric: str = "wins", period: str = "all") -> bool:
    nickname = user_nickname(user)
    repo.touch_private_user(user.id, nickname, user.username)
    top_rows = repo.get_top_players(period=period, limit=10, metric=metric)
    try:
        await bot.send_message(
            user.id,
            (
                f"<b>С возвращением, {nickname}!</b>\n\n"
                "Выбери нужный раздел кнопками ниже."
            ),
            reply_markup=private_main_menu_keyboard(user.id),
        )
        await bot.send_message(
            user.id,
            format_top_text(metric, period, top_rows),
            reply_markup=top_period_keyboard(metric, period, owner_user_id=user.id),
            parse_mode="HTML",
        )
    except TelegramForbiddenError:
        return False
    return True


@router.message(Command("top"))
async def cmd_top(message: Message) -> None:
    await cleanup_group_command_message(message)
    metric = "wins"
    period = "all"

    if message.chat.type in {"group", "supergroup"} and message.from_user is not None:
        room = storage.get_room(message.chat.id)
        if room is not None and room.started and room.phase != PHASE_FINISHED:
            sent = await send_top_to_private(message.bot, message.from_user, metric=metric, period=period)
            if sent:
                return
            else:
                start_link = await bot_start_link(message.bot)
                await message.answer(
                    "Во время игры топ доступен только в ЛС бота. Напиши боту /start и попробуй снова.",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="Открыть бота", url=start_link)],
                        ]
                    ),
                )
            return

    top_rows = repo.get_top_players(period=period, limit=10, metric=metric)
    await message.answer(
        format_top_text(metric, period, top_rows),
        reply_markup=top_period_keyboard(metric, period, owner_user_id=message.from_user.id if message.from_user is not None else None),
        parse_mode="HTML",
    )


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
