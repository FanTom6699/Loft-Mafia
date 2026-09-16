# Extracted from the original mafia_bot/handlers.py.
# Stage 1 refactor: gameplay behavior is intentionally unchanged.
from ._context import *  # noqa: F401,F403

def is_ticket_manager_user_id(user_id: int) -> bool:
    return user_id in TICKET_MANAGER_USER_IDS


def ticket_command_usage_text(command_name: str) -> str:
    return f"Использование: {command_name} <число> [@username|ID] или ответом на сообщение."


def ticket_command_hint_text(text: str | None) -> str | None:
    command_token = ((text or "").strip().split(maxsplit=1) or [""])[0].lower()
    if not command_token.startswith("!"):
        return None

    if command_token.startswith("!перед"):
        return ticket_command_usage_text("!передать")
    if command_token.startswith("!забр"):
        return ticket_command_usage_text("!забрать")
    if command_token.startswith("!начис"):
        return ticket_command_usage_text("!начислить")
    return None


async def resolve_ticket_command_target(message: Message, raw_target: str | None) -> tuple[int | None, str | None, str | None]:
    replied = message.reply_to_message
    if replied is not None and replied.from_user is not None and not replied.from_user.is_bot:
        target_user = replied.from_user
        if not repo.has_private_user(target_user.id):
            return None, None, "Не смог найти такого пользователя в боте."
        return target_user.id, user_nickname(target_user), None

    candidate = (raw_target or "").strip()
    if not candidate:
        return None, None, "Укажи пользователя через ответ на сообщение, @username или ID."

    if candidate.startswith("@"):
        private_user = repo.get_private_user_by_username(candidate)
        if private_user is None:
            return None, None, "Не смог найти такого пользователя в боте."

        target_id = int(private_user["user_id"])
        display_name = str(private_user.get("display_name", "") or candidate)
        return target_id, display_name, None

    try:
        target_id = int(candidate)
    except ValueError:
        return None, None, "Некорректный пользователь. Используй reply, @username или ID."

    try:
        if message.chat.type in {"group", "supergroup"}:
            member = await message.bot.get_chat_member(message.chat.id, target_id)
            target_user = member.user
            if target_user.is_bot:
                return None, None, "Нельзя использовать команду для бота."
            if not repo.has_private_user(target_id):
                return None, None, "Не смог найти такого пользователя в боте."
            return target_id, user_nickname(target_user), None
    except Exception:
        pass

    if not repo.has_private_user(target_id):
        return None, None, "Не смог найти такого пользователя в боте."

    stats = repo.get_player_stats(target_id)
    if stats is not None:
        display_name = str(stats.get("display_name", "") or f"Игрок {target_id}")
        return target_id, display_name, None

    return target_id, f"Игрок {target_id}", None


async def handle_ticket_adjustment_command(message: Message, *, action: str) -> None:
    if message.from_user is None:
        return
    if action in {"take", "grant"} and not is_ticket_manager_user_id(message.from_user.id):
        return

    parts = (message.text or "").strip().split(maxsplit=2)
    command_name = parts[0].lower() if parts else ""
    usage = ticket_command_usage_text(command_name)

    if len(parts) < 2:
        await message.reply(usage)
        return

    try:
        amount = int(parts[1])
    except ValueError:
        await message.reply("Число билетиков должно быть целым числом.")
        return

    if amount <= 0:
        await message.reply("Число билетиков должно быть больше нуля.")
        return

    target_id, target_name, error_text = await resolve_ticket_command_target(
        message,
        parts[2] if len(parts) > 2 else None,
    )
    if error_text is not None:
        await message.reply(error_text)
        return
    if target_id is None or target_name is None:
        await message.reply("Не удалось определить пользователя.")
        return

    if action == "transfer":
        ok, info, stats = repo.transfer_player_tickets(
            message.from_user.id,
            user_nickname(message.from_user),
            target_id,
            target_name,
            amount,
        )
    else:
        delta = amount if action == "grant" else -amount
        ok, info, stats = repo.adjust_player_tickets(target_id, target_name, delta)
    if not ok:
        await message.reply(info)
        return

    target_mark = user_profile_link_by_id(target_id, target_name)
    if action == "transfer":
        sender_mark = user_profile_link_by_id(message.from_user.id, user_nickname(message.from_user))
        try:
            await message.bot.send_message(
                target_id,
                f"{sender_mark} вам передал {amount} 🎟",
                parse_mode="HTML",
            )
        except Exception:
            pass
        response_text = f"{target_mark} получил {amount} 🎟"
    elif action == "grant":
        try:
            await message.bot.send_message(
                target_id,
                f"Администратор начислил вам {amount} 🎟",
            )
        except Exception:
            pass
        response_text = f"Администратор начислил {target_mark} {amount} 🎟"
    else:
        try:
            await message.bot.send_message(
                target_id,
                f"Администрация забрала у вас {amount} 🎟",
            )
        except Exception:
            pass
        response_text = f"Администрация забрала у {target_mark} {amount} 🎟"
    await message.reply(response_text, parse_mode="HTML")


async def on_ticket_grant_command(message: Message) -> None:
    await handle_ticket_adjustment_command(message, action="transfer")


async def on_ticket_take_command(message: Message) -> None:
    await handle_ticket_adjustment_command(message, action="take")


async def on_ticket_admin_grant_command(message: Message) -> None:
    await handle_ticket_adjustment_command(message, action="grant")


async def on_private_ticket_grant_command(message: Message) -> None:
    await handle_ticket_adjustment_command(message, action="transfer")


async def on_private_ticket_take_command(message: Message) -> None:
    await handle_ticket_adjustment_command(message, action="take")


async def on_private_ticket_admin_grant_command(message: Message) -> None:
    await handle_ticket_adjustment_command(message, action="grant")


async def on_ticket_command_hint(message: Message) -> None:
    hint_text = ticket_command_hint_text(message.text)
    if hint_text is None:
        return
    await message.reply(hint_text)


async def on_private_ticket_command_hint(message: Message) -> None:
    hint_text = ticket_command_hint_text(message.text)
    if hint_text is None:
        return
    await message.reply(hint_text)

