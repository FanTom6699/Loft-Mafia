from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message

from mafia_bot.game import MIN_PLAYERS, GameStorage

router = Router()
storage = GameStorage()


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    text = (
        "Привет. Это бот для игры в Мафию.\n\n"
        "Команды:\n"
        "/create - создать лобби\n"
        "/join - войти в лобби\n"
        "/leave - выйти из лобби\n"
        "/lobby - показать игроков\n"
        "/begin - начать игру\n"
        "/close - закрыть лобби"
    )
    await message.answer(text)


@router.message(Command("create"))
async def cmd_create(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer("Создавай лобби в групповом чате.")
        return

    ok, info = storage.create_room(chat_id=message.chat.id, host_id=message.from_user.id)
    if not ok:
        await message.answer(info)
        return

    room = storage.get_room(message.chat.id)
    room.add_player(message.from_user.id, message.from_user.full_name)
    await message.answer("Лобби создано. Ведущий уже добавлен. Используйте /join")
    await message.answer(room.lobby_text())


@router.message(Command("join"))
async def cmd_join(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Сначала создай лобби: /create")
        return

    ok, info = room.add_player(message.from_user.id, message.from_user.full_name)
    if not ok:
        await message.answer(info)
        return

    await message.answer("Ты вошел в лобби.")
    await message.answer(room.lobby_text())


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
    else:
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

    if room.host_id != message.from_user.id:
        await message.answer("Только создатель лобби может начать игру.")
        return

    if len(room.players) < MIN_PLAYERS:
        await message.answer(f"Нужно минимум {MIN_PLAYERS} игрока(ов).")
        return

    room.assign_roles()
    await message.answer("Игра началась. Раздаю роли в личку.")

    for player in room.players.values():
        try:
            await message.bot.send_message(
                player.user_id,
                f"Твоя роль: <b>{player.role}</b>\nЧат: {message.chat.title}",
            )
        except Exception:
            await message.answer(
                f"Не смог отправить роль {player.full_name}."
                " Пусть напишет боту /start в личке."
            )


@router.message(Command("close"))
async def cmd_close(message: Message) -> None:
    room = storage.get_room(message.chat.id)
    if room is None:
        await message.answer("Лобби не найдено.")
        return

    if room.host_id != message.from_user.id:
        await message.answer("Только создатель лобби может закрыть игру.")
        return

    storage.close_room(message.chat.id)
    await message.answer("Лобби закрыто.")
