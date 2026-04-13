import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats

from mafia_bot.config import get_settings
from mafia_bot.handlers import restore_runtime_state, router


async def setup_bot_commands(bot: Bot) -> None:
    group_commands = [
        BotCommand(command="game", description="Запустить новую игру (регистрация)"),
        BotCommand(command="settings", description="Настройки игры"),
        BotCommand(command="leave", description="Выйти из регистрации / игры"),
        BotCommand(command="extend", description="Продлить регистрацию"),
        BotCommand(command="start", description="Завершить регистрацию и начать игру"),
        BotCommand(command="stop", description="Отменить регистрацию / остановить игру"),
    ]
    private_commands = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="roles", description="Список ролей"),
        BotCommand(command="stats", description="Твоя статистика"),
    ]

    await bot.set_my_commands(group_commands, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(private_commands, scope=BotCommandScopeAllPrivateChats())


async def start_bot() -> None:
    settings = get_settings()
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    try:
        await setup_bot_commands(bot)
        await restore_runtime_state(bot)

        await dp.start_polling(bot)
    finally:
        await bot.session.close()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    while True:
        try:
            await start_bot()
            return
        except Exception:
            logging.exception("CRASH:")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
