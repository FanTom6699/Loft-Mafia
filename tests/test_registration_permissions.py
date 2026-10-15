"""Registration permission checks without connecting to Telegram."""

import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock


class RegistrationPermissionTests(unittest.IsolatedAsyncioTestCase):
    def load_check(self, local_mute=False):
        namespace = dict(Bot=object, is_user_blocked=lambda chat_id, user_id: local_mute,
                         safe_send_message=AsyncMock())
        path = Path(__file__).resolve().parents[1] / "mafia_bot/handlers/lobby.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "registration_write_blocked")
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
        return namespace

    async def test_telegram_mute_denies_registration_and_sends_private_reason(self):
        namespace = self.load_check()
        bot = SimpleNamespace(get_chat_member=AsyncMock(return_value=SimpleNamespace(status="restricted", can_send_messages=False)))
        self.assertTrue(await namespace["registration_write_blocked"](bot, 100, 200))
        namespace["safe_send_message"].assert_awaited_once_with(bot, 200, "Не удалось присоединиться к игре\nВы не можете писать в этом чате")

    async def test_local_mute_denies_registration(self):
        namespace = self.load_check(local_mute=True)
        bot = SimpleNamespace(get_chat_member=AsyncMock())
        self.assertTrue(await namespace["registration_write_blocked"](bot, 100, 200))
        bot.get_chat_member.assert_not_awaited()

    async def test_members_who_can_write_are_allowed(self):
        for member in (SimpleNamespace(status="member"), SimpleNamespace(status="restricted", can_send_messages=True)):
            namespace = self.load_check()
            bot = SimpleNamespace(get_chat_member=AsyncMock(return_value=member))
            self.assertFalse(await namespace["registration_write_blocked"](bot, 100, 200))
            namespace["safe_send_message"].assert_not_awaited()

    async def test_permission_check_failure_does_not_allow_registration(self):
        namespace = self.load_check()
        bot = SimpleNamespace(get_chat_member=AsyncMock(side_effect=RuntimeError("offline")))
        self.assertTrue(await namespace["registration_write_blocked"](bot, 100, 200))
