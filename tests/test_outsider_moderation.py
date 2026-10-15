"""Outsider moderation without Telegram or the production database."""

import ast
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import time
import unittest
from unittest.mock import AsyncMock


class OutsiderModerationTests(unittest.IsolatedAsyncioTestCase):
    def setup_handler(self, phase="day", enabled=True, player=None):
        state = {"blocked_until": 0}
        room = SimpleNamespace(started=True, phase=phase, get_player=lambda uid: player,
                               day_silenced_user_id=None)
        namespace = dict(
            Message=object, time=time, datetime=datetime, timedelta=timedelta,
            ChatPermissions=lambda **kwargs: SimpleNamespace(**kwargs),
            get_or_create_penalty=lambda cid, uid: state,
            safe_send_message=AsyncMock(), safe_delete_message=AsyncMock(),
            storage=SimpleNamespace(get_room=lambda cid: room),
            PHASE_DAY="day", PHASE_NIGHT="night", PHASE_FINISHED="finished",
            room_chat_settings=lambda room: {"mute": {"outsiders": enabled, "dead": False, "sleeping": False}},
            is_user_blocked=lambda cid, uid: False,
            process_rule_violation=AsyncMock(),
        )
        path = Path(__file__).resolve().parents[1] / "mafia_bot/handlers/common.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        nodes = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                 and node.name in {"process_outsider_violation", "enforce_group_game_rules"}]
        for node in nodes:
            node.decorator_list = []
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
        bot = SimpleNamespace(restrict_chat_member=AsyncMock())
        message = SimpleNamespace(bot=bot, chat=SimpleNamespace(id=100),
                                  from_user=SimpleNamespace(id=200, is_bot=False), text="hello")
        for attr in ("photo", "video", "animation", "audio", "document", "voice",
                     "video_note", "poll", "location", "contact", "dice", "game"):
            setattr(message, attr, None)
        return namespace, message, state

    async def test_outsiders_deleted_and_muted_for_one_minute_day_and_night(self):
        for phase in ("day", "night"):
            ns, message, state = self.setup_handler(phase)
            before = datetime.now()
            await ns["enforce_group_game_rules"](message)
            ns["safe_delete_message"].assert_awaited_once_with(message)
            restriction = message.bot.restrict_chat_member.call_args.kwargs
            self.assertEqual(restriction["user_id"], 200)
            self.assertFalse(restriction["permissions"].can_send_messages)
            self.assertAlmostEqual((restriction["until_date"] - before).total_seconds(), 60, delta=1)
            self.assertAlmostEqual(state["blocked_until"] - time.time(), 60, delta=1)
            ns["safe_send_message"].assert_awaited_once_with(
                message.bot, 200,
                "🛑 Ваше сообщение удалено, вы временно заблокированы.\n"
                "В чат могут писать только участники игры. Ограничение временное. "
                "Сыграть можно будет после окончания — командой /game.")
            ns["process_rule_violation"].assert_not_awaited()

    async def test_repeated_violation_does_not_escalate_duration(self):
        ns, message, state = self.setup_handler()
        for _ in range(2):
            await ns["enforce_group_game_rules"](message)
        self.assertEqual(message.bot.restrict_chat_member.await_count, 2)
        self.assertAlmostEqual(state["blocked_until"] - time.time(), 60, delta=1)

    async def test_disabled_setting_and_participant_are_not_muted(self):
        for options in ({"enabled": False}, {"player": SimpleNamespace(alive=True)}):
            ns, message, _ = self.setup_handler(**options)
            await ns["enforce_group_game_rules"](message)
            message.bot.restrict_chat_member.assert_not_awaited()
            ns["safe_send_message"].assert_not_awaited()

    async def test_finished_game_is_not_moderated(self):
        ns, message, _ = self.setup_handler(phase="finished")
        await ns["enforce_group_game_rules"](message)
        ns["safe_delete_message"].assert_not_awaited()
