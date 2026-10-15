"""Regression tests for a defendant leaving before the verdict."""

import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

from mafia_bot.game_domain import *


class TrialLeaveTests(unittest.IsolatedAsyncioTestCase):
    async def run_leave(self, candidate_role, other_roles):
        room = GameRoom(chat_id=100, host_id=1)
        room.started = True
        room.phase = PHASE_DAY
        room.round_no = 2
        room.players = {
            index: Player(user_id=index, full_name=f"Player {index}", role=role)
            for index, role in enumerate([candidate_role] + other_roles, start=1)
        }
        room.start_day_trial(1)
        message = SimpleNamespace(chat=SimpleNamespace(id=100), from_user=SimpleNamespace(id=1),
                                  answer=AsyncMock(), bot=SimpleNamespace(send_message=AsyncMock()))
        namespace = dict(Message=object, storage=SimpleNamespace(get_room=lambda _: room),
                         cleanup_group_command_message=AsyncMock(), persist_room=Mock(),
                         PHASE_DAY=PHASE_DAY, PHASE_FINISHED=PHASE_FINISHED, PHASE_NIGHT=PHASE_NIGHT,
                         DAY_STAGE_TRIAL=DAY_STAGE_TRIAL, ROLE_DON=ROLE_DON, ROLE_COMMISSAR=ROLE_COMMISSAR,
                         cancel_phase_timer=Mock(), finish_trial_vote_message=AsyncMock(),
                         room_player_mark=lambda r, p: p.full_name,
                         player_link=lambda p: f'<a href="tg://user?id={p.user_id}">{p.full_name}</a>',
                         announce_don_transfer=AsyncMock(), announce_commissar_transfer=AsyncMock(),
                         private_game_send_kwargs=lambda r: {}, ensure_stats_recorded=Mock(),
                         send_endgame_currency_summaries=AsyncMock(), night_action_keyboard=AsyncMock(),
                         send_phase_media=AsyncMock(), NIGHT_IMAGE_PATH="night.jpg",
                         safe_send_message=AsyncMock(), night_status_text=lambda r: "night",
                         push_phase_action_menus=AsyncMock(), push_kamikaze_revenge_menu=AsyncMock(),
                         start_phase_timer=AsyncMock())
        path = Path(__file__).resolve().parents[1] / "mafia_bot/handlers/lobby.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "_cmd_leave_locked")
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
        await namespace["_cmd_leave_locked"](message)
        text = message.answer.await_args_list[0].args[0]
        self.assertIn("Голосование окончено", text)
        self.assertIn("tg://user?id=1", text)
        self.assertFalse(room.players[1].alive)
        namespace["finish_trial_vote_message"].assert_awaited_once()
        return room, namespace, message

    async def test_last_don_leaves_and_game_finishes(self):
        room, namespace, message = await self.run_leave(ROLE_DON, [ROLE_CITIZEN, ROLE_CITIZEN])
        self.assertEqual(room.phase, PHASE_FINISHED)
        self.assertEqual(message.answer.await_count, 2)
        namespace["start_phase_timer"].assert_not_awaited()

    async def test_citizen_leaves_and_next_night_starts(self):
        room, namespace, message = await self.run_leave(ROLE_CITIZEN, [ROLE_MAFIA, ROLE_CITIZEN, ROLE_CITIZEN])
        self.assertEqual(room.phase, PHASE_NIGHT)
        self.assertEqual(room.round_no, 3)
        self.assertIsNone(room.trial_candidate_id)
        self.assertEqual(message.answer.await_count, 1)
        namespace["start_phase_timer"].assert_awaited_once()

    async def test_don_role_is_transferred_on_exit(self):
        room, namespace, _ = await self.run_leave(ROLE_DON, [ROLE_MAFIA, ROLE_CITIZEN, ROLE_CITIZEN])
        self.assertEqual(room.players[2].role, ROLE_DON)
        namespace["announce_don_transfer"].assert_awaited_once()
