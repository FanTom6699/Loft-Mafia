"""Restart regression checks using temporary SQLite and mocked Telegram I/O."""

import ast
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock

from mafia_bot.game_domain import GameRoom, Player, GameStorage, ROLE_MAFIA, ROLE_DOCTOR, ROLE_CITIZEN
from mafia_bot.storage import GameStateRepository


class RestartTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "restart.db")
        self.repository = GameStateRepository(self.path)
        self.now = datetime(2026, 10, 10, 12, 0)
        self.room = GameRoom(chat_id=100, host_id=1)
        self.room.started = True
        self.room.round_no = 2
        self.room.phase_started_at = self.now - timedelta(seconds=20)
        self.room.phase_duration_seconds = 60
        self.room.players = {
            1: Player(user_id=1, full_name="Mafia", role=ROLE_MAFIA),
            2: Player(user_id=2, full_name="Doctor", role=ROLE_DOCTOR),
            3: Player(user_id=3, full_name="Citizen", role=ROLE_CITIZEN),
            4: Player(user_id=4, full_name="Citizen", role=ROLE_CITIZEN),
        }

    def reload(self):
        self.repository.save_room(self.room)
        fresh_repository = GameStateRepository(self.path)
        storage = GameStorage()
        storage.rooms = fresh_repository.load_rooms()
        return storage

    def runtime(self, storage, policy="catch_up"):
        namespace = dict(
            Bot=object, storage=storage,
            datetime=SimpleNamespace(now=lambda: self.now),
            PHASE_FINISHED="finished", PHASE_DAY="day", PHASE_NIGHT="night",
            apply_room_settings=Mock(), persist_room=Mock(),
            RESTART_EXPIRED_PHASE_POLICY=policy,
            start_phase_timer=AsyncMock(), push_phase_action_menus=AsyncMock(),
            process_night_end=AsyncMock(), process_day_end=AsyncMock(),
        )
        path = Path(__file__).resolve().parents[1] / "mafia_bot" / "handlers" / "game.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "restore_runtime_state")
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
        return namespace

    async def test_all_active_phases_restore_timer_and_choices(self):
        for phase, stage in (("night", None), ("day", "discussion"), ("day", "nomination"), ("day", "trial")):
            with self.subTest(phase=phase, stage=stage):
                self.room.phase = phase
                self.room.day_stage = stage
                self.room.night_votes = {1: 3}
                self.room.doctor_target_id = 3
                self.room.day_votes = {1: 3}
                self.room.trial_candidate_id = 3
                self.room.trial_vote_message_id = 777
                self.room.trial_votes = {1: True, 2: False}
                self.room.stats_recorded = True
                storage = self.reload()
                restored = storage.get_room(100)
                namespace = self.runtime(storage)
                bot = object()
                await namespace["restore_runtime_state"](bot)
                namespace["start_phase_timer"].assert_awaited_once_with(restored, bot, remaining_sec=40, reset_deadline=False)
                self.assertEqual(restored.phase, phase)
                self.assertEqual(restored.day_stage, stage)
                self.assertEqual(restored.night_votes, {1: 3})
                self.assertEqual(restored.doctor_target_id, 3)
                self.assertEqual(restored.day_votes, {1: 3})
                self.assertEqual(restored.trial_votes, {1: True, 2: False})
                self.assertEqual(restored.trial_vote_message_id, 777)
                self.assertTrue(restored.stats_recorded)

    async def test_expired_phases_catch_up_once(self):
        for phase, stage in (("night", None), ("day", "discussion"), ("day", "nomination"), ("day", "trial")):
            with self.subTest(phase=phase, stage=stage):
                self.room.phase = phase
                self.room.day_stage = stage
                self.room.phase_started_at = self.now - timedelta(seconds=80)
                namespace = self.runtime(self.reload())
                await namespace["restore_runtime_state"](object())
                selected = "process_night_end" if phase == "night" else "process_day_end"
                self.assertEqual(namespace[selected].await_count, 1)
                namespace["start_phase_timer"].assert_not_awaited()

    async def test_expired_phase_restart_keeps_choices(self):
        self.room.phase = "night"
        self.room.night_votes = {1: 3}
        self.room.phase_started_at = self.now - timedelta(seconds=80)
        storage = self.reload()
        namespace = self.runtime(storage, policy="restart")
        bot = object()
        await namespace["restore_runtime_state"](bot)
        restored = storage.get_room(100)
        self.assertEqual(restored.phase_started_at, self.now)
        self.assertEqual(restored.night_votes, {1: 3})
        namespace["start_phase_timer"].assert_awaited_once_with(restored, bot, remaining_sec=60, reset_deadline=False)
        namespace["process_night_end"].assert_not_awaited()

    async def test_restored_night_resolves_with_saved_doctor_choice(self):
        self.room.phase = "night"
        self.room.night_votes = {1: 3}
        self.room.doctor_target_id = 3
        restored = self.reload().get_room(100)
        ok, *_ = restored.resolve_night()
        self.assertTrue(ok)
        self.assertTrue(restored.players[3].alive)
        self.assertEqual(restored.last_doctor_saved_target_id, 3)

    async def test_restored_trial_resolves_with_saved_votes(self):
        self.room.phase = "day"
        self.room.start_day_trial(3)
        self.room.trial_votes = {1: True, 2: True, 4: False}
        restored = self.reload().get_room(100)
        ok, *_ = restored.resolve_day_trial()
        self.assertTrue(ok)
        self.assertFalse(restored.players[3].alive)

    async def test_expired_registration_can_be_removed_during_restore(self):
        self.room.phase = "lobby"
        self.room.started = False
        self.room.registration_open = True
        self.room.phase_started_at = self.now - timedelta(seconds=80)
        storage = self.reload()
        namespace = self.runtime(storage)
        async def finish(bot, chat_id):
            storage.close_room(chat_id)
        namespace["registration_remaining_seconds"] = lambda room: 0
        namespace["process_registration_timeout"] = AsyncMock(side_effect=finish)
        await namespace["restore_runtime_state"](object())
        self.assertEqual(storage.rooms, {})
