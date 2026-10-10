"""Regression tests for persisted game state."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mafia_bot.game_domain import GameRoom
from mafia_bot.storage import GameStateRepository


class GameStateRepositoryTests(unittest.TestCase):
    def test_restores_last_doctor_saved_target_id(self) -> None:
        # SQLite files can be held briefly by Windows after a connection closes.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            repository = GameStateRepository(str(Path(directory) / "state.db"))
            room = GameRoom(chat_id=100, host_id=200)
            room.last_doctor_saved_target_id = 300

            repository.save_room(room)

            restored_room = repository.load_rooms()[room.chat_id]

        self.assertEqual(restored_room.last_doctor_saved_target_id, 300)
