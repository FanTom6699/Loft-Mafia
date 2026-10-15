"""Chat leaderboards use only results recorded for that chat."""

import sqlite3
import gc
import tempfile
from pathlib import Path
import unittest
from mafia_bot.storage import GameStateRepository
from mafia_bot.game_domain import GameRoom, Player, ROLE_DON, ROLE_CITIZEN


class ChatTopTests(unittest.TestCase):
    def test_results_are_saved_and_ranked_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = GameStateRepository(Path(directory) / "test.db")
            for chat_id, winner in ((100, 1), (200, 2)):
                room = GameRoom(chat_id=chat_id, host_id=1)
                room.winner_team = "Мафия"
                room.players = {
                    uid: Player(user_id=uid, full_name=str(uid),
                                role=ROLE_DON if uid == winner else ROLE_CITIZEN)
                    for uid in (1, 2)
                }
                repo.record_finished_game_stats(room)
            for period in ("all", "day", "week", "month"):
                self.assertEqual(repo.get_top_players(period=period, chat_id=100)[0]["user_id"], 1)
                self.assertEqual(repo.get_top_players(period=period, chat_id=200)[0]["user_id"], 2)
            self.assertEqual(repo.get_top_players(chat_id=300), [])
            gc.collect()

    def test_old_results_are_preserved_but_not_assigned_to_a_chat(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.db"
            repo = GameStateRepository(path)
            with sqlite3.connect(path) as conn:
                conn.execute("INSERT INTO player_game_results(user_id, display_name, won, finished_at) VALUES(1, 'Old', 1, '2026-01-01')")
            conn.close()
            self.assertEqual(repo.get_top_players(chat_id=100), [])
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM player_game_results").fetchone()[0], 1)
            conn.close()
            gc.collect()
