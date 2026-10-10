"""Regression tests for the self-playing Mafia simulator."""

from __future__ import annotations

import unittest

from scripts.simulate_games import run_simulations


class SimulationTests(unittest.TestCase):
    def test_self_play_games_finish_deterministically(self) -> None:
        results = run_simulations(
            games=12,
            seed=20260929,
            verbose=False,
            show_roles=False,
            chaos=False,
            max_rounds=60,
            log_path=None,
        )

        self.assertEqual(len(results), 12)
        self.assertTrue(all(result.winner for result in results))
        self.assertTrue(all(result.nights <= 60 for result in results))
        self.assertTrue(all(result.deaths >= 0 for result in results))
        self.assertTrue(all(result.last_words >= 0 for result in results))

    def test_self_play_chaos_exercises_afk_path(self) -> None:
        results = run_simulations(
            games=6,
            seed=12345,
            verbose=False,
            show_roles=False,
            chaos=True,
            max_rounds=60,
            log_path=None,
        )

        self.assertEqual(len(results), 6)
        self.assertTrue(all(result.winner for result in results))
