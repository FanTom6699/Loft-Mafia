"""Majority selection and explicit Don tie-breaking."""

import unittest
from mafia_bot.game_domain import GameRoom, Player, ROLE_DON, ROLE_MAFIA, ROLE_CITIZEN


class MafiaVoteSelectionTests(unittest.TestCase):
    def setUp(self):
        self.room = GameRoom(chat_id=100, host_id=1)
        self.room.players = {
            uid: Player(user_id=uid, full_name=str(uid),
                        role=ROLE_DON if uid == 1 else ROLE_MAFIA if uid < 7 else ROLE_CITIZEN)
            for uid in range(1, 10)
        }

    def test_majority_overrides_don(self):
        self.assertEqual(self.room._choose_mafia_target({1: 7, 2: 8, 3: 8}), 8)

    def test_don_breaks_tie(self):
        self.assertEqual(self.room._choose_mafia_target({1: 7, 2: 8}), 7)

    def test_don_can_choose_outside_tied_leaders(self):
        self.assertEqual(self.room._choose_mafia_target({1: 9, 2: 7, 3: 7, 4: 8, 5: 8}), 9)

    def test_tie_without_don_means_no_attack_in_either_vote_order(self):
        for votes in ({2: 7, 3: 8}, {3: 8, 2: 7}):
            self.assertIsNone(self.room._choose_mafia_target(votes))

    def test_unique_leader_without_don(self):
        self.assertEqual(self.room._choose_mafia_target({2: 7, 3: 7, 4: 8}), 7)

    def test_same_target_and_no_votes(self):
        self.assertEqual(self.room._choose_mafia_target({1: 7, 2: 7}), 7)
        self.assertIsNone(self.room._choose_mafia_target({}))

    def test_blocked_don_cannot_break_tie(self):
        self.room.night_votes = {1: 9, 2: 7, 3: 8}
        self.assertIsNone(self.room._choose_mafia_target(self.room._active_mafia_votes(1)))
