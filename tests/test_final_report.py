"""Regression coverage for the group endgame report."""

import unittest

from mafia_bot.game_domain import (
    GameRoom, Player, ROLE_MAFIA, ROLE_DON, ROLE_ADVOCATE,
    ROLE_MANIAC, ROLE_CITIZEN,
)


class FinalReportTests(unittest.TestCase):
    def test_report_for_each_winning_team(self):
        roles = [ROLE_MAFIA, ROLE_DON, ROLE_ADVOCATE, ROLE_MANIAC, ROLE_CITIZEN]
        for team, winning_ids in (("Мафия", {1, 2, 3}), ("Маньяк", {4}), ("Мирные жители", {5})):
            with self.subTest(team=team):
                room = GameRoom(chat_id=100, host_id=1)
                room.winner_team = team
                room.players = {
                    index: Player(user_id=index, full_name=f"Player {index}", role=role)
                    for index, role in enumerate(roles, start=1)
                }
                report = room.final_report_text()
                winners, others = report.split("Остальные участники:")
                self.assertIn(f"Победили: {team}", report)
                for user_id in room.players:
                    link = f"tg://user?id={user_id}"
                    self.assertIn(link, winners if user_id in winning_ids else others)
