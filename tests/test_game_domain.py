import unittest

from mafia_bot import game
from mafia_bot.game_domain import (
    GameRoom,
    Player,
    ROLE_CITIZEN,
    ROLE_DON,
    ROLE_MAFIA,
    ROLE_PLAN_BY_COUNT,
)


class GameDomainCompatibilityTests(unittest.TestCase):
    def test_public_game_api_uses_domain_models(self) -> None:
        self.assertIs(game.Player, Player)
        self.assertIs(game.GameRoom, GameRoom)

    def test_room_starts_empty_and_can_open_registration(self) -> None:
        room = GameRoom(chat_id=123, host_id=456)
        self.assertEqual(room.players, {})
        self.assertFalse(room.registration_open)

        room.open_registration()
        added, message = room.add_player(1, "Alice")

        self.assertTrue(added)
        self.assertEqual(message, "Игрок добавлен.")
        self.assertIsInstance(room.get_player(1), Player)

    def test_role_plan_keeps_expected_size(self) -> None:
        for player_count, roles in ROLE_PLAN_BY_COUNT.items():
            self.assertEqual(len(roles), player_count)

    def test_basic_role_assignment_uses_domain_state(self) -> None:
        room = GameRoom(chat_id=123, host_id=456)
        room.open_registration()
        for user_id in range(1, 7):
            room.add_player(user_id, f"Player {user_id}")

        room.assign_roles()

        self.assertTrue(room.started)
        self.assertEqual(len(room.players), 6)
        assigned_roles = {player.role for player in room.players.values()}
        self.assertIn(ROLE_DON, assigned_roles)
        self.assertIn(ROLE_MAFIA, assigned_roles)
        self.assertIn(ROLE_CITIZEN, assigned_roles)


if __name__ == "__main__":
    unittest.main()
