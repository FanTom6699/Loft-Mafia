import unittest

from mafia_bot import game
from mafia_bot.game_domain import (
    GameRoom,
    GameStorage,
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
        self.assertIs(game.GameStorage, GameStorage)

    def test_lobby_methods_are_from_domain_module(self) -> None:
        self.assertEqual(GameRoom.add_player.__module__, "mafia_bot.game_domain.lobby")
        self.assertEqual(GameRoom.open_registration.__module__, "mafia_bot.game_domain.lobby")
        self.assertEqual(GameRoom.remove_player.__module__, "mafia_bot.game_domain.lobby")

    def test_role_methods_are_from_domain_module(self) -> None:
        self.assertEqual(GameRoom.assign_roles.__module__, "mafia_bot.game_domain.assignment")
        self.assertEqual(GameRoom.build_roles.__module__, "mafia_bot.game_domain.assignment")

    def test_night_action_methods_are_from_domain_module(self) -> None:
        self.assertEqual(GameRoom.set_night_vote.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_doctor_target.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_maniac_target.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_mistress_target.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_bum_target.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_advocate_target.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.check_player_role.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_commissar_action_mode.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_commissar_shot_target.__module__, "mafia_bot.game_domain.night_actions")
        self.assertEqual(GameRoom.set_kamikaze_target.__module__, "mafia_bot.game_domain.night_actions")

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
        self.assertEqual(room.phase, "night")
        self.assertEqual(room.round_no, 1)
        self.assertEqual(len(room.players), 6)
        assigned_roles = {player.role for player in room.players.values()}
        self.assertIn(ROLE_DON, assigned_roles)
        self.assertIn(ROLE_MAFIA, assigned_roles)
        self.assertIn(ROLE_CITIZEN, assigned_roles)
        self.assertIsNotNone(room.started_at)

    def test_domain_storage_manages_rooms(self) -> None:
        storage = GameStorage()
        created, message = storage.create_room(123, 456)

        self.assertTrue(created)
        self.assertEqual(message, "Лобби создано.")
        self.assertIsInstance(storage.get_room(123), GameRoom)
        self.assertFalse(storage.create_room(123, 789)[0])

        storage.close_room(123)
        self.assertIsNone(storage.get_room(123))


if __name__ == "__main__":
    unittest.main()
