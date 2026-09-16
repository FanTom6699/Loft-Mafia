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

    def test_player_state_methods_are_from_domain_module(self) -> None:
        expected = (
            "get_player",
            "alive_players",
            "alive_mafia",
            "alive_mafia_ids",
            "all_mafia_voted",
            "current_mafia_target_id",
            "mark_night_role_announced",
            "transfer_don_if_needed",
            "transfer_commissar_if_needed",
            "remember_commissar_check",
            "set_pending_sergeant_check",
            "pop_pending_sergeant_check",
            "forget_dead_commissar_checks",
            "alive_civilians",
            "check_winner",
        )
        for method_name in expected:
            with self.subTest(method_name=method_name):
                self.assertEqual(getattr(GameRoom, method_name).__module__, "mafia_bot.game_domain.player_state")

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

    def test_night_resolution_is_from_domain_module(self) -> None:
        self.assertEqual(GameRoom.resolve_night.__module__, "mafia_bot.game_domain.night_resolution")

    def test_night_state_methods_are_from_domain_module(self) -> None:
        self.assertEqual(GameRoom.all_required_night_actions_done.__module__, "mafia_bot.game_domain.night_state")
        self.assertEqual(GameRoom.can_skip_night_action.__module__, "mafia_bot.game_domain.night_state")
        self.assertEqual(GameRoom.set_night_skip.__module__, "mafia_bot.game_domain.night_state")
        self.assertEqual(GameRoom.arm_shield.__module__, "mafia_bot.game_domain.night_state")
        self.assertEqual(GameRoom.arm_documents.__module__, "mafia_bot.game_domain.night_state")
        self.assertEqual(GameRoom.pop_spent_documents_user_ids.__module__, "mafia_bot.game_domain.night_state")
        self.assertEqual(GameRoom.pop_spent_shield_user_ids.__module__, "mafia_bot.game_domain.night_state")

    def test_day_flow_methods_are_from_domain_module(self) -> None:
        expected = (
            "all_alive_day_voted",
            "all_alive_trial_voted",
            "start_day_discussion",
            "start_day_nomination",
            "set_trial_vote",
            "trial_vote_counts",
            "end_day_no_lynch",
            "resolve_day_trial",
        )
        for method_name in expected:
            with self.subTest(method_name=method_name):
                self.assertEqual(getattr(GameRoom, method_name).__module__, "mafia_bot.game_domain.day_flow")

    def test_presentation_methods_are_from_domain_module(self) -> None:
        expected = (
            "public_player_mark",
            "commissar_check_result_text",
            "pop_night_reports",
            "add_night_report_line",
            "queue_last_words",
            "can_send_last_word",
            "consume_last_word",
            "set_day_vote",
            "resolve_day",
            "end_day_without_votes",
            "pop_night_kill_sources",
            "alive_role_counts_text",
            "alive_players_text",
            "alive_role_hints_text",
            "game_duration_text",
            "final_report_text",
            "night_intro_text",
            "night_media_caption",
            "day_intro_text",
            "day_media_caption",
            "status_text",
            "lobby_text",
        )
        for method_name in expected:
            with self.subTest(method_name=method_name):
                self.assertEqual(getattr(GameRoom, method_name).__module__, "mafia_bot.game_domain.presentation")

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

    def test_day_nomination_flow(self) -> None:
        room = GameRoom(chat_id=123, host_id=456)
        room.phase = "day"
        room.start_day_nomination()
        for user_id in range(1, 5):
            room.players[user_id] = Player(user_id=user_id, full_name=f"Player {user_id}")

        room.day_votes = {1: 2, 2: 2, 3: 2, 4: 3}
        self.assertTrue(room.all_alive_day_voted())
        ok, candidate_id = room.resolve_day_nomination()
        self.assertTrue(ok)
        self.assertEqual(candidate_id, 2)

    def test_day_trial_flow(self) -> None:
        room = GameRoom(chat_id=123, host_id=456)
        room.phase = "day"
        for user_id in range(1, 5):
            room.players[user_id] = Player(user_id=user_id, full_name=f"Player {user_id}")

        ok, message = room.start_day_trial(1)
        self.assertTrue(ok)
        self.assertEqual(room.day_stage, "trial")
        self.assertEqual(message, "Этап голосования за/против запущен.")

        self.assertEqual(room.set_trial_vote(2, True), (True, "Твой голос принят."))
        self.assertEqual(room.set_trial_vote(3, True), (True, "Твой голос принят."))
        self.assertEqual(room.set_trial_vote(4, False), (True, "Твой голос принят."))
        self.assertTrue(room.all_alive_trial_voted())
        self.assertEqual(room.trial_vote_counts(), (2, 1))

    def test_day_no_lynch_transitions_to_night(self) -> None:
        room = GameRoom(chat_id=123, host_id=456)
        room.phase = "day"
        room.round_no = 2
        room.start_day_discussion()

        ok, message = room.end_day_no_lynch()

        self.assertTrue(ok)
        self.assertEqual(message, "Сегодня решили никого не вешать. Наступает ночь.")
        self.assertEqual(room.phase, "night")
        self.assertEqual(room.round_no, 3)

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
