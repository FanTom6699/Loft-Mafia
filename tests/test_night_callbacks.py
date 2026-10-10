"""Exercise the real callback functions with an isolated Telegram runtime."""

import ast
import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from mafia_bot.game_domain import GameRoom, Player, GameStorage, ROLE_DOCTOR, ROLE_MANIAC, ROLE_CITIZEN, ROLE_COMMISSAR


HANDLERS = Path(__file__).resolve().parents[1] / "mafia_bot" / "handlers"
spec = importlib.util.spec_from_file_location("isolated_night_guard", HANDLERS / "night_guard.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def load_functions(filename, names, namespace):
    # Loading handlers normally initializes the production repository and router.
    tree = ast.parse((HANDLERS / filename).read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(HANDLERS / filename), "exec"), namespace)


class NightCallbackTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.room = GameRoom(chat_id=100, host_id=1)
        self.room.started = True
        self.room.phase = "night"
        self.room.round_no = 2
        self.room.players = {
            1: Player(user_id=1, full_name="Doctor", role=ROLE_DOCTOR),
            2: Player(user_id=2, full_name="Maniac", role=ROLE_MANIAC),
            3: Player(user_id=3, full_name="Citizen", role=ROLE_CITIZEN),
            4: Player(user_id=4, full_name="Citizen", role=ROLE_CITIZEN),
        }
        self.storage = GameStorage()
        self.storage.rooms[100] = self.room
        self.lock = asyncio.Lock()
        self.notifications = AsyncMock()
        self.persist = unittest.mock.Mock()
        self.finishes = 0

        async def finish(bot, room):
            if guard.defer_night_finish(room):
                return
            async with self.lock:
                if room.phase == "night" and room.all_required_night_actions_done():
                    self.finishes += 1
                    room.resolve_night()

        self.namespace = dict(
            CallbackQuery=object, PHASE_NIGHT="night", PHASE_DAY="day",
            ROLE_KAMIKAZE="Kamikaze", ROLE_MAFIA="Mafia", MAFIA_ROLES=set(),
            DAY_STAGE_TRIAL="trial", storage=self.storage,
            get_phase_lock=lambda chat_id: self.lock,
            action_menu_messages={},
            guarded_night_callback=guard.guarded_night_callback,
            maybe_finish_phase_early=finish, persist_room=self.persist,
            MSG_NOT_IN_GAME="Not in game", MSG_INVALID_ACTION="Invalid",
            night_role_announcement_text=lambda *args, **kwargs: "announcement",
            safe_send_message=self.notifications,
            room_player_label=lambda room, player: player.full_name,
            locked_choice_text=lambda *args: "selected",
            night_action_skip_enabled=lambda room: True,
            build_action_prompt_text=lambda *args: "prompt",
            skip_turn_selected_text=lambda: "skip",
            commissar_can_shoot_enabled=lambda room: True,
            commissar_can_shoot_this_night=lambda room: True,
            refresh_private_action_message=AsyncMock(),
        )
        load_functions("night.py", {"on_action_callback", "_on_action_callback"}, self.namespace)
        load_functions("callbacks.py", {"on_noop_callback", "_on_noop_callback"}, self.namespace)

    def callback(self, data, user_id=1):
        return SimpleNamespace(
            data=data, from_user=SimpleNamespace(id=user_id),
            message=SimpleNamespace(message_id=10, chat=SimpleNamespace(type="private"), edit_text=AsyncMock()),
            answer=AsyncMock(), bot=SimpleNamespace(send_message=AsyncMock()),
        )

    async def test_repeated_callback_does_not_duplicate_action_or_notifications(self):
        callback = self.callback("act:heal:100:3")
        handler = self.namespace["on_action_callback"]
        await asyncio.gather(handler(callback), handler(callback))
        self.assertEqual(self.room.doctor_target_id, 3)
        self.assertEqual(self.notifications.await_count, 1)
        self.assertEqual(callback.message.edit_text.await_count, 1)
        self.assertEqual(self.persist.call_count, 1)

    async def test_two_actions_hold_the_common_lock_and_finish_once(self):
        async def notify(*args, **kwargs):
            self.assertTrue(self.lock.locked())
            await asyncio.sleep(0)
        self.notifications.side_effect = notify
        handler = self.namespace["on_action_callback"]
        await asyncio.wait_for(asyncio.gather(
            handler(self.callback("act:heal:100:3")),
            handler(self.callback("act:maniac:100:4", 2)),
        ), timeout=2)
        self.assertEqual(self.finishes, 1)
        self.assertNotEqual(self.room.phase, "night")
        self.assertFalse(self.room.players[4].alive)

    async def test_callback_after_night_cannot_change_state(self):
        self.room.phase = "day"
        await self.namespace["on_action_callback"](self.callback("act:heal:100:3"))
        self.assertIsNone(self.room.doctor_target_id)
        self.notifications.assert_not_awaited()
        self.persist.assert_not_called()

    async def test_callback_waiting_for_lock_is_rechecked_after_phase_change(self):
        await self.lock.acquire()
        task = asyncio.create_task(self.namespace["on_action_callback"](self.callback("act:heal:100:3")))
        await asyncio.sleep(0)
        self.room.phase = "day"
        self.lock.release()
        await task
        self.assertIsNone(self.room.doctor_target_id)
        self.persist.assert_not_called()

    async def test_repeated_skip_is_applied_once(self):
        callback = self.callback("noop:skip:100")
        handler = self.namespace["on_noop_callback"]
        await asyncio.gather(handler(callback), handler(callback))
        self.assertEqual(self.room.night_skipped_user_ids, {1})
        self.assertEqual(callback.message.edit_text.await_count, 1)
        self.assertEqual(self.persist.call_count, 1)

    async def test_old_menu_cannot_act_in_the_next_night(self):
        self.namespace["action_menu_messages"] = {100: {1: 20}}
        await self.namespace["on_action_callback"](self.callback("act:heal:100:3"))
        await self.namespace["on_noop_callback"](self.callback("noop:skip:100"))
        self.assertIsNone(self.room.doctor_target_id)
        self.assertEqual(self.room.night_skipped_user_ids, set())
        self.persist.assert_not_called()

    async def test_repeated_commissar_mode_does_not_repeat_announcement(self):
        self.room.players[1].role = ROLE_COMMISSAR
        self.room.settings = {"roles": {"commissar_can_shoot": True}}
        callback = self.callback("act:commode:100:2")
        # Keep this test focused on callback idempotence; role settings validation
        # is covered by domain behavior, and the setter still updates real state.
        def select_mode(user_id, mode):
            self.room.commissar_action_mode = mode
            return True, "selected"
        self.room.set_commissar_action_mode = select_mode
        handler = self.namespace["on_action_callback"]
        await asyncio.gather(handler(callback), handler(callback))
        self.assertEqual(callback.bot.send_message.await_count, 1)
        self.assertEqual(self.persist.call_count, 1)
