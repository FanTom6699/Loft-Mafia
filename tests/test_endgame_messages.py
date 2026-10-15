"""Regression coverage for final-night messages and defendant vote toasts."""

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock


def load_handler(filename, name, namespace):
    path = Path(__file__).resolve().parents[1] / "mafia_bot/handlers" / filename
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    node.decorator_list = []
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


class EndgameMessageTests(unittest.IsolatedAsyncioTestCase):
    async def run_night(self, finished):
        dead = SimpleNamespace(user_id=1, role="citizen")
        room = SimpleNamespace(
            phase="night", afk_killed_user_ids=set(), stats_recorded=True,
            pending_last_words={}, pending_last_words_public={}, day_stage="discussion",
            pop_night_reports=lambda: {}, pop_night_kill_sources=lambda: {},
            pop_pending_sergeant_check=lambda: None,
            pop_spent_shield_user_ids=lambda: [], pop_spent_documents_user_ids=lambda: [],
            day_media_caption=lambda: "day", alive_players_text=Mock(return_value="ALIVE"),
            alive_role_hints_text=Mock(return_value="ROLES"),
            pop_last_words_for_day=lambda: {}, final_report_text=lambda: "FINAL",
            start_day_discussion=Mock(),
        )
        def resolve():
            room.phase = "finished" if finished else "day"
            return True, "", [dead], None, None, None, None
        room.resolve_night = resolve
        ns = dict(Bot=object, get_phase_lock=lambda cid: asyncio.Lock(),
                  storage=SimpleNamespace(get_room=lambda cid: room),
                  cancel_phase_timer=Mock(), night_skipped_user_ids=lambda r: [],
                  mark_skipped_night_menus=AsyncMock(), send_phase_media=AsyncMock(),
                  DAY_IMAGE_PATH="day.jpg", PHASE_FINISHED="finished", PHASE_DAY="day",
                  DAY_STAGE_DISCUSSION="discussion", room_chat_settings=lambda r: {},
                  show_roles_enabled=lambda r: False, room_player_mark=lambda r, p: "Victim",
                  safe_send_message=AsyncMock(), private_game_send_kwargs=lambda r: {},
                  prompt_last_words=AsyncMock(), ensure_stats_recorded=Mock(),
                  send_endgame_currency_summaries=AsyncMock(), persist_room=Mock(),
                  start_phase_timer=AsyncMock())
        bot = SimpleNamespace(send_message=AsyncMock())
        await load_handler("game.py", "process_night_end", ns)(bot, 100)
        return room, ns, [call.args[1] for call in bot.send_message.await_args_list]

    async def test_final_night_sends_death_then_final_without_alive_summary(self):
        room, ns, texts = await self.run_night(True)
        self.assertEqual(texts, ["Сегодня был жестоко убит Victim", "FINAL"])
        room.alive_players_text.assert_not_called()
        ns["start_phase_timer"].assert_not_awaited()

    async def test_ongoing_game_keeps_alive_summary_and_discussion(self):
        room, ns, texts = await self.run_night(False)
        self.assertIn("ALIVE", texts[1])
        self.assertIn("обсудить результаты ночи", texts[1])
        room.start_day_discussion.assert_called_once()
        ns["start_phase_timer"].assert_awaited_once()

    async def test_defendant_yes_and_no_show_short_non_modal_toast(self):
        room = SimpleNamespace(phase="day", day_stage="trial", chat_id=100,
                               trial_vote_message_id=5, trial_candidate_id=1,
                               get_player=lambda uid: SimpleNamespace(alive=True),
                               set_trial_vote=Mock(return_value=(False, "Long explanation")))
        ns = dict(CallbackQuery=object, get_phase_lock=lambda cid: asyncio.Lock(),
                  storage=SimpleNamespace(get_room=lambda cid: room), PHASE_DAY="day",
                  DAY_STAGE_TRIAL="trial", MISTRESS_DAY_BLOCK_TOAST="blocked")
        handler = load_handler("day.py", "on_trial_callback", ns)
        for vote in ("yes", "no"):
            callback = SimpleNamespace(data=f"trial:{vote}:100", from_user=SimpleNamespace(id=1),
                                       message=SimpleNamespace(chat=SimpleNamespace(id=100), message_id=5),
                                       answer=AsyncMock())
            await handler(callback)
            callback.answer.assert_awaited_once_with("Ты не можешь голосовать за себя", show_alert=False)
