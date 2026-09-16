"""Handler facade for the profile area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

ensure_stats_recorded = _legacy.ensure_stats_recorded
format_player_stats_text = _legacy.format_player_stats_text
top_period_keyboard = _legacy.top_period_keyboard
format_top_text = _legacy.format_top_text
format_endgame_currency_text = _legacy.format_endgame_currency_text
format_private_profile_text = _legacy.format_private_profile_text
send_endgame_currency_summaries = _legacy.send_endgame_currency_summaries
cmd_roles = _legacy.cmd_roles
cmd_stats = _legacy.cmd_stats
send_top_to_private = _legacy.send_top_to_private
cmd_top = _legacy.cmd_top
cmd_profile = _legacy.cmd_profile
on_top_callback = _legacy.on_top_callback
