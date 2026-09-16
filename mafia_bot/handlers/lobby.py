"""Handler facade for the lobby area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

registration_remaining_seconds = _legacy.registration_remaining_seconds
start_registration_timer = _legacy.start_registration_timer
launch_game_from_registration = _legacy.launch_game_from_registration
maybe_launch_full_lobby = _legacy.maybe_launch_full_lobby
process_registration_timeout = _legacy.process_registration_timeout
registration_join_link = _legacy.registration_join_link
bot_start_link = _legacy.bot_start_link
registration_text = _legacy.registration_text
registration_post_text = _legacy.registration_post_text
private_bot_link = _legacy.private_bot_link
refresh_registration_post = _legacy.refresh_registration_post
pin_registration_post = _legacy.pin_registration_post
clear_registration_post = _legacy.clear_registration_post
registration_lobby_keyboard = _legacy.registration_lobby_keyboard
registration_panel = _legacy.registration_panel
cmd_create = _legacy.cmd_create
cmd_join = _legacy.cmd_join
cmd_leave = _legacy.cmd_leave
cmd_lobby = _legacy.cmd_lobby
cmd_extend = _legacy.cmd_extend
cmd_stop = _legacy.cmd_stop
cmd_begin = _legacy.cmd_begin
on_registration_action = _legacy.on_registration_action
