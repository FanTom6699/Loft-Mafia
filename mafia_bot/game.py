"""Handler facade for the game area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

night_action_keyboard = _legacy.night_action_keyboard
trial_vote_keyboard = _legacy.trial_vote_keyboard
selected_target_for_actor = _legacy.selected_target_for_actor
build_action_keyboard = _legacy.build_action_keyboard
build_action_prompt_text = _legacy.build_action_prompt_text
night_status_text = _legacy.night_status_text
refresh_private_action_message = _legacy.refresh_private_action_message
send_action_menu = _legacy.send_action_menu
push_phase_action_menus = _legacy.push_phase_action_menus
push_kamikaze_revenge_menu = _legacy.push_kamikaze_revenge_menu
push_trial_vote_menus = _legacy.push_trial_vote_menus
finish_trial_vote_message = _legacy.finish_trial_vote_message
send_mafia_private_update = _legacy.send_mafia_private_update
notify_room_private_cancellation = _legacy.notify_room_private_cancellation
announce_don_transfer = _legacy.announce_don_transfer
announce_commissar_transfer = _legacy.announce_commissar_transfer
prompt_last_words = _legacy.prompt_last_words
compact_night_report_messages = _legacy.compact_night_report_messages
sergeant_commissar_check_text = _legacy.sergeant_commissar_check_text
process_night_end = _legacy.process_night_end
process_day_end = _legacy.process_day_end
phase_timer_worker = _legacy.phase_timer_worker
start_phase_timer = _legacy.start_phase_timer
restore_runtime_state = _legacy.restore_runtime_state
maybe_finish_phase_early = _legacy.maybe_finish_phase_early
cmd_action = _legacy.cmd_action
cmd_status = _legacy.cmd_status
