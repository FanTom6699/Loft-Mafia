"""Handler facade for the common area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

read_phase_seconds = _legacy.read_phase_seconds
read_user_id_set = _legacy.read_user_id_set
get_phase_lock = _legacy.get_phase_lock
cancel_phase_timer = _legacy.cancel_phase_timer
cancel_registration_timer = _legacy.cancel_registration_timer
current_day_stage_seconds = _legacy.current_day_stage_seconds
clear_chat_penalties = _legacy.clear_chat_penalties
persist_room = _legacy.persist_room
remove_room_state = _legacy.remove_room_state
role_mark_text = _legacy.role_mark_text
is_secret_voting_enabled = _legacy.is_secret_voting_enabled
trial_vote_prompt_text = _legacy.trial_vote_prompt_text
show_targets_enabled = _legacy.show_targets_enabled
show_roles_enabled = _legacy.show_roles_enabled
allow_team_kill_enabled = _legacy.allow_team_kill_enabled
commissar_can_shoot_enabled = _legacy.commissar_can_shoot_enabled
commissar_can_shoot_this_night = _legacy.commissar_can_shoot_this_night
night_action_skip_enabled = _legacy.night_action_skip_enabled
day_vote_skip_enabled = _legacy.day_vote_skip_enabled
content_protection_enabled = _legacy.content_protection_enabled
buffs_enabled = _legacy.buffs_enabled
invisible_mode_enabled = _legacy.invisible_mode_enabled
settings_mode_locked_message = _legacy.settings_mode_locked_message
settings_mode_locked = _legacy.settings_mode_locked
apply_game_mode_preset = _legacy.apply_game_mode_preset
room_player_mark = _legacy.room_player_mark
room_player_label = _legacy.room_player_label
private_settings_main_text = _legacy.private_settings_main_text
private_game_send_kwargs = _legacy.private_game_send_kwargs
night_role_announcement_text = _legacy.night_role_announcement_text
default_chat_settings = _legacy.default_chat_settings
merge_chat_settings = _legacy.merge_chat_settings
load_chat_settings = _legacy.load_chat_settings
room_chat_settings = _legacy.room_chat_settings
apply_room_settings = _legacy.apply_room_settings
save_chat_settings = _legacy.save_chat_settings
settings_callback_data = _legacy.settings_callback_data
selected_square = _legacy.selected_square
format_settings_screen_text = _legacy.format_settings_screen_text
format_leave_duration = _legacy.format_leave_duration
current_settings_timing_value = _legacy.current_settings_timing_value
current_settings_mute_value = _legacy.current_settings_mute_value
track_action_menu_message = _legacy.track_action_menu_message
get_action_menu_message_id = _legacy.get_action_menu_message_id
clear_action_menu_messages = _legacy.clear_action_menu_messages
resolve_phase_image_path = _legacy.resolve_phase_image_path
safe_send_message = _legacy.safe_send_message
safe_delete_message = _legacy.safe_delete_message
delete_message_later = _legacy.delete_message_later
user_nickname = _legacy.user_nickname
player_display_name = _legacy.player_display_name
normalize_link_display_name = _legacy.normalize_link_display_name
player_profile_link = _legacy.player_profile_link
user_profile_link_by_id = _legacy.user_profile_link_by_id
