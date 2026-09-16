"""Handler facade for the callbacks area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

on_registration_action = _legacy.on_registration_action
on_trial_callback = _legacy.on_trial_callback
on_action_callback = _legacy.on_action_callback
on_noop_callback = _legacy.on_noop_callback
on_private_menu_callback = _legacy.on_private_menu_callback
on_top_callback = _legacy.on_top_callback
on_private_settings_callback = _legacy.on_private_settings_callback
on_private_text = _legacy.on_private_text
on_owner_exit_phrase = _legacy.on_owner_exit_phrase
on_developer_phrase = _legacy.on_developer_phrase
enforce_group_game_rules = _legacy.enforce_group_game_rules
