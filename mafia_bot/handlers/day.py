"""Handler facade for the day area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

trial_vote_keyboard = _legacy.trial_vote_keyboard
push_trial_vote_menus = _legacy.push_trial_vote_menus
finish_trial_vote_message = _legacy.finish_trial_vote_message
prompt_last_words = _legacy.prompt_last_words
on_trial_callback = _legacy.on_trial_callback
