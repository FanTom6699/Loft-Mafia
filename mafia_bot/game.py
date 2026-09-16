"""Compatibility facade for the historical Mafia game API.

The implementation is temporarily kept in ``game_legacy`` while the domain
model is moved into ``game_domain`` in small, testable steps. Existing imports
from ``mafia_bot.game`` therefore keep working during the refactor.
"""

from .game_legacy import *
from .game_domain import *
