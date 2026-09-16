"""Public compatibility facade for the Mafia game domain.

The implementation now lives in ``mafia_bot.game_domain``.  This module
remains as the stable import path for existing handlers, storage code, and
external integrations.
"""

# Compatibility facade: keep the legacy public import path stable.
from .game_domain import *
