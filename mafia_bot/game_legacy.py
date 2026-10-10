"""Temporary compatibility shim for the extracted game domain.

The game engine no longer lives here. This module only preserves the legacy
Player import used by the domain resolver while the public API migrates to
``mafia_bot.game_domain``.
"""

from dataclasses import dataclass


@dataclass
class Player:
    user_id: int
    full_name: str
    role: str = ""
    alive: bool = True
