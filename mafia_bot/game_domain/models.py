"""Stateful domain models for the Mafia game.

This module is the compatibility boundary for ``Player`` and ``GameRoom``
while the historical implementation is extracted in small, safe steps.
The classes are intentionally re-exported from ``game_legacy`` for now so
there is only one runtime class identity and storage serialization remains
compatible during the migration.
"""

from ..game_legacy import GameRoom, Player

__all__ = ["Player", "GameRoom"]
