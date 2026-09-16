# Handlers refactor — Stage 1

The original `mafia_bot/handlers.py` was split into modules by responsibility.
`handlers_legacy.py` is kept as a backup of the original file.

Modules:
- common.py — shared helpers/runtime-facing functions
- lobby.py — registration/lobby commands
- game.py — phase/timer/runtime restoration
- night.py — night actions
- day.py — daytime/trial handling
- profile.py — profiles/stats/top/role cards
- settings.py — private game settings
- tickets.py — ticket/economy commands
- callbacks.py — remaining general callbacks

The package re-exports the original public names and shares one Router.
