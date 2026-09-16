# Handler refactor — Stage 1

The original implementation is kept in `mafia_bot/_handlers_legacy.py` so runtime behavior stays unchanged. Domain modules re-export selected helpers and handlers.

Next stages can move implementations one domain at a time into these modules and remove the legacy file only after tests pass.
