"""Serialize night callbacks with the same lock as phase transitions."""

from contextvars import ContextVar


_active_guard = ContextVar("night_callback_guard", default=None)


def defer_night_finish(room):
    guard = _active_guard.get()
    if guard is not None and guard[0] is room:
        guard[1] = True
        return True
    return False


async def guarded_night_callback(callback, chat_id, handler, storage, lock_getter, finish):
    original_room = storage.get_room(chat_id)
    original_round = getattr(original_room, "round_no", None)
    original_started_at = getattr(original_room, "phase_started_at", None)
    guard = [original_room, False]
    async with lock_getter(chat_id):
        room = storage.get_room(chat_id)
        if (
            room is None
            or room is not original_room
            or room.phase != "night"
            or room.round_no != original_round
            or room.phase_started_at != original_started_at
        ):
            await callback.answer("Ночь уже завершена. Дождись следующей фазы.", show_alert=True)
            return

        token = _active_guard.set(guard)
        try:
            await handler(callback)
        finally:
            _active_guard.reset(token)

    # The phase finisher acquires this same lock; never call it while holding it.
    if guard[1]:
        await finish(callback.bot, room)
