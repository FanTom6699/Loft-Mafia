"""Runtime storage boundary for Mafia game rooms.

The room state itself is still backed by the compatibility model during the
migration. Keeping storage here lets callers move to the domain package
without changing the public ``mafia_bot.game`` import path.
"""

from .models import GameRoom


class GameStorage:
    def __init__(self) -> None:
        self.rooms: dict[int, GameRoom] = {}

    def create_room(self, chat_id: int, host_id: int) -> tuple[bool, str]:
        if chat_id in self.rooms:
            return False, "Лобби уже существует в этом чате."
        self.rooms[chat_id] = GameRoom(chat_id=chat_id, host_id=host_id)
        return True, "Лобби создано."

    def get_room(self, chat_id: int) -> GameRoom | None:
        return self.rooms.get(chat_id)

    def close_room(self, chat_id: int) -> None:
        self.rooms.pop(chat_id, None)
