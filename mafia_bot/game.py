import random
from dataclasses import dataclass, field


MIN_PLAYERS = 4


@dataclass
class Player:
    user_id: int
    full_name: str
    role: str = ""
    alive: bool = True


@dataclass
class GameRoom:
    chat_id: int
    host_id: int
    players: dict[int, Player] = field(default_factory=dict)
    started: bool = False

    def add_player(self, user_id: int, full_name: str) -> tuple[bool, str]:
        if self.started:
            return False, "Игра уже началась."
        if user_id in self.players:
            return False, "Ты уже в лобби."
        self.players[user_id] = Player(user_id=user_id, full_name=full_name)
        return True, "Игрок добавлен."

    def remove_player(self, user_id: int) -> tuple[bool, str]:
        if user_id not in self.players:
            return False, "Тебя нет в лобби."
        del self.players[user_id]
        return True, "Игрок удален из лобби."

    def assign_roles(self) -> None:
        count = len(self.players)
        mafia_count = 1 if count <= 6 else 2
        roles = ["Мафия"] * mafia_count + ["Мирный"] * (count - mafia_count)
        random.shuffle(roles)

        for player, role in zip(self.players.values(), roles):
            player.role = role

        self.started = True

    def lobby_text(self) -> str:
        if not self.players:
            return "Лобби пустое."

        lines = ["<b>Лобби Мафии</b>", f"Игроков: {len(self.players)}"]
        for i, player in enumerate(self.players.values(), start=1):
            lines.append(f"{i}. {player.full_name}")
        return "\n".join(lines)


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
