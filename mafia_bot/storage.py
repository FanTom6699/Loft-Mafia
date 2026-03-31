import json
import os
import sqlite3
from datetime import datetime

from mafia_bot.game import MAFIA_ROLES, ROLE_MANIAC, GameRoom, Player


def _default_db_path() -> str:
    base_dir = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_dir, "mafia_state.db")


class GameStateRepository:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or os.getenv("MAFIA_STATE_DB", _default_db_path())
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rooms (
                    chat_id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_stats (
                    user_id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    games_played INTEGER NOT NULL DEFAULT 0,
                    wins INTEGER NOT NULL DEFAULT 0,
                    losses INTEGER NOT NULL DEFAULT 0,
                    survived_games INTEGER NOT NULL DEFAULT 0,
                    suicide_personal_wins INTEGER NOT NULL DEFAULT 0,
                    mafia_games INTEGER NOT NULL DEFAULT 0,
                    maniac_games INTEGER NOT NULL DEFAULT 0,
                    civilian_games INTEGER NOT NULL DEFAULT 0,
                    last_role TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS private_users (
                    user_id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    @staticmethod
    def _dt_to_str(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    @staticmethod
    def _str_to_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _serialize_room(room: GameRoom) -> dict:
        return {
            "chat_id": room.chat_id,
            "host_id": room.host_id,
            "chat_title": room.chat_title,
            "players": {
                str(user_id): {
                    "user_id": player.user_id,
                    "full_name": player.full_name,
                    "role": player.role,
                    "alive": player.alive,
                }
                for user_id, player in room.players.items()
            },
            "started": room.started,
            "phase": room.phase,
            "round_no": room.round_no,
            "registration_open": room.registration_open,
            "registration_extensions": room.registration_extensions,
            "registration_message_id": room.registration_message_id,
            "night_votes": room.night_votes,
            "mafia_vote_locked": room.mafia_vote_locked,
            "mafia_target_announced": room.mafia_target_announced,
            "announced_night_roles": sorted(room.announced_night_roles),
            "last_don_successor_id": room.last_don_successor_id,
            "day_stage": room.day_stage,
            "day_votes": room.day_votes,
            "trial_candidate_id": room.trial_candidate_id,
            "trial_votes": room.trial_votes,
            "night_kill_sources": room.night_kill_sources,
            "day_silenced_user_id": room.day_silenced_user_id,
            "doctor_target_id": room.doctor_target_id,
            "commissar_target_id": room.commissar_target_id,
            "advocate_target_id": room.advocate_target_id,
            "maniac_target_id": room.maniac_target_id,
            "mistress_target_id": room.mistress_target_id,
            "mistress_last_target_id": room.mistress_last_target_id,
            "bum_target_id": room.bum_target_id,
            "kamikaze_pending_user_id": room.kamikaze_pending_user_id,
            "kamikaze_target_id": room.kamikaze_target_id,
            "night_reports": room.night_reports,
            "pending_last_words": sorted(room.pending_last_words),
            "used_last_words": sorted(room.used_last_words),
            "last_words_log": room.last_words_log,
            "phase_started_at": GameStateRepository._dt_to_str(room.phase_started_at),
            "phase_duration_seconds": room.phase_duration_seconds,
            "stats_recorded": room.stats_recorded,
            "suicide_winners": sorted(room.suicide_winners),
            "winner_team": room.winner_team,
            "started_at": GameStateRepository._dt_to_str(room.started_at),
            "finished_at": GameStateRepository._dt_to_str(room.finished_at),
        }

    @staticmethod
    def _deserialize_room(payload: dict) -> GameRoom:
        room = GameRoom(
            chat_id=int(payload["chat_id"]),
            host_id=int(payload["host_id"]),
            chat_title=payload.get("chat_title", ""),
        )

        players_payload = payload.get("players", {})
        room.players = {
            int(user_id): Player(
                user_id=int(player.get("user_id", user_id)),
                full_name=player.get("full_name", ""),
                role=player.get("role", ""),
                alive=bool(player.get("alive", True)),
            )
            for user_id, player in players_payload.items()
        }

        room.started = bool(payload.get("started", False))
        room.phase = payload.get("phase", room.phase)
        room.round_no = int(payload.get("round_no", 0))
        room.registration_open = bool(payload.get("registration_open", False))
        room.registration_extensions = int(payload.get("registration_extensions", 0))
        room.registration_message_id = payload.get("registration_message_id")

        room.night_votes = {int(k): int(v) for k, v in payload.get("night_votes", {}).items()}
        room.mafia_vote_locked = bool(payload.get("mafia_vote_locked", False))
        room.mafia_target_announced = bool(payload.get("mafia_target_announced", False))
        room.announced_night_roles = set(payload.get("announced_night_roles", []))
        room.last_don_successor_id = payload.get("last_don_successor_id")

        room.day_stage = payload.get("day_stage")
        room.day_votes = {int(k): int(v) for k, v in payload.get("day_votes", {}).items()}
        room.trial_candidate_id = payload.get("trial_candidate_id")
        room.trial_votes = {int(k): bool(v) for k, v in payload.get("trial_votes", {}).items()}

        room.night_kill_sources = {
            int(k): [str(source) for source in v]
            for k, v in payload.get("night_kill_sources", {}).items()
        }
        room.day_silenced_user_id = payload.get("day_silenced_user_id")

        room.doctor_target_id = payload.get("doctor_target_id")
        room.commissar_target_id = payload.get("commissar_target_id")
        room.advocate_target_id = payload.get("advocate_target_id")
        room.maniac_target_id = payload.get("maniac_target_id")
        room.mistress_target_id = payload.get("mistress_target_id")
        room.mistress_last_target_id = payload.get("mistress_last_target_id")
        room.bum_target_id = payload.get("bum_target_id")
        room.kamikaze_pending_user_id = payload.get("kamikaze_pending_user_id")
        room.kamikaze_target_id = payload.get("kamikaze_target_id")

        room.night_reports = {
            int(k): [str(line) for line in lines]
            for k, lines in payload.get("night_reports", {}).items()
        }

        room.pending_last_words = {int(v) for v in payload.get("pending_last_words", [])}
        room.used_last_words = {int(v) for v in payload.get("used_last_words", [])}
        room.last_words_log = {int(k): str(v) for k, v in payload.get("last_words_log", {}).items()}
        room.phase_started_at = GameStateRepository._str_to_dt(payload.get("phase_started_at"))
        phase_duration = payload.get("phase_duration_seconds")
        room.phase_duration_seconds = int(phase_duration) if phase_duration is not None else None
        room.stats_recorded = bool(payload.get("stats_recorded", False))

        room.suicide_winners = {int(v) for v in payload.get("suicide_winners", [])}
        room.winner_team = payload.get("winner_team")
        room.started_at = GameStateRepository._str_to_dt(payload.get("started_at"))
        room.finished_at = GameStateRepository._str_to_dt(payload.get("finished_at"))
        return room

    @staticmethod
    def _did_player_win(role: str, winner_team: str | None) -> bool:
        if winner_team == "Мафия":
            return role in MAFIA_ROLES
        if winner_team == "Маньяк":
            return role == ROLE_MANIAC
        if winner_team == "Мирные жители":
            return role not in MAFIA_ROLES and role != ROLE_MANIAC
        return False

    def record_finished_game_stats(self, room: GameRoom) -> None:
        if room.winner_team is None:
            return

        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            for player in room.players.values():
                won = self._did_player_win(player.role, room.winner_team)
                survived = 1 if player.alive else 0
                suicide_personal_win = 1 if player.user_id in room.suicide_winners else 0
                mafia_game = 1 if player.role in MAFIA_ROLES else 0
                maniac_game = 1 if player.role == ROLE_MANIAC else 0
                civilian_game = 1 if player.role not in MAFIA_ROLES and player.role != ROLE_MANIAC else 0

                conn.execute(
                    """
                    INSERT INTO player_stats(
                        user_id,
                        display_name,
                        games_played,
                        wins,
                        losses,
                        survived_games,
                        suicide_personal_wins,
                        mafia_games,
                        maniac_games,
                        civilian_games,
                        last_role,
                        updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        display_name = excluded.display_name,
                        games_played = games_played + excluded.games_played,
                        wins = wins + excluded.wins,
                        losses = losses + excluded.losses,
                        survived_games = survived_games + excluded.survived_games,
                        suicide_personal_wins = suicide_personal_wins + excluded.suicide_personal_wins,
                        mafia_games = mafia_games + excluded.mafia_games,
                        maniac_games = maniac_games + excluded.maniac_games,
                        civilian_games = civilian_games + excluded.civilian_games,
                        last_role = excluded.last_role,
                        updated_at = excluded.updated_at
                    """,
                    (
                        player.user_id,
                        player.full_name,
                        1,
                        1 if won else 0,
                        0 if won else 1,
                        survived,
                        suicide_personal_win,
                        mafia_game,
                        maniac_game,
                        civilian_game,
                        player.role,
                        now,
                    ),
                )
            conn.commit()

    def get_player_stats(self, user_id: int) -> dict | None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT
                    user_id,
                    display_name,
                    games_played,
                    wins,
                    losses,
                    survived_games,
                    suicide_personal_wins,
                    mafia_games,
                    maniac_games,
                    civilian_games,
                    last_role,
                    updated_at
                FROM player_stats
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        return dict(row)

    def touch_private_user(self, user_id: int, display_name: str) -> bool:
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT user_id FROM private_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO private_users(
                        user_id,
                        display_name,
                        first_seen_at,
                        last_seen_at
                    )
                    VALUES(?, ?, ?, ?)
                    """,
                    (user_id, display_name, now, now),
                )
                conn.commit()
                return True

            conn.execute(
                """
                UPDATE private_users
                SET display_name = ?, last_seen_at = ?
                WHERE user_id = ?
                """,
                (display_name, now, user_id),
            )
            conn.commit()
        return False

    def save_room(self, room: GameRoom) -> None:
        payload = self._serialize_room(room)
        encoded = json.dumps(payload, ensure_ascii=False)
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO rooms(chat_id, payload, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (room.chat_id, encoded, now),
            )
            conn.commit()

    def delete_room(self, chat_id: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM rooms WHERE chat_id = ?", (chat_id,))
            conn.commit()

    def load_rooms(self) -> dict[int, GameRoom]:
        rooms: dict[int, GameRoom] = {}
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT payload FROM rooms").fetchall()

        for (encoded,) in rows:
            try:
                payload = json.loads(encoded)
                room = self._deserialize_room(payload)
                rooms[room.chat_id] = room
            except Exception:
                continue
        return rooms
