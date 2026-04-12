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

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {str(row[1]) for row in rows}
        if column in existing:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                    money INTEGER NOT NULL DEFAULT 0,
                    tickets INTEGER NOT NULL DEFAULT 0,
                    buff_documents INTEGER NOT NULL DEFAULT 0,
                    buff_shield INTEGER NOT NULL DEFAULT 0,
                    buff_active_role INTEGER NOT NULL DEFAULT 0,
                    last_role TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(conn, "player_stats", "money", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "player_stats", "tickets", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "player_stats", "buff_documents", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "player_stats", "buff_shield", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "player_stats", "buff_active_role", "INTEGER NOT NULL DEFAULT 0")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS private_users (
                    user_id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            self._ensure_column(conn, "private_users", "username", "TEXT NOT NULL DEFAULT ''")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_settings (
                    chat_id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
            "settings": room.settings,
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
            "night_skipped_user_ids": sorted(room.night_skipped_user_ids),
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
            "doctor_self_heal_used": room.doctor_self_heal_used,
            "lucky_save_used": room.lucky_save_used,
            "commissar_action_mode": room.commissar_action_mode,
            "commissar_target_id": room.commissar_target_id,
            "commissar_shot_target_id": room.commissar_shot_target_id,
            "commissar_known_roles": room.commissar_known_roles,
            "pending_sergeant_check": room.pending_sergeant_check,
            "advocate_target_id": room.advocate_target_id,
            "maniac_target_id": room.maniac_target_id,
            "mistress_target_id": room.mistress_target_id,
            "mistress_last_target_id": room.mistress_last_target_id,
            "bum_target_id": room.bum_target_id,
            "bum_last_target_id": room.bum_last_target_id,
            "kamikaze_pending_user_id": room.kamikaze_pending_user_id,
            "kamikaze_target_id": room.kamikaze_target_id,
            "documented_user_ids": sorted(room.documented_user_ids),
            "spent_documents_user_ids": sorted(room.spent_documents_user_ids),
            "shielded_user_ids": sorted(room.shielded_user_ids),
            "spent_shield_user_ids": sorted(room.spent_shield_user_ids),
            "active_role_queued_user_ids": sorted(room.active_role_queued_user_ids),
            "active_role_triggered_user_ids": sorted(room.active_role_triggered_user_ids),
            "active_role_failed_user_ids": sorted(room.active_role_failed_user_ids),
            "night_missed_streaks": room.night_missed_streaks,
            "afk_killed_user_ids": sorted(room.afk_killed_user_ids),
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
            settings=dict(payload.get("settings", {}) or {}),
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
        room.night_skipped_user_ids = {int(v) for v in payload.get("night_skipped_user_ids", [])}
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
        room.doctor_self_heal_used = bool(payload.get("doctor_self_heal_used", False))
        room.lucky_save_used = bool(payload.get("lucky_save_used", False))
        room.commissar_action_mode = payload.get("commissar_action_mode")
        room.commissar_target_id = payload.get("commissar_target_id")
        room.commissar_shot_target_id = payload.get("commissar_shot_target_id")
        room.commissar_known_roles = {
            int(k): str(v)
            for k, v in payload.get("commissar_known_roles", {}).items()
        }
        raw_pending_sergeant_check = payload.get("pending_sergeant_check")
        if isinstance(raw_pending_sergeant_check, dict):
            target_user_id = raw_pending_sergeant_check.get("target_user_id")
            result_role = raw_pending_sergeant_check.get("result_role")
            if isinstance(target_user_id, int) and isinstance(result_role, str):
                room.pending_sergeant_check = {
                    "target_user_id": target_user_id,
                    "result_role": result_role,
                }
            else:
                room.pending_sergeant_check = None
        else:
            room.pending_sergeant_check = None
        room.advocate_target_id = payload.get("advocate_target_id")
        room.maniac_target_id = payload.get("maniac_target_id")
        room.mistress_target_id = payload.get("mistress_target_id")
        room.mistress_last_target_id = payload.get("mistress_last_target_id")
        room.bum_target_id = payload.get("bum_target_id")
        room.bum_last_target_id = payload.get("bum_last_target_id")
        room.kamikaze_pending_user_id = payload.get("kamikaze_pending_user_id")
        room.kamikaze_target_id = payload.get("kamikaze_target_id")
        room.documented_user_ids = {int(v) for v in payload.get("documented_user_ids", [])}
        room.spent_documents_user_ids = {int(v) for v in payload.get("spent_documents_user_ids", [])}
        room.shielded_user_ids = {int(v) for v in payload.get("shielded_user_ids", [])}
        room.spent_shield_user_ids = {int(v) for v in payload.get("spent_shield_user_ids", [])}
        room.active_role_queued_user_ids = {int(v) for v in payload.get("active_role_queued_user_ids", [])}
        room.active_role_triggered_user_ids = {int(v) for v in payload.get("active_role_triggered_user_ids", [])}
        room.active_role_failed_user_ids = {int(v) for v in payload.get("active_role_failed_user_ids", [])}
        room.night_missed_streaks = {
            int(k): int(v)
            for k, v in payload.get("night_missed_streaks", {}).items()
        }
        room.afk_killed_user_ids = {int(v) for v in payload.get("afk_killed_user_ids", [])}

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
        win_money = 10
        with sqlite3.connect(self.db_path) as conn:
            for player in room.players.values():
                won = self._did_player_win(player.role, room.winner_team)
                survived = 1 if player.alive else 0
                suicide_personal_win = 1 if player.user_id in room.suicide_winners else 0
                mafia_game = 1 if player.role in MAFIA_ROLES else 0
                maniac_game = 1 if player.role == ROLE_MANIAC else 0
                civilian_game = 1 if player.role not in MAFIA_ROLES and player.role != ROLE_MANIAC else 0
                money_award = win_money if won and player.alive else 0

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
                        money,
                        tickets,
                        last_role,
                        updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        money = money + excluded.money,
                        tickets = tickets + excluded.tickets,
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
                        money_award,
                        0,
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
                    money,
                    tickets,
                    buff_documents,
                    buff_shield,
                    buff_active_role,
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

    def _ensure_player_stats_row(self, conn: sqlite3.Connection, user_id: int, display_name: str) -> None:
        now = datetime.now().isoformat()
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
                money,
                tickets,
                buff_documents,
                buff_shield,
                buff_active_role,
                last_role,
                updated_at
            )
            VALUES(?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '', ?)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (user_id, display_name, now),
        )

    def purchase_buff(
        self,
        user_id: int,
        display_name: str,
        *,
        inventory_column: str,
        currency_column: str,
        price: int,
        currency_label: str,
    ) -> tuple[bool, str, dict | None]:
        allowed_inventory_columns = {"buff_documents", "buff_shield", "buff_active_role"}
        allowed_currency_columns = {"money", "tickets"}
        if inventory_column not in allowed_inventory_columns or currency_column not in allowed_currency_columns:
            return False, "Некорректная покупка.", self.get_player_stats(user_id)

        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            self._ensure_player_stats_row(conn, user_id, display_name)
            row = conn.execute(
                f"SELECT {currency_column} FROM player_stats WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            balance = int(row[0]) if row is not None else 0
            if balance < price:
                return False, f"Не хватает {currency_label}.", self.get_player_stats(user_id)

            conn.execute(
                f"""
                UPDATE player_stats
                SET display_name = ?,
                    {currency_column} = {currency_column} - ?,
                    {inventory_column} = {inventory_column} + 1,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (display_name, price, now, user_id),
            )
            conn.commit()
        return True, "Покупка выполнена.", self.get_player_stats(user_id)

    def purchase_shield_buff(self, user_id: int, display_name: str, price: int = 100) -> tuple[bool, str, dict | None]:
        return self.purchase_buff(
            user_id,
            display_name,
            inventory_column="buff_shield",
            currency_column="money",
            price=price,
            currency_label="денег",
        )

    def adjust_player_currency(
        self,
        user_id: int,
        display_name: str,
        *,
        currency_column: str,
        delta: int,
        insufficient_label: str,
    ) -> tuple[bool, str, dict | None]:
        allowed_currency_columns = {"money", "tickets"}
        if currency_column not in allowed_currency_columns:
            return False, "Некорректное изменение баланса.", self.get_player_stats(user_id)

        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            self._ensure_player_stats_row(conn, user_id, display_name)
            row = conn.execute(
                f"SELECT {currency_column} FROM player_stats WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            balance = int(row[0]) if row is not None else 0
            updated_balance = balance + int(delta)
            if updated_balance < 0:
                return False, f"Не хватает {insufficient_label}.", self.get_player_stats(user_id)

            conn.execute(
                f"""
                UPDATE player_stats
                SET display_name = ?,
                    {currency_column} = ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (display_name, updated_balance, now, user_id),
            )
            conn.commit()

        return True, "Баланс обновлен.", self.get_player_stats(user_id)

    def adjust_player_tickets(self, user_id: int, display_name: str, delta: int) -> tuple[bool, str, dict | None]:
        return self.adjust_player_currency(
            user_id,
            display_name,
            currency_column="tickets",
            delta=delta,
            insufficient_label="билетиков",
        )

    def transfer_player_tickets(
        self,
        sender_user_id: int,
        sender_display_name: str,
        recipient_user_id: int,
        recipient_display_name: str,
        amount: int,
    ) -> tuple[bool, str, dict | None]:
        transfer_amount = int(amount)
        if transfer_amount <= 0:
            return False, "Число билетиков должно быть больше нуля.", self.get_player_stats(recipient_user_id)

        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            self._ensure_player_stats_row(conn, sender_user_id, sender_display_name)
            self._ensure_player_stats_row(conn, recipient_user_id, recipient_display_name)

            row = conn.execute(
                "SELECT tickets FROM player_stats WHERE user_id = ?",
                (sender_user_id,),
            ).fetchone()
            sender_balance = int(row[0]) if row is not None else 0
            if sender_balance < transfer_amount:
                return False, "Не хватает билетиков.", self.get_player_stats(recipient_user_id)

            conn.execute(
                """
                UPDATE player_stats
                SET display_name = ?,
                    tickets = tickets - ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (sender_display_name, transfer_amount, now, sender_user_id),
            )
            conn.execute(
                """
                UPDATE player_stats
                SET display_name = ?,
                    tickets = tickets + ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (recipient_display_name, transfer_amount, now, recipient_user_id),
            )
            conn.commit()

        return True, "Баланс обновлен.", self.get_player_stats(recipient_user_id)

    def consume_buff(self, user_id: int, *, inventory_column: str) -> bool:
        allowed_inventory_columns = {"buff_documents", "buff_shield", "buff_active_role"}
        if inventory_column not in allowed_inventory_columns:
            return False

        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                f"""
                UPDATE player_stats
                SET {inventory_column} = {inventory_column} - 1,
                    updated_at = ?
                WHERE user_id = ? AND {inventory_column} > 0
                """,
                (now, user_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def consume_shield_buff(self, user_id: int) -> bool:
        return self.consume_buff(user_id, inventory_column="buff_shield")

    def consume_documents_buff(self, user_id: int) -> bool:
        return self.consume_buff(user_id, inventory_column="buff_documents")

    def touch_private_user(self, user_id: int, display_name: str, username: str | None = None) -> bool:
        now = datetime.now().isoformat()
        normalized_username = (username or "").strip().lstrip("@")
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
                        username,
                        first_seen_at,
                        last_seen_at
                    )
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (user_id, display_name, normalized_username, now, now),
                )
                conn.commit()
                return True

            conn.execute(
                """
                UPDATE private_users
                SET display_name = ?, username = ?, last_seen_at = ?
                WHERE user_id = ?
                """,
                (display_name, normalized_username, now, user_id),
            )
            conn.commit()
        return False

    def has_private_user(self, user_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT 1 FROM private_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return row is not None

    def get_private_user_by_username(self, username: str) -> dict | None:
        normalized_username = (username or "").strip().lstrip("@").lower()
        if not normalized_username:
            return None

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT user_id, display_name, username
                FROM private_users
                WHERE lower(username) = ?
                """,
                (normalized_username,),
            ).fetchone()

        if row is None:
            return None

        return {
            "user_id": int(row[0]),
            "display_name": str(row[1] or "").strip(),
            "username": str(row[2] or "").strip(),
        }

    def get_chat_settings(self, chat_id: int) -> dict | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT payload FROM chat_settings WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def save_chat_settings(self, chat_id: int, settings: dict) -> None:
        encoded = json.dumps(settings, ensure_ascii=False)
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO chat_settings(chat_id, payload, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (chat_id, encoded, now),
            )
            conn.commit()

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
