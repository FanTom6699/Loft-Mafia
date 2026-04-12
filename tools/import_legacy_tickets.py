import argparse
import os
import sqlite3
from datetime import datetime


def default_target_db() -> str:
    base_dir = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_dir, "mafia_state.db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import ticket balances from a legacy SQLite bot database by Telegram user_id.",
    )
    parser.add_argument("source_db", help="Path to the legacy SQLite database file")
    parser.add_argument(
        "--target-db",
        dest="target_db",
        default=os.getenv("MAFIA_STATE_DB", default_target_db()),
        help="Path to the current Loft Mafia SQLite database",
    )
    parser.add_argument(
        "--mode",
        choices=["add", "replace"],
        default="add",
        help="How to import balances into player_stats.tickets: add to current value or replace it",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to the target database. Without this flag the script only shows a preview.",
    )
    return parser.parse_args()


def fetch_legacy_rows(source_db: str) -> list[tuple[int, str, int]]:
    with sqlite3.connect(source_db) as conn:
        rows = conn.execute(
            """
            SELECT user_id, COALESCE(username, ''), COALESCE(balance, 0)
            FROM users
            WHERE COALESCE(balance, 0) > 0
            ORDER BY user_id
            """
        ).fetchall()
    return [(int(user_id), str(username or "").strip(), int(balance)) for user_id, username, balance in rows]


def ensure_target_schema(conn: sqlite3.Connection) -> None:
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


def import_rows(rows: list[tuple[int, str, int]], target_db: str, mode: str, apply_changes: bool) -> tuple[int, int, int]:
    changed_rows = 0
    total_tickets = 0
    skipped_unregistered = 0
    now = datetime.now().isoformat()

    with sqlite3.connect(target_db) as conn:
        ensure_target_schema(conn)

        for user_id, username, legacy_balance in rows:
            private_user_row = conn.execute(
                "SELECT display_name FROM private_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if private_user_row is None:
                skipped_unregistered += 1
                continue

            display_name = username or f"Игрок {user_id}"
            if private_user_row[0]:
                display_name = str(private_user_row[0]).strip() or display_name
            row = conn.execute(
                "SELECT tickets FROM player_stats WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            current_tickets = int(row[0]) if row is not None else 0
            new_tickets = legacy_balance if mode == "replace" else current_tickets + legacy_balance

            if new_tickets == current_tickets and row is not None:
                continue

            changed_rows += 1
            total_tickets += legacy_balance
            print(
                f"user_id={user_id} username=@{username or '-'} current={current_tickets} legacy={legacy_balance} result={new_tickets}"
            )

            if not apply_changes:
                continue

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
                VALUES(?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, 0, 0, 0, '', ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    tickets = excluded.tickets,
                    updated_at = excluded.updated_at
                """,
                (user_id, display_name, new_tickets, now),
            )

        if apply_changes:
            conn.commit()

    return changed_rows, total_tickets, skipped_unregistered


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.source_db):
        print(f"Source DB not found: {args.source_db}")
        return 1

    if not os.path.exists(args.target_db):
        print(f"Target DB not found: {args.target_db}")
        return 1

    rows = fetch_legacy_rows(args.source_db)
    if not rows:
        print("No positive balances found in legacy database")
        return 0

    changed_rows, total_tickets, skipped_unregistered = import_rows(rows, args.target_db, args.mode, args.apply)
    action = "Applied" if args.apply else "Previewed"
    print(f"{action} {changed_rows} rows, legacy total={total_tickets}")
    print(f"Skipped unregistered users: {skipped_unregistered}")
    if not args.apply:
        print("Dry run only. Re-run with --apply to write changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())