"""Self-playing Mafia simulator for exercising the game domain without Telegram.

Usage:
    python scripts/simulate_games.py --games 1 --verbose --show-roles
    python scripts/simulate_games.py --games 100 --seed 20260929
    python scripts/simulate_games.py --games 20 --chaos --verbose

The simulator deliberately uses the real GameRoom domain methods. It does not
talk to Telegram, so it is intended to expose domain/state bugs while keeping
the run fast and reproducible.
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from pathlib import Path

from mafia_bot.game_domain import (
    DAY_STAGE_DISCUSSION,
    DAY_STAGE_NOMINATION,
    DAY_STAGE_TRIAL,
    MAFIA_ROLES,
    PHASE_DAY,
    PHASE_FINISHED,
    PHASE_NIGHT,
    ROLE_ADVOCATE,
    ROLE_BUM,
    ROLE_CITIZEN,
    ROLE_COMMISSAR,
    ROLE_DOCTOR,
    ROLE_DON,
    ROLE_KAMIKAZE,
    ROLE_LUCKY,
    ROLE_MAFIA,
    ROLE_MANIAC,
    ROLE_MISTRESS,
    ROLE_SERGEANT,
    ROLE_SUICIDE,
    GameRoom,
    Player,
)


ROLE_NAMES = {
    ROLE_DON: "🤵🏻 Дон",
    ROLE_MAFIA: "🤵🏼 Мафия",
    ROLE_MANIAC: "🔪 Маньяк",
    ROLE_COMMISSAR: "🕵️‍♂️ Комиссар",
    ROLE_DOCTOR: "👨🏼‍⚕️ Доктор",
    ROLE_MISTRESS: "💃🏼 Любовница",
    ROLE_BUM: "🧙🏼‍♂️ Бомж",
    ROLE_ADVOCATE: "👨🏼‍💼 Адвокат",
    ROLE_SERGEANT: "👮🏼‍♂️ Сержант",
    ROLE_SUICIDE: "☠️ Самоубийца",
    ROLE_LUCKY: "🤞 Счастливчик",
    ROLE_KAMIKAZE: "💣 Камикадзе",
    ROLE_CITIZEN: "👨🏼 Мирный",
}


@dataclass
class SimulationResult:
    game_no: int
    winner: str
    nights: int
    deaths: int
    last_words: int


class GameObserver:
    def __init__(self, verbose: bool, show_roles: bool, log_file: Path | None) -> None:
        self.verbose = verbose
        self.show_roles = show_roles
        self.log_file = log_file
        self.lines: list[str] = []

    def write(self, line: str = "") -> None:
        self.lines.append(line)
        if self.verbose:
            print(line)
        if self.log_file is not None:
            self.log_file.write_text("\n".join(self.lines) + "\n", encoding="utf-8")


class GameSimulator:
    def __init__(
        self,
        game_no: int,
        rng: random.Random,
        *,
        verbose: bool,
        show_roles: bool,
        chaos: bool,
        log_file: Path | None,
    ) -> None:
        self.game_no = game_no
        self.rng = rng
        self.chaos = chaos
        self.observer = GameObserver(verbose, show_roles, log_file)
        self.room = GameRoom(chat_id=10_000 + game_no, host_id=1)
        self._next_player_id = 1

    def _new_player(self) -> Player:
        player = Player(
            user_id=self._next_player_id,
            full_name=f"Бот {self._next_player_id}",
        )
        self._next_player_id += 1
        return player

    def setup(self, player_count: int) -> None:
        self.room.open_registration()
        for _ in range(player_count):
            player = self._new_player()
            self.room.add_player(player.user_id, player.full_name)
        self.room.assign_roles()
        self.observer.write(f"===== ИГРА #{self.game_no} =====")
        self.observer.write(f"Игроков: {player_count}")
        self.observer.write(f"Роли: {', '.join(self._role_counts())}")
        if self.observer.show_roles:
            for player in self.room.players.values():
                self.observer.write(
                    f"  {player.full_name}: {ROLE_NAMES.get(player.role, player.role)}"
                )
        self.observer.write("")

    def _role_counts(self) -> list[str]:
        counts: dict[str, int] = {}
        for player in self.room.players.values():
            counts[player.role] = counts.get(player.role, 0) + 1
        return [
            f"{ROLE_NAMES.get(role, role)} x{count}"
            for role, count in sorted(counts.items())
        ]

    def _alive_others(self, player: Player) -> list[Player]:
        return [p for p in self.room.alive_players() if p.user_id != player.user_id]

    def _random_target(self, player: Player) -> int | None:
        targets = self._alive_others(player)
        if not targets:
            return None
        return self.rng.choice(targets).user_id

    def _mafia_target(self, player: Player) -> int | None:
        targets = [
            p for p in self._alive_others(player)
            if p.role not in MAFIA_ROLES
        ]
        if not targets:
            targets = self._alive_others(player)
        if not targets:
            return None
        return self.rng.choice(targets).user_id

    def _day_target(self, player: Player) -> int | None:
        targets = self._alive_others(player)
        if not targets:
            return None

        # Make the day phase more useful for testing: mafia avoid their own
        # team, while civilians sample from everyone without seeing roles.
        if player.role in MAFIA_ROLES:
            civilian_targets = [p for p in targets if p.role not in MAFIA_ROLES]
            if civilian_targets:
                targets = civilian_targets
        return self.rng.choice(targets).user_id

    def _maybe_skip(self, player: Player) -> bool:
        if not self.chaos:
            return False
        # Small fault-injection rate exercises the game's AFK death path.
        return self.rng.random() < 0.04

    def _perform_night_actions(self) -> None:
        alive = list(self.room.alive_players())
        for player in alive:
            if not player.alive or self._maybe_skip(player):
                continue

            target_id = self._random_target(player)
            if player.role in MAFIA_ROLES:
                target_id = self._mafia_target(player)
                if target_id is not None:
                    self.room.set_night_vote(player.user_id, target_id)
            elif player.role == ROLE_DOCTOR:
                if target_id is not None:
                    self.room.set_doctor_target(player.user_id, target_id)
            elif player.role == ROLE_MANIAC:
                if target_id is not None:
                    self.room.set_maniac_target(player.user_id, target_id)
            elif player.role == ROLE_MISTRESS:
                if target_id is not None:
                    # Retry once if the same target as the previous night is selected.
                    ok, _ = self.room.set_mistress_target(player.user_id, target_id)
                    if not ok:
                        alternatives = [
                            p for p in self._alive_others(player)
                            if p.user_id != self.room.mistress_last_target_id
                        ]
                        if alternatives:
                            self.room.set_mistress_target(
                                player.user_id,
                                self.rng.choice(alternatives).user_id,
                            )
            elif player.role == ROLE_BUM:
                if target_id is not None:
                    ok, _ = self.room.set_bum_target(player.user_id, target_id)
                    if not ok:
                        alternatives = [
                            p for p in self._alive_others(player)
                            if p.user_id != self.room.bum_last_target_id
                        ]
                        if alternatives:
                            self.room.set_bum_target(
                                player.user_id,
                                self.rng.choice(alternatives).user_id,
                            )
            elif player.role == ROLE_ADVOCATE:
                if target_id is not None:
                    self.room.set_advocate_target(player.user_id, target_id)
            elif player.role == ROLE_COMMISSAR:
                # Check first; on later nights sometimes exercise the shooting branch.
                if self.room.round_no >= 2 and self.rng.random() < 0.25:
                    ok, _ = self.room.set_commissar_action_mode(player.user_id, "shoot")
                    if ok:
                        shot_target = self._random_target(player)
                        if shot_target is not None:
                            self.room.set_commissar_shot_target(player.user_id, shot_target)
                    continue

                if self.room.round_no >= 2:
                    self.room.set_commissar_action_mode(player.user_id, "check")
                target = self._random_target(player)
                if target is not None:
                    self.room.check_player_role(player.user_id, target)
            # Sergeant, Suicide, Lucky and Citizen intentionally have no active night action.

        kamikaze_id = self.room.kamikaze_pending_user_id
        if kamikaze_id is not None:
            kamikaze = self.room.get_player(kamikaze_id)
            if kamikaze is not None and kamikaze.alive and not self._maybe_skip(kamikaze):
                target_id = self._random_target(kamikaze)
                if target_id is not None:
                    self.room.set_kamikaze_target(kamikaze.user_id, target_id)

    def _record_last_words_after_night(self, eliminated: list[Player]) -> int:
        queued = self.room.queue_last_words(eliminated)
        if not queued:
            return 0

        submitted = 0
        for user_id in queued:
            player = self.room.get_player(user_id)
            if player is None:
                continue
            ok, payload = self.room.consume_last_word(
                user_id,
                f"Я умер в ночь {self.room.last_word_death_nights[user_id]}.",
            )
            if not ok:
                raise AssertionError(
                    f"Last word could not be consumed for user {user_id}: {payload}"
                )

            # Simulate a late/night submission for roughly half of the deaths.
            if self.rng.random() < 0.5:
                public_text = self.room.last_word_public_text(player, payload)
                self.room.queue_last_word_for_day(user_id, public_text)
                self.observer.write(
                    f"☠️ {player.full_name}: предсмертное отправлено поздно -> очередь дня"
                )
            else:
                public_text = self.room.last_word_public_text(player, payload)
                if "<i>" not in public_text:
                    raise AssertionError("Last-word text must be italic")
                self.observer.write(
                    f"☠️ {player.full_name}: предсмертное принято"
                )
            submitted += 1
        return submitted

    def _release_queued_last_words(self) -> None:
        queued = self.room.pop_last_words_for_day()
        for public_text in queued.values():
            if "<i>" not in public_text:
                raise AssertionError("Queued last-word public text lost italic formatting")
            self.observer.write(f"💬 ОПУБЛИКОВАНО НА ДНЕ: {public_text}")

    def _day_phase(self) -> None:
        self.room.phase = PHASE_DAY
        self.room.start_day_discussion()
        self.observer.write(f"☀️ День {self.room.round_no}: обсуждение")

        # Deliver delayed last words only at the start of daytime discussion.
        self._release_queued_last_words()

        self.room.start_day_nomination()
        self.observer.write(f"🗳 День {self.room.round_no}: номинация")
        eligible = list(self.room.alive_players())
        for player in eligible:
            if self._maybe_skip(player):
                continue
            target_id = self._day_target(player)
            if target_id is not None:
                self.room.set_day_vote(player.user_id, target_id)

        ok, candidate_id = self.room.resolve_day_nomination()
        if not ok:
            raise AssertionError("Day nomination failed")

        if candidate_id is None:
            self.observer.write("🗿 Никого не выбрали -> следующая ночь")
            ok_end, _ = self.room.end_day_no_lynch()
            if not ok_end:
                raise AssertionError("Day without lynch failed")
            return

        candidate = self.room.get_player(candidate_id)
        if candidate is None:
            raise AssertionError("Nomination returned unknown candidate")

        self.room.start_day_trial(candidate.user_id)
        self.observer.write(f"⚖️ Суд: {candidate.full_name} ({ROLE_NAMES.get(candidate.role, candidate.role)})")
        for player in list(self.room.alive_players()):
            if player.user_id == candidate.user_id:
                continue
            if self._maybe_skip(player):
                continue
            approve = self.rng.random() < 0.68
            self.room.set_trial_vote(player.user_id, approve)

        if candidate.role == ROLE_KAMIKAZE:
            self.observer.write("💣 На суде камикадзе под угрозой.")

        result = self.room.resolve_day_trial()
        ok, info, eliminated, *_ = result
        if not ok:
            raise AssertionError(f"Day trial failed: {info}")

        if eliminated:
            self.observer.write(
                "⚰️ Убиты днем: " +
                ", ".join(f"{p.full_name} ({p.role})" for p in eliminated)
            )
        else:
            self.observer.write("🕊 Кандидат пережил голосование")

        # If a kamikaze was lynched, the real game opens a revenge choice in the next night.
        if self.room.phase == PHASE_NIGHT and self.room.kamikaze_pending_user_id is not None:
            self.observer.write("💣 Следующей ночью камикадзе получает ответный выбор.")

    def run(self, player_count: int = 14, max_rounds: int = 60) -> SimulationResult:
        self.setup(player_count)
        last_words = 0
        deaths = 0

        for _ in range(max_rounds):
            if self.room.phase == PHASE_FINISHED:
                break
            if self.room.phase != PHASE_NIGHT:
                raise AssertionError(f"Unexpected phase before night: {self.room.phase}")

            self.observer.write(f"🌙 Ночь {self.room.round_no}")
            before_alive = {p.user_id for p in self.room.alive_players()}
            self._perform_night_actions()
            (
                ok,
                info,
                eliminated,
                don_transfer_note,
                don_successor_id,
                commissar_transfer_note,
                commissar_successor_id,
            ) = self.room.resolve_night()
            if not ok:
                raise AssertionError(f"Night resolution failed: {info}")

            after_alive = {p.user_id for p in self.room.alive_players()}
            night_deaths = len(before_alive - after_alive)
            deaths += night_deaths

            if eliminated:
                self.observer.write(
                    "☠️ Ночная смерть: " +
                    ", ".join(
                        f"{p.full_name} ({ROLE_NAMES.get(p.role, p.role)})"
                        for p in eliminated
                    )
                )
            else:
                self.observer.write("🌃 Никто не умер")

            if don_transfer_note is not None:
                self.observer.write(
                    f"🤵🏻 Наследник Дона: {self.room.get_player(don_successor_id).full_name}"
                    if don_successor_id is not None and self.room.get_player(don_successor_id) is not None
                    else "🤵🏻 Произошло наследование Дона"
                )
            if commissar_transfer_note is not None:
                self.observer.write(
                    f"🕵️‍♂️ Наследник Комиссара: {self.room.get_player(commissar_successor_id).full_name}"
                    if commissar_successor_id is not None and self.room.get_player(commissar_successor_id) is not None
                    else "🕵️‍♂️ Произошло наследование Комиссара"
                )

            last_words += self._record_last_words_after_night(eliminated)
            if self.room.phase == PHASE_FINISHED:
                break

            self._day_phase()
            if self.room.phase == PHASE_FINISHED:
                break

        if self.room.phase != PHASE_FINISHED:
            raise AssertionError(
                f"Game did not finish within {max_rounds} rounds; "
                f"alive={[(p.full_name, p.role) for p in self.room.alive_players()]}"
            )

        self.observer.write(
            f"🏁 Победитель: {self.room.winner_team}; "
            f"ночей={self.room.round_no}; смертей={deaths}; предсмертных={last_words}"
        )
        return SimulationResult(
            game_no=self.game_no,
            winner=str(self.room.winner_team),
            nights=self.room.round_no,
            deaths=deaths,
            last_words=last_words,
        )


def run_simulations(
    games: int,
    seed: int,
    *,
    verbose: bool,
    show_roles: bool,
    chaos: bool,
    max_rounds: int,
    log_path: Path | None,
) -> list[SimulationResult]:
    results: list[SimulationResult] = []
    base_rng = random.Random(seed)

    for game_no in range(1, games + 1):
        if log_path is None:
            per_game_log = None
        elif games == 1:
            per_game_log = log_path
        else:
            per_game_log = log_path.with_name(
                f"{log_path.stem}-{game_no}{log_path.suffix}"
            )

        # assignment.py and night_resolution.py use the module-level random
        # generator, so seed it per game for reproducible full-domain behavior.
        random.seed(base_rng.randrange(0, 2**31))
        game_seed = base_rng.randrange(0, 2**31)
        game_rng = random.Random(game_seed)

        simulator = GameSimulator(
            game_no,
            game_rng,
            verbose=verbose,
            show_roles=show_roles,
            chaos=chaos,
            log_file=per_game_log,
        )
        results.append(simulator.run())

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run self-playing Mafia games")
    parser.add_argument("--games", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20260929)
    parser.add_argument("--players", type=int, default=14)
    parser.add_argument("--max-rounds", type=int, default=60)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--show-roles", action="store_true")
    parser.add_argument("--chaos", action="store_true")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()

    if args.games < 1:
        parser.error("--games must be >= 1")
    if not 4 <= args.players <= 20:
        parser.error("--players must be between 4 and 20")

    results = run_simulations(
        args.games,
        args.seed,
        verbose=args.verbose,
        show_roles=args.show_roles,
        chaos=args.chaos,
        max_rounds=args.max_rounds,
        log_path=args.log,
    )

    winners: dict[str, int] = {}
    for result in results:
        winners[result.winner] = winners.get(result.winner, 0) + 1

    print("")
    print("===== СИМУЛЯЦИЯ ЗАВЕРШЕНА =====")
    print(f"Игр: {len(results)}")
    print("Победители: " + ", ".join(f"{team}={count}" for team, count in sorted(winners.items())))
    print(f"Среднее число ночей: {sum(r.nights for r in results) / len(results):.2f}")
    print(f"Всего смертей: {sum(r.deaths for r in results)}")
    print(f"Всего предсмертных: {sum(r.last_words for r in results)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
