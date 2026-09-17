"""Night resolution rules for the Mafia game."""

from __future__ import annotations

import random

from ..game_legacy import Player
from .constants import (
    DAY_STAGE_DISCUSSION,
    DOCUMENTS_FAKEABLE_ROLES,
    MAFIA_ROLES,
    PHASE_DAY,
    PHASE_NIGHT,
    ROLE_ADVOCATE,
    ROLE_BUM,
    ROLE_CITIZEN,
    ROLE_COMMISSAR,
    ROLE_DOCTOR,
    ROLE_DON,
    ROLE_KAMIKAZE,
    ROLE_LUCKY,
    ROLE_MANIAC,
    ROLE_MISTRESS,
)
from .roles import (
    action_notifications_from_settings,
    commissar_can_shoot_this_round,
    kamikaze_night_revenge_from_settings,
)


def resolve_night(self) -> tuple[bool, str, list[Player], str | None, int | None, str | None, int | None]:
    if self.phase != PHASE_NIGHT:
        return False, "Сейчас не ночь.", [], None, None, None, None

    self.afk_killed_user_ids.clear()
    self.night_reports.clear()
    self.last_doctor_saved_target_id = None
    self.day_silenced_user_id = None
    self.pending_sergeant_check = None

    mistress = next((p for p in self.alive_players() if p.role == ROLE_MISTRESS), None)
    mistress_target_id: int | None = None
    if mistress is not None and self.mistress_target_id is not None:
        mistress_target_id = self.mistress_target_id
    mistress_effective_target_id = mistress_target_id

    if mistress is not None and mistress_target_id is not None:
        blocked_target = self.get_player(mistress_target_id)
        if blocked_target is not None and blocked_target.alive:
            mistress_killer_bypass = False

            doctor_same_target = self.doctor_target_id == blocked_target.user_id
            if not doctor_same_target:
                temporary_mafia_target_id = self._choose_mafia_target(self._active_mafia_votes())
                if blocked_target.role in MAFIA_ROLES and temporary_mafia_target_id == mistress.user_id:
                    mistress_killer_bypass = True
                elif blocked_target.role == ROLE_MANIAC and self.maniac_target_id == mistress.user_id:
                    mistress_killer_bypass = True
                elif (
                    blocked_target.role == ROLE_COMMISSAR
                    and self.commissar_action_mode == "shoot"
                    and self.commissar_shot_target_id == mistress.user_id
                ):
                    mistress_killer_bypass = True

            if mistress_killer_bypass:
                mistress_effective_target_id = None

    doctor = next((p for p in self.alive_players() if p.role == ROLE_DOCTOR), None)
    doctor_target_id: int | None = None
    doctor_blocked_by_mistress = (
        doctor is not None
        and mistress_effective_target_id is not None
        and doctor.user_id == mistress_effective_target_id
    )
    if doctor is not None and self.doctor_target_id is not None and not doctor_blocked_by_mistress:
        target = self.get_player(self.doctor_target_id)
        if target is not None and target.alive:
            doctor_target_id = target.user_id
            if target.user_id == doctor.user_id:
                self.doctor_self_heal_used = True

    notify_actions = action_notifications_from_settings(self.settings)

    if mistress_effective_target_id is not None:
        blocked_target = self.get_player(mistress_effective_target_id)
        if blocked_target is not None and blocked_target.alive:
            self.add_night_report_line(
                blocked_target.user_id,
                '"Ты со мною забудь обо всём...", - пела 💃🏼 Любовница\nДнем ты не сможешь писать в чат и голосовать.',
            )

    if doctor_target_id is not None:
        healed_target = self.get_player(doctor_target_id)
        if healed_target is not None and healed_target.alive:
            pass

    mafia_blocked_by_mistress = (
        mistress_effective_target_id if mistress_effective_target_id in self.alive_mafia_ids() else None
    )
    mafia_votes = self._active_mafia_votes(mafia_blocked_by_mistress)
    mafia_target_id = self._choose_mafia_target(mafia_votes)

    maniac = next((p for p in self.alive_players() if p.role == ROLE_MANIAC), None)
    maniac_target_id: int | None = None
    maniac_blocked_by_mistress = (
        maniac is not None
        and mistress_effective_target_id is not None
        and maniac.user_id == mistress_effective_target_id
    )
    if maniac is not None and self.maniac_target_id is not None and not maniac_blocked_by_mistress:
        target = self.get_player(self.maniac_target_id)
        if target is not None and target.alive and target.user_id != maniac.user_id:
            maniac_target_id = target.user_id

    don = next((p for p in self.alive_players() if p.role == ROLE_DON), None)

    commissar = next((p for p in self.alive_players() if p.role == ROLE_COMMISSAR), None)
    advocate = next((p for p in self.alive_players() if p.role == ROLE_ADVOCATE), None)
    advocate_target_id: int | None = None
    if advocate is not None and self.advocate_target_id is not None:
        protected_target = self.get_player(self.advocate_target_id)
        if protected_target is not None and protected_target.alive:
            advocate_target_id = protected_target.user_id

    commissar_check_target_id: int | None = None
    commissar_shot_target_id: int | None = None
    commissar_blocked_by_mistress = (
        commissar is not None
        and mistress_effective_target_id is not None
        and commissar.user_id == mistress_effective_target_id
    )
    if commissar is not None and not commissar_blocked_by_mistress:
        if commissar_can_shoot_this_round(self.settings, self.round_no):
            if self.commissar_action_mode == "check":
                commissar_check_target_id = self.commissar_target_id
            elif self.commissar_action_mode == "shoot":
                commissar_shot_target_id = self.commissar_shot_target_id
        else:
            commissar_check_target_id = self.commissar_target_id

    if commissar is not None and commissar_check_target_id is not None:
        checked = self.get_player(commissar_check_target_id)
        if checked is not None and checked.alive:
            mafia_checked = checked.role in MAFIA_ROLES
            masked_by_advocate = mafia_checked and advocate_target_id == checked.user_id
            fake_documents_used = (
                checked.role in DOCUMENTS_FAKEABLE_ROLES and checked.user_id in self.documented_user_ids
            )
            if fake_documents_used:
                self.documented_user_ids.discard(checked.user_id)
                self.spent_documents_user_ids.add(checked.user_id)
                self.add_night_report_line(checked.user_id, "Кто-то сильно заинтересовался твоей ролью...")
                self.add_night_report_line(checked.user_id, "Но ты показал фальшивые 📂 Документы :)")
                if masked_by_advocate and notify_actions:
                    self.add_night_report_line(checked.user_id, "Но 👨🏼‍💼 Адвокат сказал, что ты 👨🏼 Мирный житель!")
                self.remember_commissar_check(checked.user_id, ROLE_CITIZEN)
                self.set_pending_sergeant_check(checked.user_id, ROLE_CITIZEN)
                self.add_night_report_line(
                    commissar.user_id,
                    f"{self.public_player_mark(checked)} - 👨🏼 <b>{ROLE_CITIZEN}</b>",
                )
                checked = None
            elif notify_actions:
                self.add_night_report_line(checked.user_id, "Кто-то сильно заинтересовался твоей ролью...")
            if checked is None:
                pass
            else:
                if masked_by_advocate:
                    if notify_actions:
                        self.add_night_report_line(checked.user_id, "Но 👨🏼‍💼 Адвокат сказал, что ты 👨🏼 Мирный житель!")
                    self.remember_commissar_check(checked.user_id, ROLE_CITIZEN)
                    self.set_pending_sergeant_check(checked.user_id, ROLE_CITIZEN)
                    self.add_night_report_line(
                        commissar.user_id,
                        f"{self.public_player_mark(checked)} - 👨🏼 <b>{ROLE_CITIZEN}</b>",
                    )
                else:
                    self.remember_commissar_check(checked.user_id, checked.role)
                    self.set_pending_sergeant_check(checked.user_id, checked.role)
                    self.add_night_report_line(
                        commissar.user_id,
                        self.commissar_check_result_text(checked),
                    )

    attacks: dict[int, list[str]] = {}
    if mafia_target_id is not None:
        attacks.setdefault(mafia_target_id, []).append("мафия")
    if maniac_target_id is not None:
        attacks.setdefault(maniac_target_id, []).append("маньяк")
    if commissar_shot_target_id is not None:
        attacks.setdefault(commissar_shot_target_id, []).append("комиссар")
    if self.kamikaze_pending_user_id is not None and self.kamikaze_target_id is not None:
        attacks.setdefault(self.kamikaze_target_id, []).append("камикадзе")

    def night_kamikaze_revenge_targets(victim_user_id: int, sources: list[str]) -> list[int]:
        if not kamikaze_night_revenge_from_settings(self.settings):
            return []

        targets: list[int] = []

        for source in sources:
            if source == "мафия":
                if don is not None and don.alive:
                    targets.append(don.user_id)

            if source == "маньяк" and maniac is not None and maniac.alive:
                targets.append(maniac.user_id)

            if source == "комиссар" and commissar is not None and commissar.alive:
                targets.append(commissar.user_id)

            if source == "камикадзе":
                revenge_user_id = self.kamikaze_pending_user_id
                if revenge_user_id is None or revenge_user_id == victim_user_id:
                    continue
                revenge_player = self.get_player(revenge_user_id)
                if revenge_player is not None and revenge_player.alive:
                    targets.append(revenge_user_id)

        return list(dict.fromkeys(targets))

    eliminated: list[Player] = []
    self.night_kill_sources.clear()
    pending_targets = list(attacks.keys())
    processed_targets: set[int] = set()
    while pending_targets:
        target_id = pending_targets.pop(0)
        if target_id in processed_targets:
            continue
        processed_targets.add(target_id)

        target = self.get_player(target_id)
        if target is None or not target.alive:
            continue

        if doctor_target_id == target.user_id:
            self.add_night_report_line(target.user_id, "Тебя убили :(")
            self.add_night_report_line(target.user_id, "Ты можешь отправить сюда своё предсмертное сообщение")
            self.last_doctor_saved_target_id = target.user_id

            if target.role == ROLE_KAMIKAZE:
                sources = attacks.get(target_id, [])
                revenge_target_ids = night_kamikaze_revenge_targets(target.user_id, sources)
                for revenge_target_id in revenge_target_ids:
                    attacks.setdefault(revenge_target_id, []).append("камикадзе")
                    if revenge_target_id not in processed_targets:
                        pending_targets.append(revenge_target_id)
            continue
        sources = attacks.get(target_id, [])
        source_count = len(sources)
        if target.user_id in self.shielded_user_ids:
            self.shielded_user_ids.discard(target.user_id)
            self.spent_shield_user_ids.add(target.user_id)
            self.add_night_report_line(target.user_id, "Тебя пытались убить, но защита спасла")
            continue
        if (
            target.role == ROLE_LUCKY
            and not self.lucky_save_used
            and source_count == 1
            and random.random() < 0.5
        ):
            self.add_night_report_line(target.user_id, "Тебя пытались убить, но тебе повезло!")
            self.lucky_save_used = True
            continue

        target.alive = False
        eliminated.append(target)
        self.night_kill_sources[target.user_id] = sources

        if target.role == ROLE_KAMIKAZE:
            revenge_target_ids = night_kamikaze_revenge_targets(target.user_id, sources)
            for revenge_target_id in revenge_target_ids:
                attacks.setdefault(revenge_target_id, []).append("камикадзе")
                if revenge_target_id not in processed_targets:
                    pending_targets.append(revenge_target_id)

    don_transfer_note: str | None = None
    don_successor_id: int | None = None
    commissar_transfer_note: str | None = None
    commissar_successor_id: int | None = None
    if any(player.role == ROLE_DON for player in eliminated):
        don_transfer_result = self.transfer_don_if_needed("убийства Дона ночью")
        if don_transfer_result is not None:
            don_transfer_note, don_successor_id = don_transfer_result
    if any(player.role == ROLE_COMMISSAR for player in eliminated):
        commissar_transfer_result = self.transfer_commissar_if_needed()
        if commissar_transfer_result is not None:
            commissar_transfer_note, commissar_successor_id = commissar_transfer_result

    if mistress_effective_target_id is not None:
        silenced_player = self.get_player(mistress_effective_target_id)
        if silenced_player is not None and silenced_player.alive:
            if doctor_target_id != silenced_player.user_id:
                self.day_silenced_user_id = silenced_player.user_id

    bum = next((p for p in self.alive_players() if p.role == ROLE_BUM), None)
    if bum is not None and self.bum_target_id is not None:
        observed = self.get_player(self.bum_target_id)
        if observed is not None:
            observed_name = self.public_player_mark(observed)
            visitors: list[Player] = []
            seen_ids: set[int] = set()

            def add_visitor(visitor_user_id: int | None) -> None:
                if visitor_user_id is None:
                    return
                if visitor_user_id == bum.user_id:
                    return
                if visitor_user_id in seen_ids:
                    return
                visitor = self.get_player(visitor_user_id)
                if visitor is None:
                    return
                seen_ids.add(visitor_user_id)
                visitors.append(visitor)

            if doctor_target_id == observed.user_id and doctor is not None:
                add_visitor(doctor.user_id)
            if commissar_check_target_id == observed.user_id and commissar is not None:
                add_visitor(commissar.user_id)
            if commissar_shot_target_id == observed.user_id and commissar is not None:
                add_visitor(commissar.user_id)
            if maniac_target_id == observed.user_id and maniac is not None:
                add_visitor(maniac.user_id)
            if mistress_target_id == observed.user_id and mistress is not None:
                add_visitor(mistress.user_id)
            if advocate_target_id == observed.user_id and advocate is not None:
                add_visitor(advocate.user_id)

            if don is not None:
                don_target_id = mafia_votes.get(don.user_id)
                if don_target_id == observed.user_id:
                    add_visitor(don.user_id)

            if visitors:
                visitor_names = ", ".join(self.public_player_mark(player) for player in visitors)
                self.add_night_report_line(
                    bum.user_id,
                    f"Ночью ты пришёл за бутылкой к {observed_name} и увидел там {visitor_names}",
                )
            else:
                self.add_night_report_line(
                    bum.user_id,
                    f"Ты выпросил у {observed_name} бутылку и ушёл обратно на улицу. Ничего подозрительного не произошло.",
                )

    if doctor_target_id is not None:
        healed_target = self.get_player(doctor_target_id)
        if healed_target is not None and healed_target.alive:
            target_was_attacked = healed_target.user_id in attacks
            doctor_self_heal_without_attack = (
                doctor is not None
                and healed_target.user_id == doctor.user_id
                and not target_was_attacked
            )
            if doctor_self_heal_without_attack:
                self.add_night_report_line(
                    healed_target.user_id,
                    "Бинты, скальпель и ножницы не пригодились... И хорошо!",
                )
            elif not target_was_attacked and notify_actions:
                self.add_night_report_line(
                    healed_target.user_id,
                    "👨🏼‍⚕️ Доктор приходил к тебе в гости",
                )
            else:
                if doctor is not None and healed_target.user_id == doctor.user_id:
                    self.add_night_report_line(healed_target.user_id, "Ты успешно вылечил себя!")
                elif notify_actions:
                    self.add_night_report_line(healed_target.user_id, "👨🏼‍⚕️ Доктор вылечил тебя")

    active_night_ids: set[int] = set()
    acted_night_ids: set[int] = set(self.night_skipped_user_ids)
    for player in self.alive_players():
        if player.role in MAFIA_ROLES:
            active_night_ids.add(player.user_id)
            if player.user_id in self.night_votes:
                acted_night_ids.add(player.user_id)
        elif player.role == ROLE_DOCTOR:
            active_night_ids.add(player.user_id)
            if self.doctor_target_id is not None:
                acted_night_ids.add(player.user_id)
        elif player.role == ROLE_COMMISSAR:
            active_night_ids.add(player.user_id)
            if commissar_can_shoot_this_round(self.settings, self.round_no):
                if self.commissar_action_mode == "check" and self.commissar_target_id is not None:
                    acted_night_ids.add(player.user_id)
                elif self.commissar_action_mode == "shoot" and self.commissar_shot_target_id is not None:
                    acted_night_ids.add(player.user_id)
            elif self.commissar_target_id is not None:
                acted_night_ids.add(player.user_id)
        elif player.role == ROLE_ADVOCATE:
            active_night_ids.add(player.user_id)
            if self.advocate_target_id is not None:
                acted_night_ids.add(player.user_id)
        elif player.role == ROLE_MANIAC:
            active_night_ids.add(player.user_id)
            if self.maniac_target_id is not None:
                acted_night_ids.add(player.user_id)
        elif player.role == ROLE_MISTRESS:
            active_night_ids.add(player.user_id)
            if self.mistress_target_id is not None:
                acted_night_ids.add(player.user_id)
        elif player.role == ROLE_BUM:
            active_night_ids.add(player.user_id)
            if self.bum_target_id is not None:
                acted_night_ids.add(player.user_id)

    if self.kamikaze_pending_user_id is not None:
        active_night_ids.add(self.kamikaze_pending_user_id)
        if self.kamikaze_target_id is not None:
            acted_night_ids.add(self.kamikaze_pending_user_id)

    for user_id in list(self.night_missed_streaks.keys()):
        if user_id not in active_night_ids:
            self.night_missed_streaks.pop(user_id, None)

    for user_id in active_night_ids:
        if user_id in acted_night_ids:
            self.night_missed_streaks[user_id] = 0
            continue

        streak = int(self.night_missed_streaks.get(user_id, 0)) + 1
        self.night_missed_streaks[user_id] = streak
        if streak < 2:
            continue

        player = self.get_player(user_id)
        if player is None or not player.alive:
            continue
        player.alive = False
        eliminated.append(player)
        self.afk_killed_user_ids.add(player.user_id)
        self.night_missed_streaks[player.user_id] = 0

    if don_transfer_note is None and any(player.role == ROLE_DON for player in eliminated):
        don_transfer_result = self.transfer_don_if_needed("гибели Дона из-за бездействия ночью")
        if don_transfer_result is not None:
            don_transfer_note, don_successor_id = don_transfer_result

    if commissar_transfer_note is None and any(player.role == ROLE_COMMISSAR for player in eliminated):
        commissar_transfer_result = self.transfer_commissar_if_needed()
        if commissar_transfer_result is not None:
            commissar_transfer_note, commissar_successor_id = commissar_transfer_result

    self.forget_dead_commissar_checks()

    self.night_votes.clear()
    self.mafia_vote_locked = False
    self.mafia_target_announced = False
    self.announced_night_roles.clear()
    self.day_stage = DAY_STAGE_DISCUSSION
    self.day_votes.clear()
    self.trial_candidate_id = None
    self.trial_vote_message_id = None
    self.trial_votes.clear()
    self.doctor_target_id = None
    self.commissar_action_mode = None
    self.commissar_target_id = None
    self.commissar_shot_target_id = None
    self.advocate_target_id = None
    self.maniac_target_id = None
    self.mistress_last_target_id = mistress_target_id
    self.mistress_target_id = None
    self.bum_last_target_id = self.bum_target_id
    self.bum_target_id = None
    self.kamikaze_pending_user_id = None
    self.kamikaze_target_id = None

    winner = self.check_winner()
    if winner:
        return (
            True,
            f"Игра окончена. Победила команда: {winner}.",
            eliminated,
            don_transfer_note,
            don_successor_id,
            commissar_transfer_note,
            commissar_successor_id,
        )

    self.phase = PHASE_DAY
    if not eliminated:
        return (
            True,
            "Удивительно, но этой ночью все выжили.",
            [],
            don_transfer_note,
            don_successor_id,
            commissar_transfer_note,
            commissar_successor_id,
        )
    return (
        True,
        "Ночь окончена. Наступает день.",
        eliminated,
        don_transfer_note,
        don_successor_id,
        commissar_transfer_note,
        commissar_successor_id,
    )


__all__ = ["resolve_night"]
