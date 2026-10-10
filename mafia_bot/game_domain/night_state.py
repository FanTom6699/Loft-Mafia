"""Night-phase state helpers for the Mafia domain."""

from .constants import PHASE_FINISHED, PHASE_NIGHT, ROLE_ADVOCATE, ROLE_BUM, ROLE_COMMISSAR, ROLE_DOCTOR, ROLE_KAMIKAZE, ROLE_MANIAC, ROLE_MISTRESS, MAFIA_ROLES
from .roles import commissar_can_shoot_this_round


def all_required_night_actions_done(self) -> bool:
    if self.phase != PHASE_NIGHT:
        return False

    alive_mafia_ids = {player.user_id for player in self.alive_mafia()}
    committed_mafia_ids = set(self.night_votes.keys()) | set(self.night_skipped_user_ids)
    if alive_mafia_ids and not alive_mafia_ids.issubset(committed_mafia_ids):
        return False

    doctor_alive = any(p.alive and p.role == ROLE_DOCTOR for p in self.players.values())
    doctor_player = next((p for p in self.players.values() if p.alive and p.role == ROLE_DOCTOR), None)
    if doctor_alive and self.doctor_target_id is None and (doctor_player is None or doctor_player.user_id not in self.night_skipped_user_ids):
        return False

    commissar_alive = any(p.alive and p.role == ROLE_COMMISSAR for p in self.players.values())
    commissar_player = next((p for p in self.players.values() if p.alive and p.role == ROLE_COMMISSAR), None)
    if commissar_alive:
        if commissar_player is not None and commissar_player.user_id in self.night_skipped_user_ids:
            pass
        elif commissar_can_shoot_this_round(self.settings, self.round_no):
            if self.commissar_action_mode is None:
                return False
            elif self.commissar_action_mode == "check" and self.commissar_target_id is None:
                return False
            elif self.commissar_action_mode == "shoot" and self.commissar_shot_target_id is None:
                return False
        elif self.commissar_target_id is None:
            return False

    advocate_alive = any(p.alive and p.role == ROLE_ADVOCATE for p in self.players.values())
    advocate_player = next((p for p in self.players.values() if p.alive and p.role == ROLE_ADVOCATE), None)
    if advocate_alive and self.advocate_target_id is None and (advocate_player is None or advocate_player.user_id not in self.night_skipped_user_ids):
        return False

    maniac_alive = any(p.alive and p.role == ROLE_MANIAC for p in self.players.values())
    maniac_player = next((p for p in self.players.values() if p.alive and p.role == ROLE_MANIAC), None)
    if maniac_alive and self.maniac_target_id is None and (maniac_player is None or maniac_player.user_id not in self.night_skipped_user_ids):
        return False

    mistress_alive = any(p.alive and p.role == ROLE_MISTRESS for p in self.players.values())
    mistress_player = next((p for p in self.players.values() if p.alive and p.role == ROLE_MISTRESS), None)
    if mistress_alive and self.mistress_target_id is None and (mistress_player is None or mistress_player.user_id not in self.night_skipped_user_ids):
        return False

    bum_alive = any(p.alive and p.role == ROLE_BUM for p in self.players.values())
    bum_player = next((p for p in self.players.values() if p.alive and p.role == ROLE_BUM), None)
    if bum_alive and self.bum_target_id is None and (bum_player is None or bum_player.user_id not in self.night_skipped_user_ids):
        return False

    if self.kamikaze_pending_user_id is not None and self.kamikaze_target_id is None and self.kamikaze_pending_user_id not in self.night_skipped_user_ids:
        return False

    return True


def can_skip_night_action(self, user_id: int) -> bool:
    if self.phase != PHASE_NIGHT:
        return False
    if user_id in self.night_skipped_user_ids:
        return False

    player = self.get_player(user_id)
    is_kamikaze_revenge = self.kamikaze_pending_user_id == user_id
    if player is None:
        return False
    if not player.alive and not is_kamikaze_revenge:
        return False

    if player.role in MAFIA_ROLES:
        return not self.mafia_vote_locked and user_id not in self.night_votes
    if player.role == ROLE_DOCTOR:
        return self.doctor_target_id is None
    if player.role == ROLE_COMMISSAR:
        if commissar_can_shoot_this_round(self.settings, self.round_no):
            if self.commissar_action_mode is None:
                return True
            if self.commissar_action_mode == "check":
                return self.commissar_target_id is None
            if self.commissar_action_mode == "shoot":
                return self.commissar_shot_target_id is None
            return False
        return self.commissar_target_id is None
    if player.role == ROLE_ADVOCATE:
        return self.advocate_target_id is None
    if player.role == ROLE_MANIAC:
        return self.maniac_target_id is None
    if player.role == ROLE_MISTRESS:
        return self.mistress_target_id is None
    if player.role == ROLE_BUM:
        return self.bum_target_id is None
    if is_kamikaze_revenge:
        return self.kamikaze_target_id is None
    return False


def set_night_skip(self, user_id: int) -> tuple[bool, str]:
    if not can_skip_night_action(self, user_id):
        return False, "🚷 Сейчас нельзя пропустить ход."

    self.night_skipped_user_ids.add(user_id)
    alive_mafia_ids = {player.user_id for player in self.alive_mafia()}
    committed_mafia_ids = set(self.night_votes.keys()) | set(self.night_skipped_user_ids)
    if alive_mafia_ids and alive_mafia_ids.issubset(committed_mafia_ids):
        self.mafia_vote_locked = True
    return True, "🚷 Ход пропущен."


def arm_shield(self, user_id: int) -> bool:
    player = self.get_player(user_id)
    if player is None or not player.alive:
        return False
    if not self.started or self.phase == PHASE_FINISHED:
        return False
    if user_id in self.spent_shield_user_ids:
        return False
    self.shielded_user_ids.add(user_id)
    return True


def arm_documents(self, user_id: int) -> bool:
    player = self.get_player(user_id)
    if player is None or not player.alive:
        return False
    if not self.started or self.phase == PHASE_FINISHED:
        return False
    if user_id in self.spent_documents_user_ids:
        return False
    self.documented_user_ids.add(user_id)
    return True


def pop_spent_documents_user_ids(self) -> set[int]:
    payload = set(self.spent_documents_user_ids)
    self.spent_documents_user_ids.clear()
    return payload


def pop_spent_shield_user_ids(self) -> set[int]:
    payload = set(self.spent_shield_user_ids)
    self.spent_shield_user_ids.clear()
    return payload
