from pathlib import Path
import ast, shutil

ROOT = Path(__file__).resolve().parents[1]
MAFIA = ROOT / 'mafia_bot'
SRC = MAFIA / 'handlers.py'
PKG = MAFIA / 'handlers'
LEGACY = MAFIA / 'handlers_legacy.py'

if SRC.exists() and not LEGACY.exists():
    shutil.copy2(SRC, LEGACY)

source = SRC.read_text(encoding='utf-8')
tree = ast.parse(source)
lines = source.splitlines(keepends=True)
first_fn = next((n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))), None)
context_end = first_fn.lineno - 1 if first_fn else len(lines)
context = ''.join(lines[:context_end])

roles = {
    'common': '''cancel_registration_timer current_day_stage_seconds clear_chat_penalties persist_room remove_room_state role_mark_text room_player_mark room_player_label private_game_send_kwargs night_role_announcement_text track_action_menu_message get_action_menu_message_id clear_action_menu_messages clear_registration_panel_message clear_registration_notice_message upsert_registration_warning_message clear_registration_warning_message resolve_phase_image_path send_phase_media safe_send_message skipped_turn_keyboard locked_choice_keyboard skip_turn_button_text skip_turn_selected_text locked_choice_text night_skipped_user_ids mark_skipped_night_menus format_killer_sources_text ensure_stats_recorded prime_room_documents prime_room_shields apply_room_active_role_buffs send_endgame_currency_summaries notify_missing_delete_permission_once safe_delete_message cleanup_group_command_message delete_message_later should_send_chat_welcome send_group_welcome get_or_create_penalty is_user_blocked blocked_seconds_left notify_registration_blocked is_group_admin is_group_settings_admin bot_has_delete_permission process_rule_violation user_nickname player_display_name normalize_link_display_name player_profile_link user_profile_link_by_id get_private_action_room get_player_profile_room get_pending_last_word_room notify_room_private_cancellation announce_don_transfer announce_commissar_transfer cmd_panel on_owner_exit_phrase on_developer_phrase enforce_group_game_rules''',
    'lobby': '''registration_remaining_seconds start_registration_timer launch_game_from_registration maybe_launch_full_lobby process_registration_timeout registration_join_link bot_start_link registration_text registration_post_text private_bot_link refresh_registration_post pin_registration_post clear_registration_post registration_lobby_keyboard registration_panel cmd_start on_new_chat_members on_chat_member_joined cmd_roles cmd_create cmd_join cmd_leave cmd_lobby cmd_extend cmd_stop cmd_begin on_registration_action cmd_status cmd_id''',
    'game': '''get_phase_lock cancel_phase_timer process_night_end process_day_end phase_timer_worker start_phase_timer restore_runtime_state maybe_finish_phase_early''',
    'day': '''trial_vote_prompt_text trial_vote_keyboard push_trial_vote_menus finish_trial_vote_message prompt_last_words on_trial_callback''',
    'profile': '''format_player_stats_text top_period_keyboard format_top_text format_endgame_currency_text format_private_profile_text format_buffs_shop_text format_buff_details_text private_main_menu_keyboard private_profile_keyboard private_back_to_menu_keyboard private_buffs_shop_keyboard private_buff_details_keyboard private_roles_keyboard private_back_to_roles_keyboard private_role_details_text role_card_for_player cmd_stats send_top_to_private cmd_top cmd_profile''',
    'settings': '''game_mode_announcement_text is_secret_voting_enabled show_targets_enabled show_roles_enabled allow_team_kill_enabled commissar_can_shoot_enabled commissar_can_shoot_this_night night_action_skip_enabled day_vote_skip_enabled content_protection_enabled buffs_enabled invisible_mode_enabled settings_mode_locked_message settings_mode_locked apply_game_mode_preset private_settings_main_text default_chat_settings merge_chat_settings load_chat_settings room_chat_settings apply_room_settings save_chat_settings settings_callback_data selected_square format_settings_screen_text format_leave_duration current_settings_timing_value current_settings_mute_value private_settings_main_keyboard private_settings_game_mode_keyboard private_settings_roles_keyboard private_settings_role_toggle_keyboard private_settings_roles_text private_settings_role_toggle_text private_settings_timings_keyboard private_settings_timing_values_keyboard private_settings_mute_keyboard private_settings_mute_toggle_keyboard private_settings_misc_keyboard private_settings_mafia_ratio_keyboard private_settings_voting_mode_keyboard private_settings_misc_toggle_keyboard private_settings_leave_keyboard cmd_settings on_private_settings_callback''',
    'tickets': '''is_ticket_manager_user_id ticket_command_usage_text ticket_command_hint_text resolve_ticket_command_target handle_ticket_adjustment_command on_ticket_grant_command on_ticket_take_command on_ticket_admin_grant_command on_private_ticket_grant_command on_private_ticket_take_command on_private_ticket_admin_grant_command on_ticket_command_hint on_private_ticket_command_hint''',
    'night': '''night_action_keyboard selected_target_for_actor build_action_keyboard build_action_prompt_text night_status_text refresh_private_action_message send_action_menu push_phase_action_menus push_kamikaze_revenge_menu send_mafia_private_update compact_night_report_messages sergeant_commissar_check_text mafia_allies_text city_power_allies_text cmd_action on_action_callback''',
    'callbacks': '''on_noop_callback on_private_menu_callback on_top_callback on_private_text''',
}
roles = {k: set(v.split()) for k,v in roles.items()}

nodes = []
for n in tree.body:
    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
        start = n.lineno - 1
        if n.decorator_list:
            start = min(d.lineno - 1 for d in n.decorator_list)
        end = n.end_lineno
        nodes.append((n.name, ''.join(lines[start:end])))

assigned = set()
for mod, names in roles.items():
    chunks = []
    for name, text in nodes:
        if name in names:
            chunks.append(text)
            assigned.add(name)
    body = '# Extracted from mafia_bot/handlers.py.\n# Refactor only: gameplay behavior is unchanged.\nfrom ._context import *  # noqa: F401,F403\n\n' + '\n\n'.join(chunks)
    (PKG / f'{mod}.py').write_text(body.rstrip() + '\n', encoding='utf-8')

unknown = [(name, text) for name, text in nodes if name not in assigned]
if unknown:
    path = PKG / 'common.py'
    with path.open('a', encoding='utf-8') as f:
        f.write('\n\n# Unclassified handlers retained here for compatibility.\n\n')
        f.write('\n\n'.join(text for _, text in unknown) + '\n')

(PKG / '_context.py').write_text('# Shared runtime context extracted from handlers.py.\n' + context.rstrip() + '\n', encoding='utf-8')

init = '''"""Split Telegram handler package.\n\nPublic names are re-exported to keep imports compatible with the old monolithic handlers.py module.\n"""\n\nfrom . import _context as _context\nfrom . import common as _common\nfrom . import lobby as _lobby\nfrom . import game as _game\nfrom . import day as _day\nfrom . import profile as _profile\nfrom . import settings as _settings\nfrom . import tickets as _tickets\nfrom . import night as _night\nfrom . import callbacks as _callbacks\n\n_modules = [_common, _lobby, _game, _day, _profile, _settings, _tickets, _night, _callbacks]\n_shared = {}\nfor _module in _modules:\n    for _name, _value in vars(_module).items():\n        if not _name.startswith('__'):\n            _shared[_name] = _value\n\nfor _module in _modules:\n    _module.__dict__.update(_shared)\n\nglobals().update(_context.__dict__)\nglobals().update(_shared)\n__all__ = [name for name in globals() if not name.startswith('_')]\n'''
(PKG / '__init__.py').write_text(init, encoding='utf-8')

SRC.unlink(missing_ok=True)
print(f'Generated split handler package: {PKG}; functions={len(nodes)} assigned={len(assigned)} unknown={len(unknown)}')
