"""Handler facade for the tickets area.

Stage 1 keeps runtime compatibility: implementations remain in
``mafia_bot._handlers_legacy`` and are re-exported here.
"""

from .. import _handlers_legacy as _legacy

is_ticket_manager_user_id = _legacy.is_ticket_manager_user_id
ticket_command_usage_text = _legacy.ticket_command_usage_text
ticket_command_hint_text = _legacy.ticket_command_hint_text
resolve_ticket_command_target = _legacy.resolve_ticket_command_target
handle_ticket_adjustment_command = _legacy.handle_ticket_adjustment_command
on_ticket_grant_command = _legacy.on_ticket_grant_command
on_ticket_take_command = _legacy.on_ticket_take_command
on_ticket_admin_grant_command = _legacy.on_ticket_admin_grant_command
on_private_ticket_grant_command = _legacy.on_private_ticket_grant_command
on_private_ticket_take_command = _legacy.on_private_ticket_take_command
on_private_ticket_admin_grant_command = _legacy.on_private_ticket_admin_grant_command
on_ticket_command_hint = _legacy.on_ticket_command_hint
on_private_ticket_command_hint = _legacy.on_private_ticket_command_hint
