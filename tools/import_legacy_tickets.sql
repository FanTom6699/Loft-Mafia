ATTACH DATABASE 'bot_edit.db' AS legacy;

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
SELECT
    legacy.users.user_id,
    CASE
        WHEN COALESCE(legacy.users.username, '') <> '' THEN legacy.users.username
        ELSE 'Игрок ' || legacy.users.user_id
    END,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    COALESCE(legacy.users.balance, 0),
    0,
    0,
    0,
    '',
    datetime('now')
FROM legacy.users
INNER JOIN private_users ON private_users.user_id = legacy.users.user_id
WHERE COALESCE(legacy.users.balance, 0) > 0
ON CONFLICT(user_id) DO UPDATE SET
    tickets = player_stats.tickets + excluded.tickets,
    updated_at = excluded.updated_at;

DETACH DATABASE legacy;