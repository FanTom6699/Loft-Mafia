"""Core constants for the Mafia game domain.

Kept separate from the compatibility-facing ``mafia_bot.game`` module so
handlers and storage can continue importing the historical names unchanged.
"""

MIN_PLAYERS = 4
MAX_PLAYERS = 20

PHASE_LOBBY = "lobby"
PHASE_NIGHT = "night"
PHASE_DAY = "day"
PHASE_FINISHED = "finished"

GAME_MODE_CLASSIC = "classic"
GAME_MODE_INVISIBLE = "invisible"

DAY_STAGE_DISCUSSION = "discussion"
DAY_STAGE_NOMINATION = "nomination"
DAY_STAGE_TRIAL = "trial"

ROLE_DON = "Дон"
ROLE_MAFIA = "Мафия"
ROLE_MANIAC = "Маньяк"
ROLE_COMMISSAR = "Комиссар Каттани"
ROLE_DOCTOR = "Доктор"
ROLE_MISTRESS = "Любовница"
ROLE_BUM = "Бомж"
ROLE_ADVOCATE = "Адвокат"
ROLE_SERGEANT = "Сержант"
ROLE_SUICIDE = "Самоубийца"
ROLE_LUCKY = "Счастливчик"
ROLE_KAMIKAZE = "Камикадзе"
ROLE_CITIZEN = "Мирный житель"

MAFIA_ROLES = {ROLE_DON, ROLE_MAFIA}
DOCUMENTS_FAKEABLE_ROLES = {ROLE_DON, ROLE_MAFIA, ROLE_ADVOCATE, ROLE_MANIAC}
MAFIA_RATIO_TARGETS = {
    "high": 3,
    "low": 4,
}

GAME_MODE_TITLES = {
    GAME_MODE_CLASSIC: "Классика",
    GAME_MODE_INVISIBLE: "Невидимка",
}
