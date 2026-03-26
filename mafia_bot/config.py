import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    bot_token: str
    night_phase_seconds: int
    day_phase_seconds: int


def _read_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def get_settings() -> Settings:
    load_dotenv()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("BOT_TOKEN is not set. Create .env from .env.example")
    return Settings(
        bot_token=token,
        night_phase_seconds=_read_positive_int("NIGHT_PHASE_SECONDS", 90),
        day_phase_seconds=_read_positive_int("DAY_PHASE_SECONDS", 150),
    )
