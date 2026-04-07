from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class AccountConfig:
    account_id: str
    name: str
    bearer_token: str
    client_version: str = "mngmv2a9"
    http_proxy: str | None = None
    captcha_log_file: str = "captcha_history.jsonl"
    auto_open_chests: bool = True
    auto_equip: bool = True
    auto_upgrade_stats: bool = True
    auto_claim_bp: bool = True
    cheater_wave_mode: bool = False
    stealth_teleport_when_wave_ge: int | None = None
    stealth_teleport_to_wave: int | None = None
    stealth_random_fail_chance: float = 0.0
    sleep_min_seconds: float = 8.0
    sleep_max_seconds: float = 14.0
    browser_profile: dict[str, Any] | None = None


@dataclass
class BotSnapshot:
    account_id: str
    running: bool = False
    wave: int | None = None
    floor: int | None = None
    room: int | None = None
    level: int | None = None
    balance: float | None = None
    total_earned: float | None = None
    bp_tier: int | None = None
    bp_xp: int | None = None
    last_error: str | None = None
    last_message: str | None = None
    account_banned: bool = False
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class BotEvent:
    account_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AppSettings:
    """Глобальные настройки для новых аккаунтов и массовых операций."""

    sleep_min_seconds: float = 8.0
    sleep_max_seconds: float = 14.0
    auto_open_chests: bool = True
    auto_equip: bool = True
    auto_upgrade_stats: bool = True
    auto_claim_bp: bool = True
    cheater_wave_mode: bool = False


def apply_app_settings(acc: AccountConfig, settings: AppSettings) -> AccountConfig:
    acc.sleep_min_seconds = settings.sleep_min_seconds
    acc.sleep_max_seconds = settings.sleep_max_seconds
    acc.auto_open_chests = settings.auto_open_chests
    acc.auto_equip = settings.auto_equip
    acc.auto_upgrade_stats = settings.auto_upgrade_stats
    acc.auto_claim_bp = settings.auto_claim_bp
    acc.cheater_wave_mode = settings.cheater_wave_mode
    return acc
