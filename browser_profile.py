"""Уникальный «отпечаток» браузера на аккаунт: User-Agent, sec-ch-ua, язык, client_proof."""
from __future__ import annotations

import random
import re
import string
from typing import Any

_CHROME_MAJORS = (118, 120, 121, 124, 126, 129, 131, 132, 133, 134, 135, 136)

_RESOLUTIONS = (
    "1920x1080x24",
    "1920x1080x32",
    "1366x768x24",
    "1536x864x24",
    "1440x900x24",
    "2560x1440x24",
    "1280x720x24",
    "1680x1050x24",
)

_LANG_HEADERS = (
    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-US,en;q=0.9",
    "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
    "de-DE,de;q=0.9,en;q=0.8",
    "fr-FR,fr;q=0.9,en;q=0.8",
    "pl-PL,pl;q=0.9,en;q=0.8",
    "en-GB,en;q=0.9,en-US;q=0.8",
)

# Короткий код языка для поля lang в client_proof (как в старом клиенте)
_LANG_PROOF = ("ru", "en", "uk", "de", "fr", "pl", "en")


def _gl_angle(r: random.Random) -> str:
    vendor = r.choice(
        (
            ("NVIDIA", "NVIDIA GeForce RTX 4060 Ti (0x00002803)"),
            ("NVIDIA", "NVIDIA GeForce RTX 3060 (0x00002504)"),
            ("NVIDIA", "NVIDIA GeForce GTX 1660 SUPER (0x000021c4)"),
            ("AMD", "AMD Radeon RX 6700 XT"),
            ("AMD", "AMD Radeon RX 580 Series"),
            ("Intel", "Intel(R) UHD Graphics 630"),
            ("Intel", "Intel(R) Iris(R) Xe Graphics"),
        )
    )
    vendor_name, model = vendor
    return (
        f"ANGLE ({vendor_name}, {model} "
        f"Direct3D11 vs_5_0 ps_5_0, D3D11)"
    )


def _cv_string(r: random.Random, n: int = 6) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(r.choice(alphabet) for _ in range(n))


def _tz_minutes(r: random.Random) -> int:
    # Частые смещения: от UTC−12 до UTC+14, кратно 15 мин (как реальные TZ)
    quarters = r.randint(-48, 56)
    return quarters * 15


def generate_browser_profile(r: random.Random | None = None) -> dict[str, Any]:
    """
    Полный профиль для HTTP-заголовков и для поля client_proof в теле запросов.
    ts в proof не храним — подставляется при каждом вызове client_proof().
    """
    rng = r or random.Random()
    major = rng.choice(_CHROME_MAJORS)
    build = rng.randint(6000, 6999)
    patch = rng.randint(80, 220)
    full_ver = f"{major}.0.{build}.{patch}"
    win_nt = rng.choice(("Windows NT 10.0", "Windows NT 11.0"))
    ua = (
        f"Mozilla/5.0 ({win_nt}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{full_ver} Safari/537.36"
    )
    not_brand = rng.choice(("8", "24", "99"))
    sec_ch_ua = (
        f'"Google Chrome";v="{major}", "Chromium";v="{major}", '
        f'"Not_A Brand";v="{not_brand}"'
    )
    sec_ch_ua_full_version_list = (
        f'"Google Chrome";v="{full_ver}", "Chromium";v="{full_ver}", '
        f'"Not_A Brand";v="24.0.0.0"'
    )
    accept_language = rng.choice(_LANG_HEADERS)
    lang_idx = _LANG_HEADERS.index(accept_language)
    lang_proof = _LANG_PROOF[lang_idx] if lang_idx < len(_LANG_PROOF) else "en"

    proof_inner: dict[str, Any] = {
        "sc": rng.choice(_RESOLUTIONS),
        "lang": lang_proof,
        "plt": "Win32",
        "mem": rng.choice((4, 8, 12, 16, 32)),
        "hw": rng.randint(4, 32),
        "cv": _cv_string(rng),
        "touch": False,
        "gl": _gl_angle(rng),
        "tz": _tz_minutes(rng),
        "vis": "visible",
    }

    return {
        "user_agent": ua,
        "sec_ch_ua": sec_ch_ua,
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "sec_ch_ua_full_version_list": sec_ch_ua_full_version_list,
        "accept_language": accept_language,
        "proof_inner": proof_inner,
    }


def upgrade_browser_profile_headers(profile: dict[str, Any]) -> bool:
    """Достраивает Sec-CH-UA-Full-Version-List из User-Agent (миграция старых профилей)."""
    if profile.get("sec_ch_ua_full_version_list"):
        return False
    ua = str(profile.get("user_agent") or "")
    m = re.search(r"Chrome/([\d.]+)", ua)
    if not m:
        return False
    fv = m.group(1)
    profile["sec_ch_ua_full_version_list"] = (
        f'"Google Chrome";v="{fv}", "Chromium";v="{fv}", "Not_A Brand";v="24.0.0.0"'
    )
    return True


def is_complete_browser_profile(p: object) -> bool:
    if not isinstance(p, dict):
        return False
    if not (isinstance(p.get("user_agent"), str) and p["user_agent"].strip()):
        return False
    inner = p.get("proof_inner")
    if not isinstance(inner, dict):
        return False
    return bool(inner.get("gl")) and bool(inner.get("sc"))
