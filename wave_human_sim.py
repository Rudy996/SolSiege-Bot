"""
Оценка исхода только босс-волны по JSON ответа GET /wave/current.

Бот вызывает это только при is_boss и выключенном cheater_wave_mode; обычные волны всегда complete.

Упрощённая пошаговая модель: урон игрока раз в attack_speed_ms (с учётом крита),
урон врагов раз в enemy_attack_ms, входящий урон масштабируется как доля оставшихся HP врагов.
Регенерации в ответе API нет — не учитывается. Совпадение с клиентом не гарантируется.
"""

from __future__ import annotations

from typing import Any

from siege_client import build_enemy_kills

DEFAULT_CRIT_MULTIPLIER = 1.5
MAX_SIM_WALL_MS = 900_000


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except (TypeError, ValueError):
        return default


def total_enemy_hp_pool(wave: dict) -> float:
    """Суммарные HP всех целей волны (босс + миньоны или обычные мобы)."""
    if wave.get("is_boss") and wave.get("boss_info"):
        bi = wave["boss_info"] or {}
        boss_t = bi.get("boss_type")
        min_hp = bi.get("minion_hp") or {}
        kills = build_enemy_kills(wave)
        total = 0.0
        for etype, cnt in kills.items():
            cnt = int(cnt)
            if cnt <= 0:
                continue
            if etype == boss_t:
                total += _f(bi.get("boss_hp"), _f(wave.get("enemy_hp")))
            else:
                h = _f(min_hp.get(etype), _f(wave.get("enemy_hp")))
                total += h * cnt
        return max(total, 0.0)
    ec = _i(wave.get("enemy_count"))
    eh = _f(wave.get("enemy_hp"))
    return max(float(ec) * eh, 0.0)


def initial_enemy_tick_damage(wave: dict) -> float:
    """Входящий урон за один тик врагов в начале волны."""
    ec = max(1, _i(wave.get("enemy_count"), 1))
    ed = _f(wave.get("enemy_damage"))
    if wave.get("is_boss") and wave.get("boss_info"):
        bi = wave["boss_info"] or {}
        bd = _f(bi.get("boss_damage"), ed)
        md = bi.get("minion_damage") or {}
        kills = build_enemy_kills(wave)
        boss_t = bi.get("boss_type")
        s = bd
        for etype, cnt in kills.items():
            cnt = int(cnt)
            if cnt <= 0 or etype == boss_t:
                continue
            s += _f(md.get(etype), ed) * cnt
        return max(s, 0.0)
    return max(float(ec) * ed, 0.0)


def expected_player_hit(wave: dict) -> float:
    base = _f(wave.get("player_damage"))
    crit = _f(wave.get("player_crit"))
    mult = _f(wave.get("crit_multiplier"), DEFAULT_CRIT_MULTIPLIER)
    crit = min(max(crit, 0.0), 1.0)
    return base * (1.0 - crit) + base * crit * mult


def simulate_boss_wave_win(wave: dict) -> tuple[bool, str]:
    """
    True, если по модели игрок успевает обнулить HP врагов раньше, чем умирает сам.
    Предназначено для wave с is_boss=True.
    """
    player_hp = _f(wave.get("player_max_hp"))
    if player_hp <= 0:
        return False, "Нет player_max_hp"

    hit = expected_player_hit(wave)
    if hit <= 0:
        return False, "Нулевой урон игрока"

    p_int = max(1, _i(wave.get("attack_speed_ms"), 1000))
    e_int = max(1, _i(wave.get("enemy_attack_ms"), 2500))

    e0 = total_enemy_hp_pool(wave)
    if e0 <= 0:
        return True, "Нет врагов (0 HP)"

    d0 = initial_enemy_tick_damage(wave)
    enemy_hp = e0
    next_p = 0
    next_e = 0
    t = 0

    while enemy_hp > 1e-6 and player_hp > 1e-6:
        if t > MAX_SIM_WALL_MS:
            return False, "Таймаут симуляции (>15 мин)"

        if next_p <= next_e:
            t = next_p
            enemy_hp = max(0.0, enemy_hp - hit)
            next_p += p_int
        else:
            t = next_e
            frac = enemy_hp / e0 if e0 > 0 else 0.0
            incoming = d0 * frac
            player_hp = max(0.0, player_hp - incoming)
            next_e += e_int

    win = enemy_hp <= 1e-6
    if win:
        return True, "Симуляция: победа"
    return False, "Симуляция: поражение (урон врагов)"
