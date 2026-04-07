from __future__ import annotations

import base64
import json
import random
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from browser_profile import generate_browser_profile, is_complete_browser_profile
from captcha_logger import log_captcha_event
from models import AccountConfig, BotEvent, BotSnapshot
from siege_client import (
    SiegeApiError,
    align_teleport_target_wave,
    get_bp_challenges,
    get_bp_progress,
    get_inventory,
    get_player_progress,
    get_random_stat_tree,
    get_token_balance,
    get_wave_current,
    post_bp_claim_challenge,
    post_bp_claim_seasonal,
    post_bp_claim_tier,
    post_captcha_solve,
    post_chest_open,
    post_inventory_equip,
    post_stat_upgrade,
    post_wave_complete,
    post_wave_fail,
    post_wave_teleport,
    set_http_proxy,
    set_request_browser_profile,
)
from twocaptcha_solver import (
    try_local_color_captcha,
    try_local_emoji_captcha,
    try_local_math_answer,
    try_local_word_captcha,
)
from wave_human_sim import simulate_boss_wave_win


class AccountBannedError(Exception):
    """Сервер вернул ACCOUNT_BANNED — дальнейшие запросы с этого аккаунта бессмысленны."""


def _fmt_ch_amount(v) -> str:
    if v is None:
        return "—"
    try:
        x = float(v)
        s = f"{x:.8f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except (TypeError, ValueError):
        return str(v)


class BotWorker:
    def __init__(
        self,
        project_root: Path,
        account: AccountConfig,
        on_event: Callable[[BotEvent], None] | None = None,
        on_log: Callable[[str, str], None] | None = None,
        snapshot_seed: BotSnapshot | None = None,
    ) -> None:
        self.project_root = project_root
        self.account = account
        self.on_event = on_event
        self.on_log = on_log
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        if snapshot_seed is not None:
            self._snapshot = replace(
                snapshot_seed,
                account_id=account.account_id,
                running=False,
            )
        else:
            self._snapshot = BotSnapshot(account_id=account.account_id, running=False)

    @property
    def snapshot(self) -> BotSnapshot:
        return self._snapshot

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"bot-{self.account.account_id}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)

    def _emit(self, event_type: str, **payload) -> None:
        ev = BotEvent(account_id=self.account.account_id, event_type=event_type, payload=payload)
        if self.on_event:
            self.on_event(ev)

    def _log(self, level: str, message: str) -> None:
        self._snapshot.last_message = message
        self._snapshot.updated_at = datetime.utcnow()
        if self.on_log:
            self.on_log(level, message)
        self._emit("log", level=level, message=message)

    def _set_error(self, message: str) -> None:
        self._snapshot.last_error = message
        self._snapshot.updated_at = datetime.utcnow()
        self._emit("error", message=message)

    def _set_account_banned(self, message: str) -> None:
        self._snapshot.account_banned = True
        self._snapshot.last_error = message
        self._snapshot.updated_at = datetime.utcnow()
        self._log("error", message)
        self._emit("error", message=message, account_banned=True)

    @staticmethod
    def _ban_detail_text(e: SiegeApiError) -> str:
        d = e.detail
        if isinstance(d, str):
            return d
        if isinstance(d, list):
            return " ".join(str(x) for x in d)
        if isinstance(d, dict):
            return json.dumps(d, ensure_ascii=False)
        return str(d)

    @staticmethod
    def _is_account_banned(e: SiegeApiError) -> bool:
        if e.status != 403:
            return False
        return "ACCOUNT_BANNED" in BotWorker._ban_detail_text(e)

    def _jwt_exp_unix(self, bearer_token: str):
        try:
            payload_b64 = bearer_token.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            return payload.get("exp")
        except (IndexError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _check_token_not_expired(self, bearer_token: str) -> None:
        exp = self._jwt_exp_unix(bearer_token)
        if exp is None:
            self._log("warning", "JWT не удалось разобрать (нет exp).")
            return
        until = datetime.fromtimestamp(exp, tz=timezone.utc)
        now = time.time()
        if now >= exp:
            raise RuntimeError(f"JWT истёк: {until:%Y-%m-%d %H:%M:%S} UTC")

    @staticmethod
    def _floor_room(wave: int | None) -> tuple[int | None, int | None]:
        if wave is None or wave < 1:
            return None, None
        floor = ((wave - 1) // 10) + 1
        room = ((wave - 1) % 10) + 1
        return floor, room

    def _next_sleep(self) -> float:
        lo = float(self.account.sleep_min_seconds)
        hi = float(self.account.sleep_max_seconds)
        if lo > hi:
            lo, hi = hi, lo
        return random.uniform(lo, hi)

    def _safe_err(self, e: SiegeApiError, name: str) -> None:
        if e.status == 401:
            raise RuntimeError("HTTP 401 — сессия недействительна.")
        if self._is_account_banned(e):
            raise AccountBannedError(
                "HTTP 403 — ACCOUNT_BANNED (аккаунт заблокирован, запросы с него прекращены)."
            ) from None
        self._log("warning", f"[!] {name}: HTTP {e.status} — {e.detail}")

    @staticmethod
    def _item_score(it: dict) -> float:
        return (
            (it.get("bonus_damage") or 0)
            + (it.get("bonus_hp") or 0) * 0.1
            + (it.get("bonus_crit") or 0.0) * 10
            + (it.get("bonus_speed") or 0) * 0.5
        )

    def _open_chests(self, token: str, version: str, chests) -> None:
        if not isinstance(chests, list):
            chests = [chests]
        for ch in chests:
            if self._stop_event.is_set():
                return
            if not ch:
                continue
            ct = ch if isinstance(ch, str) else (ch.get("chest_type") or ch.get("type"))
            is_boss = ch.get("is_boss_drop", True) if isinstance(ch, dict) else True
            if not ct:
                continue
            try:
                res = post_chest_open(token, version, ct, is_boss)
                w = res.get("weapon") or res.get("item")
                if w:
                    self._log(
                        "info",
                        (
                            f"Сундук {ct.upper()}: {w.get('weapon_type','?')} {w.get('item_type','?')} "
                            f"r={w.get('rarity','?')} dmg+{w.get('bonus_damage',0)} hp+{w.get('bonus_hp',0)} "
                            f"crit+{w.get('bonus_crit',0)} sp+{w.get('bonus_speed',0)}"
                        ),
                    )
                    self._emit("chest_opened", chest_type=ct, item=w)
            except SiegeApiError as e:
                self._safe_err(e, f"chest/{ct}")
            time.sleep(0.15)

    def _equip_best(self, token: str, version: str) -> None:
        try:
            inv = get_inventory(token, version)
        except SiegeApiError as e:
            self._safe_err(e, "inventory")
            return
        if not inv or not isinstance(inv, list):
            return

        rarity_order = {"common": 0, "uncommon": 1, "rare": 2, "epic": 3, "legendary": 4, "mythic": 5}
        equipped: dict[str, dict] = {}
        free_items: list[dict] = []
        for it in inv:
            if not isinstance(it, dict):
                continue
            if it.get("is_equipped"):
                equipped[it.get("equipped_slot") or it.get("item_type")] = it
            else:
                free_items.append(it)

        new_eq = 0
        for item in free_items:
            if self._stop_event.is_set():
                return
            slot = item.get("item_type") or item.get("equipped_slot")
            if not slot:
                continue
            cur = equipped.get(slot)
            cur_s = self._item_score(cur) if cur else -1
            new_s = self._item_score(item)
            cur_r = rarity_order.get((cur or {}).get("rarity", "common"), -1)
            new_r = rarity_order.get(item.get("rarity", "common"), 0)
            if new_s > cur_s or new_r > cur_r:
                try:
                    post_inventory_equip(token, version, item["id"], slot)
                    equipped[slot] = item
                    new_eq += 1
                    self._emit("equip_changed", slot=slot, item=item)
                except SiegeApiError as e:
                    self._safe_err(e, "inventory/equip")
            time.sleep(0.1)
        if new_eq:
            self._log("info", f"Экипировано: {new_eq}")

    def _upgrade_stats(self, token: str, version: str) -> None:
        try:
            prog = get_player_progress(token, version)
        except SiegeApiError as e:
            self._safe_err(e, "player/progress")
            return
        self._snapshot.level = prog.get("level")
        sp = prog.get("stat_points", 0) or 0
        if sp <= 0:
            return
        remaining = sp
        while remaining > 0 and not self._stop_event.is_set():
            tree = get_random_stat_tree()
            try:
                res = post_stat_upgrade(token, version, tree, 1)
                remaining -= 1
                self._emit("stats_upgraded", tree=tree, result=res)
            except SiegeApiError as e:
                self._safe_err(e, f"stat-upgrade/{tree}")
                break
            time.sleep(0.1)

    def _claim_bp(self, token: str, version: str) -> None:
        try:
            chs = get_bp_challenges(token, version)
        except SiegeApiError as e:
            self._safe_err(e, "bp/challenges")
            chs = {}

        for cat in ("daily", "weekly", "mini_event"):
            for c in (chs.get(cat) or []):
                if self._stop_event.is_set():
                    return
                if isinstance(c, dict) and c.get("is_completed") and not c.get("is_reward_claimed"):
                    uid = c.get("id")
                    if uid:
                        try:
                            post_bp_claim_challenge(token, version, uid)
                            self._emit("bp_claimed", kind="challenge", challenge=c)
                        except SiegeApiError as e:
                            self._safe_err(e, "bp/claim-challenge")
                    time.sleep(0.1)

        for s in (chs.get("seasonal") or []):
            if self._stop_event.is_set():
                return
            if isinstance(s, dict) and s.get("is_completed") and not s.get("is_claimed"):
                key = s.get("key") or s.get("challenge_key")
                if key:
                    try:
                        res = post_bp_claim_seasonal(token, version, key)
                        self._emit("bp_claimed", kind="seasonal", challenge=s, result=res)
                    except SiegeApiError as e:
                        self._safe_err(e, "bp/claim-seasonal")
                time.sleep(0.1)

        try:
            prog = get_bp_progress(token, version)
        except SiegeApiError as e:
            self._safe_err(e, "bp/progress")
            return
        self._snapshot.bp_tier = prog.get("current_tier")
        self._snapshot.bp_xp = prog.get("current_xp")

        tier = prog.get("current_tier", 0) or 0
        cf = set(prog.get("claimed_free") or [])
        cp = set(prog.get("claimed_premium") or [])
        is_prem = prog.get("is_premium", False)
        if tier <= 0:
            return

        need_free = [t for t in range(1, tier + 1) if t not in cf]
        need_prem = [t for t in range(1, tier + 1) if is_prem and t not in cp] if is_prem else []

        for t in need_free:
            if self._stop_event.is_set():
                return
            try:
                res = post_bp_claim_tier(token, version, t, "free")
                self._emit("bp_claimed", kind="tier_free", tier=t, result=res)
            except SiegeApiError as e:
                self._safe_err(e, f"bp/claim-tier/{t}")
            time.sleep(0.1)
        for t in need_prem:
            if self._stop_event.is_set():
                return
            try:
                res = post_bp_claim_tier(token, version, t, "premium")
                self._emit("bp_claimed", kind="tier_premium", tier=t, result=res)
            except SiegeApiError as e:
                self._safe_err(e, f"bp/claim-tier-premium/{t}")
            time.sleep(0.1)

    def _solve_captcha_loop(self, token: str, version: str, wave: dict) -> dict:
        cap = 0
        while wave.get("captcha_required"):
            if self._stop_event.is_set():
                return wave
            cap += 1
            if cap > 6:
                raise RuntimeError("Капча не решена за 6 попыток.")
            ct = (wave.get("captcha_type") or "").strip()
            if cap == 1:
                log_captcha_event(self.project_root, self.account.captcha_log_file, {
                    "event": "captcha_seen",
                    "wave_number": wave.get("wave_number"),
                    "captcha_type": ct,
                    "captcha_id": wave.get("captcha_id"),
                    "captcha_question": wave.get("captcha_question"),
                })
            answer = None
            solver_src = None
            if ct == "math":
                answer = try_local_math_answer(wave.get("captcha_question") or "")
                if answer:
                    solver_src = "local_math"
            elif ct == "color":
                answer = try_local_color_captcha(wave.get("captcha_question") or "")
                if answer:
                    solver_src = "local_color"
            elif ct == "emoji":
                answer = try_local_emoji_captcha(wave.get("captcha_question") or "")
                if answer:
                    solver_src = "local_emoji"
            elif ct == "word":
                answer = try_local_word_captcha(wave.get("captcha_question") or "")
                if answer:
                    solver_src = "local_word"
            if answer is None:
                raise RuntimeError("Нет локального решения для капчи.")
            try:
                cap_res = post_captcha_solve(token, version, answer)
            except SiegeApiError as e:
                self._safe_err(e, "captcha/solve")
            log_captcha_event(self.project_root, self.account.captcha_log_file, {
                "event": "captcha_solve_attempt",
                "attempt": cap,
                "wave_number": wave.get("wave_number"),
                "captcha_type": ct,
                "captcha_id": wave.get("captcha_id"),
                "captcha_question": wave.get("captcha_question"),
                "solver": solver_src or "local",
                "answer_submitted": answer,
                "correct": cap_res.get("correct"),
                "message": cap_res.get("message"),
            })
            if not cap_res.get("correct"):
                try:
                    wave = get_wave_current(token, version)
                except SiegeApiError as e:
                    self._safe_err(e, "wave/current")
                continue
            try:
                wave = get_wave_current(token, version)
            except SiegeApiError as e:
                self._safe_err(e, "wave/current")
        return wave

    def _run(self) -> None:
        token = (self.account.bearer_token or "").strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        if not token:
            self._set_error("Пустой bearer_token.")
            return

        try:
            self._check_token_not_expired(token)
            set_http_proxy(self.account.http_proxy)
            prof = self.account.browser_profile
            if not is_complete_browser_profile(prof):
                prof = generate_browser_profile()
            set_request_browser_profile(prof)
        except Exception as e:
            self._set_error(str(e))
            set_request_browser_profile(None)
            return

        self._snapshot.running = True
        self._emit("started")
        wave_i = 0
        version = self.account.client_version

        try:
            while not self._stop_event.is_set():
                wave_i += 1
                try:
                    wave = get_wave_current(token, version)
                except SiegeApiError as e:
                    self._safe_err(e, "wave/current")
                    time.sleep(self._next_sleep())
                    continue

                try:
                    wave = self._solve_captcha_loop(token, version, wave)
                except AccountBannedError as e:
                    self._set_account_banned(str(e))
                    break
                except Exception as e:
                    self._set_error(str(e))
                    break

                tw_ge = self.account.stealth_teleport_when_wave_ge
                tw_to = self.account.stealth_teleport_to_wave
                if tw_ge is not None and tw_to is not None:
                    try:
                        if int(wave.get("wave_number", 0)) >= int(tw_ge):
                            tw_raw = int(tw_to)
                            post_wave_teleport(token, version, tw_raw)
                            wave = get_wave_current(token, version)
                    except SiegeApiError as e:
                        ds = e.detail if isinstance(e.detail, str) else str(e.detail or "")
                        if e.status == 429 and "daily downward teleport limit" in ds.lower():
                            time.sleep(4 * 3600)
                            continue
                        self._safe_err(e, "wave/teleport")
                    except (TypeError, ValueError):
                        pass

                # По умолчанию «человеческий» босс (cheater_wave_mode выкл.). «Читерский» — без симуляции.
                if not self.account.cheater_wave_mode and wave.get("is_boss"):
                    can_win, sim_reason = simulate_boss_wave_win(wave)
                    if not can_win:
                        self._log(
                            "info",
                            f"Человеческий режим: босс волна {wave.get('wave_number')} — {sim_reason}. Отправляю поражение и прокачку…",
                        )
                        try:
                            post_wave_fail(token, version)
                        except SiegeApiError as e:
                            self._safe_err(e, "wave/fail")
                            time.sleep(self._next_sleep())
                            continue
                        if self.account.auto_equip:
                            self._equip_best(token, version)
                        if self.account.auto_upgrade_stats:
                            self._upgrade_stats(token, version)
                        if self.account.auto_claim_bp:
                            self._claim_bp(token, version)
                        time.sleep(self._next_sleep())
                        continue

                fail_ch = float(self.account.stealth_random_fail_chance or 0)
                if 0 < fail_ch < 1 and random.random() < fail_ch:
                    try:
                        post_wave_fail(token, version)
                    except SiegeApiError as e:
                        self._safe_err(e, "wave/fail")
                    time.sleep(self._next_sleep())
                    continue

                wn = wave.get("wave_number")
                floor, room = self._floor_room(wn)
                self._snapshot.wave = wn
                self._snapshot.floor = floor
                self._snapshot.room = room

                try:
                    done = post_wave_complete(token, version, wave)
                except SiegeApiError as e:
                    self._safe_err(e, "wave/complete")
                    time.sleep(self._next_sleep())
                    continue
                if not done.get("accepted"):
                    time.sleep(self._next_sleep())
                    continue

                chest = done.get("chest_drop")
                if chest and self.account.auto_open_chests:
                    self._open_chests(token, version, [chest])
                if self.account.auto_equip:
                    self._equip_best(token, version)
                if self.account.auto_upgrade_stats:
                    self._upgrade_stats(token, version)
                if self.account.auto_claim_bp:
                    self._claim_bp(token, version)

                try:
                    bal = get_token_balance(token, version)
                except SiegeApiError as e:
                    self._safe_err(e, "token/balance")
                    bal = {}
                self._snapshot.balance = bal.get("raw_balance")
                self._snapshot.total_earned = bal.get("total_earned")

                self._snapshot.updated_at = datetime.utcnow()

                tr = done.get("token_reward")
                xp = done.get("xp_reward")
                sc = done.get("scrap_reward")
                boss_txt = " · босс" if wave.get("is_boss") else ""
                self._log(
                    "info",
                    (
                        f"Победа в волне {wn}{boss_txt}  │  +{_fmt_ch_amount(tr)} $SIEGE  │  "
                        f"+{xp if xp is not None else 0} XP  │  +{sc if sc is not None else 0} лом  │  "
                        f"баланс {_fmt_ch_amount(self._snapshot.balance)} $SIEGE"
                    ),
                )
                self._emit(
                    "wave_done",
                    wave=wn,
                    floor=floor,
                    room=room,
                    is_boss=bool(wave.get("is_boss")),
                    token_reward=done.get("token_reward"),
                    xp_reward=done.get("xp_reward"),
                    scrap_reward=done.get("scrap_reward"),
                    next_wave=done.get("next_wave"),
                    chest_drop=chest,
                    leveled_up=done.get("leveled_up"),
                    balance=self._snapshot.balance,
                    total_earned=self._snapshot.total_earned,
                )
                time.sleep(self._next_sleep())
        except AccountBannedError as e:
            self._set_account_banned(str(e))
        except RuntimeError as e:
            self._set_error(str(e))
        except Exception as e:
            self._set_error(f"Необработанная ошибка: {e}")
        finally:
            set_request_browser_profile(None)
            self._snapshot.running = False
            self._snapshot.updated_at = datetime.utcnow()
            self._emit("stopped")
