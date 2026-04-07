from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from models import AccountConfig, AppSettings, BotSnapshot


def _cheater_wave_mode_from_storage_row(row: dict) -> bool:
    """
    True — режим «читерский»: босс без симуляции (как старый бот).
    False — по умолчанию «человеческий» босс с проверкой статов.
    Миграция: раньше было human_like_wave (инверсия).
    """
    if "cheater_wave_mode" in row:
        return bool(row["cheater_wave_mode"])
    if "human_like_wave" in row:
        return not bool(row.get("human_like_wave", False))
    return False


def _xor_bytes(data: bytes, key: bytes) -> bytes:
    out = bytearray(len(data))
    klen = len(key)
    for i, b in enumerate(data):
        out[i] = b ^ key[i % klen]
    return bytes(out)


class SecureBox:
    """
    Лёгкое локальное шифрование для секретов в JSON.
    Не заменяет HSM/OS vault, но лучше чем plaintext.
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.key_path = data_dir / ".secret.key"
        self.key = self._load_or_create_key()

    def _load_or_create_key(self) -> bytes:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if self.key_path.is_file():
            return self.key_path.read_bytes()
        key = secrets.token_bytes(32)
        self.key_path.write_bytes(key)
        return key

    def encrypt_text(self, plain: str) -> str:
        nonce = secrets.token_bytes(16)
        stream = hashlib.pbkdf2_hmac("sha256", self.key, nonce, 100_000, dklen=32)
        cipher = _xor_bytes(plain.encode("utf-8"), stream)
        return base64.b64encode(nonce + cipher).decode("ascii")

    def decrypt_text(self, token: str) -> str:
        raw = base64.b64decode(token.encode("ascii"))
        nonce, cipher = raw[:16], raw[16:]
        stream = hashlib.pbkdf2_hmac("sha256", self.key, nonce, 100_000, dklen=32)
        plain = _xor_bytes(cipher, stream)
        return plain.decode("utf-8")


class AccountStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.data_dir = root / "data"
        self.path = self.data_dir / "accounts.json"
        self.sec = SecureBox(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            self._write({"accounts": []})

    def _read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, obj: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def list_accounts(self) -> list[AccountConfig]:
        raw = self._read().get("accounts", [])
        out: list[AccountConfig] = []
        for row in raw:
            token_enc = row.get("bearer_token_enc")
            token = self.sec.decrypt_text(token_enc) if token_enc else ""
            out.append(
                AccountConfig(
                    account_id=row["account_id"],
                    name=row.get("name") or row["account_id"],
                    bearer_token=token,
                    client_version=row.get("client_version", "mngmv2a9"),
                    http_proxy=row.get("http_proxy"),
                    captcha_log_file=row.get("captcha_log_file", "captcha_history.jsonl"),
                    auto_open_chests=bool(row.get("auto_open_chests", True)),
                    auto_equip=bool(row.get("auto_equip", True)),
                    auto_upgrade_stats=bool(row.get("auto_upgrade_stats", True)),
                    auto_claim_bp=bool(row.get("auto_claim_bp", True)),
                    cheater_wave_mode=_cheater_wave_mode_from_storage_row(row),
                    stealth_teleport_when_wave_ge=row.get("stealth_teleport_when_wave_ge"),
                    stealth_teleport_to_wave=row.get("stealth_teleport_to_wave"),
                    stealth_random_fail_chance=float(row.get("stealth_random_fail_chance") or 0),
                    sleep_min_seconds=float(row.get("sleep_min_seconds", 8.0)),
                    sleep_max_seconds=float(row.get("sleep_max_seconds", 14.0)),
                )
            )
        return out

    def upsert_account(self, account: AccountConfig) -> None:
        data = self._read()
        rows = data.get("accounts", [])
        body = asdict(account)
        token = body.pop("bearer_token", "")
        body["bearer_token_enc"] = self.sec.encrypt_text(token)
        found = False
        for i, row in enumerate(rows):
            if row.get("account_id") == account.account_id:
                rows[i] = body
                found = True
                break
        if not found:
            rows.append(body)
        data["accounts"] = rows
        self._write(data)

    def delete_account(self, account_id: str) -> None:
        data = self._read()
        data["accounts"] = [a for a in data.get("accounts", []) if a.get("account_id") != account_id]
        self._write(data)


class WalletStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.data_dir = root / "data"
        self.path = self.data_dir / "wallets.json"
        self.sec = SecureBox(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            self.path.write_text("{}", encoding="utf-8")

    def _read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, obj: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def put_wallet(self, account_id: str, wallet_address: str, private_key_b58: str) -> None:
        db = self._read()
        db[account_id] = {
            "wallet_address": wallet_address,
            "private_key_enc": self.sec.encrypt_text(private_key_b58),
        }
        self._write(db)

    def get_wallet(self, account_id: str) -> dict[str, str] | None:
        db = self._read()
        row = db.get(account_id)
        if not row:
            return None
        return {
            "wallet_address": row["wallet_address"],
            "private_key_b58": self.sec.decrypt_text(row["private_key_enc"]),
        }

    def get_all_wallets(self) -> dict[str, dict[str, str]]:
        """Один проход по файлу — для таблицы/экспорта, без N отдельных чтений."""
        db = self._read()
        out: dict[str, dict[str, str]] = {}
        for aid, row in db.items():
            if not isinstance(row, dict):
                continue
            key = str(aid)
            try:
                pk = self.sec.decrypt_text(row.get("private_key_enc") or "")
            except Exception:
                pk = ""
            out[key] = {
                "wallet_address": str(row.get("wallet_address") or ""),
                "private_key_b58": pk,
            }
        return out


class ProxyPoolStore:
    """Пул прокси: одна строка = один прокси (формат как в siege_client)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.data_dir = root / "data"
        self.path = self.data_dir / "proxy_pool.json"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            self._write({"proxies": []})

    def _read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, obj: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def list_all(self) -> list[str]:
        return list(self._read().get("proxies", []))

    def count(self) -> int:
        return len(self.list_all())

    def add_raw_text(self, text: str, normalize_line: Callable[[str], str | None]) -> tuple[int, int]:
        """Возвращает (добавлено, дубликатов пропущено). Дубликаты считаются по нормализованному виду (не по сырой строке)."""
        xs = self.list_all()
        seen_norms: set[str] = set()
        for x in xs:
            nx = normalize_line(x)
            if nx:
                seen_norms.add(nx)
            else:
                st = str(x).strip()
                if st:
                    seen_norms.add(st)
        added = 0
        skipped_dup = 0
        for raw in text.splitlines():
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            n = normalize_line(s)
            if not n:
                continue
            if n in seen_norms:
                skipped_dup += 1
                continue
            xs.append(n)
            seen_norms.add(n)
            added += 1
        self._write({"proxies": xs})
        return added, skipped_dup

    def dedupe_normalized(self, normalize_fn: Callable[[str], str | None]) -> int:
        """Удаляет из пула повторы одного и того же прокси (по нормализации). Возвращает, сколько строк убрали."""
        xs = self.list_all()
        before = len(xs)
        out: list[str] = []
        keys: set[str] = set()
        for line in xs:
            n = normalize_fn(line)
            if n:
                if n in keys:
                    continue
                keys.add(n)
                out.append(n)
                continue
            st = str(line).strip()
            if not st or st in keys:
                continue
            keys.add(st)
            out.append(st)
        self._write({"proxies": out})
        return before - len(out)

    def peek_front(self) -> str | None:
        xs = self.list_all()
        return xs[0] if xs else None

    def pop_front(self) -> str | None:
        data = self._read()
        xs = list(data.get("proxies", []))
        if not xs:
            return None
        p = xs.pop(0)
        data["proxies"] = xs
        self._write(data)
        return p

    def peek_first_unused(
        self, normalize_fn: Callable[[str], str | None], occupied_norms: set[str]
    ) -> str | None:
        """Первая строка пула, ещё не закреплённая за аккаунтом (по нормализованному виду)."""
        for line in self.list_all():
            n = normalize_fn(line)
            if n and n not in occupied_norms:
                return line
        return None

    def ensure_in_pool(self, normalize_fn: Callable[[str], str | None], norm: str | None) -> bool:
        """Добавляет прокси в пул в нормализованном виде, если такого прокси ещё нет (дубликат не записываем)."""
        if not norm:
            return False
        xs = self.list_all()
        for line in xs:
            if normalize_fn(line) == norm:
                return False
        xs.append(norm)
        self._write({"proxies": xs})
        return True

    def remove_at_indices(self, indices: list[int]) -> None:
        xs = self.list_all()
        for i in sorted({j for j in indices if isinstance(j, int)}, reverse=True):
            if 0 <= i < len(xs):
                xs.pop(i)
        self._write({"proxies": xs})

    def clear(self) -> None:
        self._write({"proxies": []})

    def remove_unassigned_only(
        self, normalize_fn: Callable[[str], str | None], occupied_norms: set[str]
    ) -> int:
        """Удаляет из пула только строки без аккаунта; закреплённые по нормализации оставляет (по одной копии)."""
        xs = self.list_all()
        before = len(xs)
        out: list[str] = []
        keys: set[str] = set()
        for line in xs:
            n = normalize_fn(line)
            if n and n in occupied_norms:
                if n not in keys:
                    keys.add(n)
                    out.append(n)
        removed = before - len(out)
        self._write({"proxies": out})
        return removed


class AccountManagerCacheStore:
    """Кэш строк для менеджера аккаунтов (баланс, уровень и т.д.) без приватных ключей."""

    def __init__(self, root: Path) -> None:
        self.path = root / "data" / "account_manager_cache.json"
        self.data_dir = root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            self._write({"version": 1, "rows": {}})

    def _read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, obj: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_rows(self) -> dict[str, dict[str, Any]]:
        return dict(self._read().get("rows", {}))

    def upsert_partial(self, account_id: str, patch: dict[str, Any]) -> None:
        safe = {k: v for k, v in patch.items() if v is not None and k != "private_key"}
        if not safe:
            return
        data = self._read()
        rows: dict[str, Any] = dict(data.get("rows", {}))
        cur = dict(rows.get(account_id, {}))
        cur.update(safe)
        cur["updated_at"] = datetime.utcnow().isoformat() + "Z"
        rows[account_id] = cur
        data["rows"] = rows
        self._write(data)

    def upsert_many_partials(self, patches: dict[str, dict[str, Any]]) -> None:
        """Одно чтение и одна запись вместо N раз upsert_partial — убирает фриз UI при старте."""
        if not patches:
            return
        now = datetime.utcnow().isoformat() + "Z"
        data = self._read()
        rows: dict[str, Any] = dict(data.get("rows", {}))
        changed = False
        for account_id, patch in patches.items():
            safe = {k: v for k, v in patch.items() if v is not None and k != "private_key"}
            if not safe:
                continue
            cur = dict(rows.get(account_id, {}))
            cur.update(safe)
            cur["updated_at"] = now
            rows[account_id] = cur
            changed = True
        if changed:
            data["rows"] = rows
            self._write(data)

    def prune_removed_accounts(self, valid_ids: set[str]) -> None:
        data = self._read()
        rows = {k: v for k, v in data.get("rows", {}).items() if k in valid_ids}
        data["rows"] = rows
        self._write(data)


class AppSettingsStore:
    def __init__(self, root: Path) -> None:
        self.path = root / "data" / "app_settings.json"
        self.data_dir = root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def load(self) -> AppSettings:
        if not self.path.is_file():
            return AppSettings()
        row = json.loads(self.path.read_text(encoding="utf-8"))
        return AppSettings(
            sleep_min_seconds=float(row.get("sleep_min_seconds", 8.0)),
            sleep_max_seconds=float(row.get("sleep_max_seconds", 14.0)),
            auto_open_chests=bool(row.get("auto_open_chests", True)),
            auto_equip=bool(row.get("auto_equip", True)),
            auto_upgrade_stats=bool(row.get("auto_upgrade_stats", True)),
            auto_claim_bp=bool(row.get("auto_claim_bp", True)),
            cheater_wave_mode=_cheater_wave_mode_from_storage_row(row),
        )

    def save(self, settings: AppSettings) -> None:
        self.path.write_text(
            json.dumps(asdict(settings), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


class StateStore:
    def __init__(self, root: Path) -> None:
        self.path = root / "data" / "state"
        self.path.mkdir(parents=True, exist_ok=True)

    def save_snapshot(self, snapshot: BotSnapshot) -> None:
        p = self.path / f"{snapshot.account_id}.json"
        row = asdict(snapshot)
        # Состояние «бот запущен» не восстанавливаем после перезапуска — на диске всегда оффлайн.
        row["running"] = False
        row["updated_at"] = snapshot.updated_at.isoformat()
        p.write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding="utf-8")

    def load_snapshot(self, account_id: str) -> BotSnapshot | None:
        p = self.path / f"{account_id}.json"
        if not p.is_file():
            return None
        row = json.loads(p.read_text(encoding="utf-8"))
        snap = BotSnapshot(account_id=account_id)
        for k, v in row.items():
            if k == "updated_at" or not hasattr(snap, k):
                continue
            setattr(snap, k, v)
        ua = row.get("updated_at")
        if isinstance(ua, str):
            try:
                snap.updated_at = datetime.fromisoformat(ua.replace("Z", "+00:00"))
            except ValueError:
                snap.updated_at = datetime.utcnow()
        snap.running = False
        return snap


def ensure_project_data_initialized(root: Path) -> None:
    """
    Создаёт data/, state/ и JSON-файлы с дефолтами до открытия окна.
    Безопасно вызывать при каждом запуске (идемпотентно).
    """
    AccountStore(root)
    WalletStore(root)
    ProxyPoolStore(root)
    AccountManagerCacheStore(root)
    AppSettingsStore(root)
    StateStore(root)
