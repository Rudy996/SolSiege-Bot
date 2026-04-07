import base64
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://api.solsiege.com"

_tls = threading.local()
_tls_browser = threading.local()

# Отпечаток до выставления профиля аккаунта (регистрация/одиночный вызов)
_LEGACY_PROOF_INNER_BASE = {
    "sc": "1920x1080x24",
    "lang": "ru",
    "plt": "Win32",
    "mem": 8,
    "hw": 16,
    "cv": "oega4d",
    "touch": False,
    "gl": (
        "ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Ti (0x00002803) "
        "Direct3D11 vs_5_0 ps_5_0, D3D11)"
    ),
    "tz": -180,
    "vis": "visible",
}


def set_request_browser_profile(profile: dict | None) -> None:
    """Профиль из AccountConfig.browser_profile; None — сброс (заголовки без UA-маскировки)."""
    if profile:
        _tls_browser.profile = profile
    else:
        if hasattr(_tls_browser, "profile"):
            delattr(_tls_browser, "profile")


# Если профиль потока не выставлен — не светим Python-urllib (общий вид «как браузер»).
_FALLBACK_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://solsiege.com/",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Dest": "empty",
}


def _browser_header_overlay() -> dict[str, str]:
    p = getattr(_tls_browser, "profile", None)
    if not p or not isinstance(p, dict):
        return dict(_FALLBACK_BROWSER_HEADERS)
    h: dict[str, str] = {}
    ua = p.get("user_agent")
    if ua:
        h["User-Agent"] = str(ua)
    if p.get("accept_language"):
        h["Accept-Language"] = str(p["accept_language"])
    if p.get("sec_ch_ua"):
        h["sec-ch-ua"] = str(p["sec_ch_ua"])
    if p.get("sec_ch_ua_mobile") is not None:
        h["sec-ch-ua-mobile"] = str(p["sec_ch_ua_mobile"])
    if p.get("sec_ch_ua_platform"):
        h["sec-ch-ua-platform"] = str(p["sec_ch_ua_platform"])
    full_list = p.get("sec_ch_ua_full_version_list")
    if full_list:
        h["Sec-CH-UA-Full-Version-List"] = str(full_list)
    h["Accept"] = "*/*"
    h["Referer"] = "https://solsiege.com/"
    h["Sec-Fetch-Mode"] = "cors"
    h["Sec-Fetch-Site"] = "cross-site"
    h["Sec-Fetch-Dest"] = "empty"
    if "User-Agent" not in h:
        h["User-Agent"] = _FALLBACK_BROWSER_HEADERS["User-Agent"]
    if "Accept-Language" not in h:
        h["Accept-Language"] = _FALLBACK_BROWSER_HEADERS["Accept-Language"]
    return h


def _build_api_headers(
    bearer_token: str | None,
    client_version: str,
    with_json_body: bool,
) -> dict[str, str]:
    headers: dict[str, str] = {
        "X-Client-Version": client_version,
        "Origin": "https://solsiege.com",
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    headers.update(_browser_header_overlay())
    if with_json_body:
        headers["Content-Type"] = "application/json"
    return headers


def _parse_proxy_line(line: str | None) -> tuple[str | None, str | None]:
    """
    host:port:user:pass или host:port без авторизации.
    Возвращает (url для ProxyHandler, строка для лога без пароля).
    """
    if line is None:
        return None, None
    s = str(line).strip()
    if not s:
        return None, None
    # Normalize common paste formats from UI:
    # - host:port:user:pass
    # - host port user pass
    # - host;port;user;pass
    # - http://user:pass@host:port
    # - http://host:port
    s = s.replace("\r", "").replace("\n", "").replace("\t", "")
    s = s.replace(";", ":").replace(",", ":").replace("：", ":")
    s = s.strip().strip("'").strip('"')
    if s.startswith("http://") or s.startswith("https://"):
        rest = s.split("://", 1)[1]
        if "@" in rest:
            creds, hp = rest.split("@", 1)
            if ":" in creds and ":" in hp:
                user, pwd = creds.split(":", 1)
                host, port = hp.rsplit(":", 1)
                s = f"{host}:{port}:{user}:{pwd}"
            else:
                s = hp
        else:
            s = rest
    if " " in s and ":" not in s:
        s = ":".join([p for p in s.split() if p])
    else:
        s = s.replace(" ", "")

    parts = s.split(":", 3)
    if len(parts) == 4:
        host, port, user, pwd = parts
        url = "http://%s:%s@%s:%s" % (
            urllib.parse.quote(user, safe=""),
            urllib.parse.quote(pwd, safe=""),
            host,
            port,
        )
        log_safe = f"{host}:{port} (user {user})"
        return url, log_safe
    if len(parts) == 2:
        host, port = parts
        url = f"http://{host}:{port}"
        return url, f"{host}:{port}"
    raise ValueError(
        "http_proxy: ожидается host:port или host:port:user:pass, "
        f"получено {len(parts)} частей после разбора по ':'"
    )


def set_http_proxy(line: str | None) -> str | None:
    """
    Включает HTTP(S)-прокси для запросов в **текущем потоке** (thread-local).
    Возвращает безопасную строку для лога или None, если прокси отключён.
    """
    url, log_safe = _parse_proxy_line(line)
    _tls.http_proxy_url = url
    return log_safe


def _opener():
    url = getattr(_tls, "http_proxy_url", None)
    if url:
        h = urllib.request.ProxyHandler({"http": url, "https": url})
        return urllib.request.build_opener(h)
    return urllib.request.build_opener()


PROXY_TUNNEL_502_RETRY_SEC = 10


def _is_tunnel_502_bad_gateway(exc: BaseException) -> bool:
    """CONNECT через прокси вернул 502 (апстрим/прокси временно отвалился)."""
    msg = str(exc).lower()
    return "tunnel connection failed" in msg and "502" in msg


def _urlopen_with_tunnel_502_retry(req: urllib.request.Request, timeout: float = 60):
    """
    Открывает URL; при <urlopen error Tunnel connection failed: 502 Bad Gateway>
    ждёт PROXY_TUNNEL_502_RETRY_SEC секунд и повторяет (без лимита попыток).
    HTTPError и прочие сетевые ошибки не маскируем.
    """
    while True:
        try:
            return _opener().open(req, timeout=timeout)
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, OSError) as e:
            if _is_tunnel_502_bad_gateway(e):
                time.sleep(PROXY_TUNNEL_502_RETRY_SEC)
                continue
            raise


def uses_proxy() -> bool:
    return getattr(_tls, "http_proxy_url", None) is not None


class SiegeApiError(Exception):
    def __init__(self, status, detail):
        self.status = status
        self.detail = detail
        super().__init__(f"[{status}] {detail}")


def _request(bearer_token, client_version, method, path, body=None):
    url = BASE_URL + path
    data = None
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = _build_api_headers(bearer_token, client_version, body is not None)
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with _urlopen_with_tunnel_502_retry(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"detail": raw[:500] if raw else e.reason}
        detail = payload.get("detail", payload)
        raise SiegeApiError(e.code, detail) from None


def _request_public(client_version, method, path, body=None, bearer_token: str | None = None):
    url = BASE_URL + path
    data = None
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = _build_api_headers(bearer_token, client_version, body is not None)
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with _urlopen_with_tunnel_502_retry(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"detail": raw[:500] if raw else e.reason}
        detail = payload.get("detail", payload)
        raise SiegeApiError(e.code, detail) from None


def client_proof():
    p = getattr(_tls_browser, "profile", None)
    inner_src = None
    if p and isinstance(p, dict) and isinstance(p.get("proof_inner"), dict):
        inner_src = p["proof_inner"]
    if inner_src is None:
        inner_src = _LEGACY_PROOF_INNER_BASE
    e = dict(inner_src)
    e["ts"] = int(time.time() * 1000)
    return base64.b64encode(json.dumps(e, separators=(",", ":")).encode()).decode()


def build_enemy_kills(wave):
    n = wave["enemy_count"]
    pool = list(wave["enemy_pool"])
    if wave.get("is_boss") and wave.get("boss_info"):
        bi = wave["boss_info"]
        boss_t = bi["boss_type"]
        mins = list(bi.get("minion_pool") or pool)
        total = int(bi.get("total_expected_kills") or n)
        kills = {boss_t: 1}
        left = total - 1
        i = 0
        while left > 0 and mins:
            t = mins[i % len(mins)]
            kills[t] = kills.get(t, 0) + 1
            left -= 1
            i += 1
        return kills
    if n <= len(pool):
        return {pool[i]: 1 for i in range(n)}
    kills = {t: 1 for t in pool}
    left = n - len(pool)
    kills[pool[0]] = kills.get(pool[0], 0) + left
    return kills


def wave_modifier_name(wave):
    mod = wave.get("modifier")
    if isinstance(mod, dict) and "name" in mod:
        return mod["name"]
    return None


def complete_payload(wave):
    return {
        "wave_number": wave["wave_number"],
        "is_boss": wave["is_boss"],
        "wave_token": wave["wave_token"],
        "enemy_kills": build_enemy_kills(wave),
        "time_elapsed_ms": 12_000 + wave["enemy_count"] * 800,
        "modifier": wave_modifier_name(wave),
        "champion_killed": False,
        "client_proof": client_proof(),
        "wave_nonce": wave["wave_nonce"],
    }


def get_wave_current(bearer_token, client_version):
    st, data = _request(bearer_token, client_version, "GET", "/wave/current")
    return data


def post_wave_complete(bearer_token, client_version, wave):
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        "/wave/complete",
        complete_payload(wave),
    )
    return data


def get_token_balance(bearer_token, client_version):
    st, data = _request(bearer_token, client_version, "GET", "/token/balance")
    return data


def post_captcha_solve(bearer_token, client_version, answer: str):
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        "/player/captcha/solve",
        {"answer": str(answer).strip()},
    )
    return data


def post_wave_fail(bearer_token, client_version):
    """Намеренный проигрыш текущей волны (как в клиенте при поражении). Тело пустое."""
    url = BASE_URL + "/wave/fail"
    headers = _build_api_headers(bearer_token, client_version, False)
    req = urllib.request.Request(url, data=b"", method="POST", headers=headers)
    try:
        with _urlopen_with_tunnel_502_retry(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"detail": raw[:500] if raw else e.reason}
        detail = payload.get("detail", payload)
        raise SiegeApiError(e.code, detail) from None


def align_teleport_target_wave(target: int) -> int:
    """
    API принимает только «начало этажа»: 1, 11, 21, 31, … (сообщение сервера: floor start).
    Любое число приводим вниз к ближайшему допустимому.
    """
    t = int(target)
    if t < 1:
        return 1
    return 1 + 10 * ((t - 1) // 10)


def post_wave_teleport(bearer_token, client_version, target_wave: int):
    """Прыжок на целевую волну (в игре может стоить ресурсов / иметь лимиты)."""
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        "/wave/teleport",
        {"target_wave": align_teleport_target_wave(target_wave)},
    )
    return data


def get_auth_nonce(client_version, wallet_address: str):
    q = urllib.parse.quote(wallet_address, safe="")
    st, data = _request_public(client_version, "GET", f"/auth/nonce?wallet={q}")
    return data


def post_auth_login(client_version, wallet_address: str, signature: str, referral_code=None):
    payload = {
        "wallet_address": wallet_address,
        "signature": signature,
        "referral_code": referral_code,
        "client_proof": client_proof(),
    }
    st, data = _request_public(client_version, "POST", "/auth/login", payload)
    return data


def get_auth_me(bearer_token, client_version):
    st, data = _request_public(client_version, "GET", "/auth/me", bearer_token=bearer_token)
    return data


def get_character_classes(bearer_token, client_version):
    st, data = _request_public(client_version, "GET", "/character/classes", bearer_token=bearer_token)
    return data


def get_character_list(bearer_token, client_version):
    st, data = _request_public(client_version, "GET", "/character/list", bearer_token=bearer_token)
    return data


def post_character_create(bearer_token, client_version, character_class: str, name: str):
    st, data = _request_public(
        client_version,
        "POST",
        "/character/create",
        {"character_class": character_class, "name": name},
        bearer_token=bearer_token,
    )
    return data


# ── Player Progress ──

def get_player_progress(bearer_token, client_version):
    """Уровень, XP, stat_points, stats_tree, computed, materials, mastery."""
    st, data = _request(bearer_token, client_version, "GET", "/player/progress")
    return data


# ── Inventory & Equip ──

def get_inventory(bearer_token, client_version):
    """Список всех предметов игрока."""
    st, data = _request(bearer_token, client_version, "GET", "/inventory")
    return data if isinstance(data, list) else []


def post_inventory_equip(bearer_token, client_version, weapon_id: str, target_slot: str):
    """Экипировать предмет в конкретный слот."""
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        "/inventory/equip",
        {"weapon_id": weapon_id, "target_slot": target_slot},
    )
    return data


# ── Stat Upgrade ──

_STAT_TREES = ["attack", "crit", "speed", "collector", "defense"]


def post_stat_upgrade(bearer_token, client_version, tree: str, points: int = 1):
    """
    Прокачать ветку дерева статов.
    tree: attack | crit | speed | collector | defense
    """
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        "/player/stat-upgrade",
        {"tree": tree, "points": points},
    )
    return data


def get_random_stat_tree():
    import random
    return random.choice(_STAT_TREES)


# ── Chest Open ──

def post_chest_open(bearer_token, client_version, chest_type: str, is_boss_drop: bool = False):
    """Открыть сундук за токены."""
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        "/chest/open",
        {"chest_type": chest_type, "is_boss_drop": is_boss_drop, "sol_tx_signature": None},
    )
    return data


# ── Battle Pass ──

def get_bp_season(bearer_token, client_version):
    """Структура сезона: тиров, free/premium награды."""
    st, data = _request(bearer_token, client_version, "GET", "/battle-pass/season")
    return data


def get_bp_progress(bearer_token, client_version):
    """Текущий тир, XP, claimed_free/claimed_premium, streak."""
    st, data = _request(bearer_token, client_version, "GET", "/battle-pass/progress")
    return data


def get_bp_challenges(bearer_token, client_version):
    """All challenges: daily, weekly, mini_event, seasonal."""
    st, data = _request(bearer_token, client_version, "GET", "/battle-pass/challenges")
    return data


def post_bp_claim_tier(bearer_token, client_version, tier: int, track: str = "free"):
    """Claim награды BP за tier (track: free | premium)."""
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        f"/battle-pass/claim/{tier}",
        {"track": track},
    )
    return data


def post_bp_claim_challenge(bearer_token, client_version, challenge_uuid: str):
    """Claim завершённого челленджа (daily/weekly/mini_event)."""
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        f"/battle-pass/claim-challenge/{challenge_uuid}",
        None,
    )
    return data


def post_bp_claim_seasonal(bearer_token, client_version, challenge_key: str):
    """Claim сезонного челленджа (награда XP)."""
    st, data = _request(
        bearer_token,
        client_version,
        "POST",
        f"/battle-pass/claim-seasonal/{challenge_key}",
        None,
    )
    return data
