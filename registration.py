from __future__ import annotations

import hashlib
import os
import random
import re
from dataclasses import dataclass

from browser_profile import generate_browser_profile
from models import AccountConfig
from siege_client import (
    get_auth_me,
    get_auth_nonce,
    get_character_classes,
    post_auth_login,
    post_character_create,
    set_http_proxy,
    set_request_browser_profile,
)

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    chars: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        chars.append(_B58_ALPHABET[r])
    # leading zeros
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return "1" * pad + ("".join(reversed(chars)) if chars else "")


def b58decode(text: str) -> bytes:
    n = 0
    for ch in text:
        n = n * 58 + _B58_ALPHABET.index(ch)
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(text) - len(text.lstrip("1"))
    return b"\x00" * pad + raw


def _sign_message_ed25519(seed32: bytes, message: str) -> tuple[str, str]:
    # Prefer pynacl if available; fallback to cryptography.
    try:
        from nacl.signing import SigningKey  # type: ignore

        sk = SigningKey(seed32)
        vk = sk.verify_key
        signature = sk.sign(message.encode("utf-8")).signature
        wallet = b58encode(bytes(vk))
        return wallet, b58encode(signature)
    except Exception:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

        sk = Ed25519PrivateKey.from_private_bytes(seed32)
        vk = sk.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        signature = sk.sign(message.encode("utf-8"))
        wallet = b58encode(vk)
        return wallet, b58encode(signature)


def random_nickname() -> str:
    adjectives = [
        "Silent",
        "Night",
        "Nova",
        "Iron",
        "Shadow",
        "Echo",
        "Pixel",
        "Rogue",
        "Frost",
        "Blaze",
    ]
    nouns = [
        "Mage",
        "Archer",
        "Knight",
        "Fox",
        "Wolf",
        "Raven",
        "Storm",
        "Rune",
        "Viper",
        "Drake",
    ]
    return f"{random.choice(adjectives)}{random.choice(nouns)}{random.randint(10,9999)}"


@dataclass
class RegistrationResult:
    account: AccountConfig
    wallet_address: str
    private_key_b58: str
    player_id: str | None
    character_id: str | None


def register_new_account_api_only(
    proxy: str | None,
    client_version: str = "mngmv2a9",
    nickname: str | None = None,
    character_class: str = "mage",
) -> RegistrationResult:
    set_http_proxy(proxy)
    profile = generate_browser_profile()
    set_request_browser_profile(profile)
    try:
        seed = os.urandom(32)
        wallet_address, _ = _sign_message_ed25519(seed, "bootstrap")

        nonce_payload = get_auth_nonce(client_version, wallet_address)
        msg = nonce_payload.get("message", "")
        if not msg:
            raise RuntimeError(f"auth/nonce не вернул message: {nonce_payload}")

        wallet_address, signature = _sign_message_ed25519(seed, msg)

        login = post_auth_login(client_version, wallet_address, signature, referral_code=None)
        access_token = login.get("access_token")
        if not access_token:
            raise RuntimeError(f"auth/login не вернул access_token: {login}")

        me = get_auth_me(access_token, client_version)
        player_id = me.get("player_id")

        classes = get_character_classes(access_token, client_version)
        class_names = [c.get("name") for c in classes.get("classes", []) if isinstance(c, dict)]
        selected_class = (
            character_class if character_class in class_names else (class_names[0] if class_names else "mage")
        )
        nick = nickname or random_nickname()
        nick = re.sub(r"[^A-Za-z0-9_]", "", nick)[:16] or random_nickname()

        ch = post_character_create(access_token, client_version, selected_class, nick)
        character_id = (ch.get("character") or {}).get("id")

        account = AccountConfig(
            account_id=player_id or hashlib.sha1(wallet_address.encode("utf-8")).hexdigest()[:10],
            name=nick,
            bearer_token=access_token,
            client_version=client_version,
            http_proxy=proxy,
            browser_profile=profile,
        )
        private_key_b58 = b58encode(seed)
        return RegistrationResult(
            account=account,
            wallet_address=wallet_address,
            private_key_b58=private_key_b58,
            player_id=player_id,
            character_id=character_id,
        )
    finally:
        set_request_browser_profile(None)
