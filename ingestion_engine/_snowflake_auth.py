from __future__ import annotations

import hashlib
import time
from base64 import b64encode
from typing import Optional

import httpx
import jwt
from cryptography.hazmat.primitives import serialization

from ingestion_engine.config import EngineConfig


def _compute_public_key_fingerprint(private_key_pem: bytes) -> str:
    private_key = serialization.load_pem_private_key(private_key_pem, password=None)
    public_key = private_key.public_key()
    der = public_key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    digest = hashlib.sha256(der).digest()
    return f"SHA256:{b64encode(digest).decode()}"


class _TokenCache:
    def __init__(self):
        self._cache: dict[str, tuple[str, float]] = {}

    def get(self, key: str) -> Optional[str]:
        entry = self._cache.get(key)
        if entry and entry[1] > time.time() + 300:
            return entry[0]
        return None

    def set(self, key: str, token: str, expires_in: int):
        self._cache[key] = (token, time.time() + expires_in)

    def clear(self, key: Optional[str] = None):
        if key:
            self._cache.pop(key, None)
        else:
            self._cache.clear()


_token_cache = _TokenCache()


async def mint_access_token(config: EngineConfig, runtime_name: str) -> str:
    cached = _token_cache.get(runtime_name)
    if cached:
        return cached

    if config.is_spcs and config.snowflake_oauth_token_path:
        token = config.snowflake_oauth_token_path.read_text().strip()
        _token_cache.set(runtime_name, token, 3600)
        return token

    if not config.snowflake_private_key_path:
        raise ValueError("snowflake_private_key_path required for JWT auth")

    key_pem = config.snowflake_private_key_path.read_bytes()
    account = config.snowflake_account.replace(".", "-").upper()
    username = config.snowflake_user.upper()
    fp = _compute_public_key_fingerprint(key_pem)

    now = int(time.time())
    payload = {
        "iss": f"{account}.{username}.{fp}",
        "sub": f"{account}.{username}",
        "iat": now,
        "exp": now + 60,
    }

    private_key = serialization.load_pem_private_key(
        key_pem,
        password=config.snowflake_private_key_passphrase.encode() if config.snowflake_private_key_passphrase else None,
    )
    signed_jwt = jwt.encode(payload, private_key, algorithm="RS256")

    account_host = config.snowflake_account.replace("_", "-").lower()
    token_url = f"https://{account_host}.snowflakecomputing.com/oauth/token"
    params = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": signed_jwt,
    }
    if config.snowflake_role:
        params["scope"] = f"SESSION:ROLE:{config.snowflake_role.upper()}"

    async with httpx.AsyncClient() as client:
        r = await client.post(token_url, data=params, headers={"Content-Type": "application/x-www-form-urlencoded"})
        if r.status_code >= 400:
            raise RuntimeError(f"Snowflake token exchange failed ({r.status_code}): {r.text}")

        body = r.text.strip()
        token = body
        expires_in = 3600
        if body.startswith("{"):
            import json
            data = json.loads(body)
            token = data.get("access_token") or data.get("token") or body
            expires_in = data.get("expires_in", 3600)

    _token_cache.set(runtime_name, token, expires_in)
    return token
