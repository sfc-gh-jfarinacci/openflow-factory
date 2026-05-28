from __future__ import annotations

from typing import Any, Optional

import snowflake.connector

from ingestion_engine.config import EngineConfig
from ingestion_engine.exceptions import SnowflakeConnectionError


class SnowflakeConn:
    def __init__(self, config: EngineConfig):
        self._config = config
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    def _ensure_connected(self):
        if self._conn and not self._conn.is_closed():
            return
        cfg = self._config
        opts: dict[str, Any] = {
            "account": cfg.snowflake_account,
            "user": cfg.snowflake_user,
            "role": cfg.snowflake_role,
            "warehouse": cfg.snowflake_warehouse,
            "database": cfg.snowflake_database,
            "schema": cfg.snowflake_schema,
        }
        if cfg.is_spcs and cfg.snowflake_oauth_token_path:
            opts["authenticator"] = "oauth"
            opts["token"] = cfg.snowflake_oauth_token_path.read_text().strip()
            if cfg.snowflake_host:
                opts["host"] = cfg.snowflake_host
        elif cfg.snowflake_private_key_path:
            from cryptography.hazmat.primitives import serialization
            key_bytes = cfg.snowflake_private_key_path.read_bytes()
            p_key = serialization.load_pem_private_key(
                key_bytes,
                password=cfg.snowflake_private_key_passphrase.encode() if cfg.snowflake_private_key_passphrase else None,
            )
            pkb = p_key.private_bytes(
                serialization.Encoding.DER,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            opts["private_key"] = pkb
        else:
            raise SnowflakeConnectionError("No authentication method available (need private key or SPCS OAuth token)")

        try:
            self._conn = snowflake.connector.connect(**opts)
        except Exception as e:
            raise SnowflakeConnectionError(f"Connection failed: {e}") from e

    def query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        self._ensure_connected()
        cur = self._conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params or [])
            return cur.fetchall()
        finally:
            cur.close()

    def query_one(self, sql: str, params: Optional[list] = None) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Optional[list] = None) -> None:
        self._ensure_connected()
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or [])
        finally:
            cur.close()

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            self._conn = None
