from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

from ingestion_engine._snowflake_conn import SnowflakeConn
from ingestion_engine.config import EngineConfig

logger = logging.getLogger(__name__)


@dataclass
class SecretInfo:
    fqn: str
    connector_id: str
    exists: bool = False


class Secrets:
    def __init__(self, config: EngineConfig):
        self._sf = SnowflakeConn(config)
        self._schema = "OPENFLOW_FACTORY.SECRETS"

    def create(
        self,
        connector_id: str,
        credentials: dict[str, str],
        *,
        comment: Optional[str] = None,
    ) -> SecretInfo:
        fqn = self._fqn(connector_id)
        secret_json = json.dumps(credentials)
        escaped = secret_json.replace("'", "''")
        sql = (
            f"CREATE OR REPLACE SECRET {fqn}\n"
            f"  TYPE = GENERIC_STRING\n"
            f"  SECRET_STRING = '{escaped}'"
        )
        if comment:
            sql += f"\n  COMMENT = '{comment}'"
        self._sf.execute(sql)
        logger.info("Created secret %s", fqn)
        return SecretInfo(fqn=fqn, connector_id=connector_id, exists=True)

    def read(self, connector_id: str) -> Optional[dict]:
        fqn = self._fqn(connector_id)
        try:
            rows = self._sf.query(f"DESCRIBE SECRET {fqn}")
            if rows:
                return {"fqn": fqn, "type": rows[0].get("property_value", "GENERIC_STRING")}
        except Exception:
            pass
        return None

    def delete(self, connector_id: str) -> None:
        fqn = self._fqn(connector_id)
        self._sf.execute(f"DROP SECRET IF EXISTS {fqn}")
        logger.info("Dropped secret %s", fqn)

    def exists(self, connector_id: str) -> bool:
        fqn = self._fqn(connector_id)
        try:
            rows = self._sf.query(f"DESCRIBE SECRET {fqn}")
            return len(rows) > 0
        except Exception:
            return False

    def fqn(self, connector_id: str) -> str:
        return self._fqn(connector_id)

    def _fqn(self, connector_id: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", connector_id).upper()
        if not sanitized[0:1].isalpha() and sanitized[0:1] != "_":
            sanitized = f"C_{sanitized}"
        return f"{self._schema}.{sanitized}_CREDS"

    def close(self):
        self._sf.close()
