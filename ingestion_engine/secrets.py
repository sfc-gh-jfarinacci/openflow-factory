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

    def create_named(
        self,
        fqn: str,
        value: str,
        *,
        comment: Optional[str] = None,
    ) -> SecretInfo:
        parts = fqn.replace('"', '').split(".")
        if len(parts) >= 2:
            db_schema = f"{parts[0]}.{parts[1]}"
            self._sf.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")

        escaped = value.replace("'", "''")
        sql = (
            f"CREATE OR REPLACE SECRET {fqn}\n"
            f"  TYPE = GENERIC_STRING\n"
            f"  SECRET_STRING = '{escaped}'"
        )
        if comment:
            sql += f"\n  COMMENT = '{comment}'"
        self._sf.execute(sql)
        logger.info("Created secret %s", fqn)
        return SecretInfo(fqn=fqn, connector_id="", exists=True)

    def create_from_contract(
        self,
        contract: dict,
        values: dict[str, str],
    ) -> list[SecretInfo]:
        secrets_spec = contract.get("secrets", {})
        schema = secrets_spec.get("schema")
        if not schema:
            return []

        results = []
        for param_name, secret_value in values.items():
            fqn = f'{schema}."{param_name}"'
            info = self.create_named(fqn, secret_value, comment=f"Parameter provider secret: {param_name}")
            results.append(info)
        return results

    def read(self, connector_id: str) -> Optional[dict]:
        fqn = self._fqn(connector_id)
        return self.read_by_fqn(fqn)

    def read_by_fqn(self, fqn: str) -> Optional[dict]:
        try:
            rows = self._sf.query(f"DESCRIBE SECRET {fqn}")
            if rows:
                return {"fqn": fqn, "type": rows[0].get("property_value", "GENERIC_STRING")}
        except Exception:
            pass
        return None

    def delete(self, connector_id: str) -> None:
        fqn = self._fqn(connector_id)
        self.delete_by_fqn(fqn)

    def delete_by_fqn(self, fqn: str) -> None:
        self._sf.execute(f"DROP SECRET IF EXISTS {fqn}")
        logger.info("Dropped secret %s", fqn)

    def exists(self, connector_id: str) -> bool:
        fqn = self._fqn(connector_id)
        return self.exists_by_fqn(fqn)

    def exists_by_fqn(self, fqn: str) -> bool:
        try:
            rows = self._sf.query(f"DESCRIBE SECRET {fqn}")
            return len(rows) > 0
        except Exception:
            return False

    def list_secrets(self, database: Optional[str] = None, schema: Optional[str] = None) -> list[dict]:
        if database and schema:
            sql = f"SHOW SECRETS IN SCHEMA {database}.{schema}"
        elif database:
            sql = f"SHOW SECRETS IN DATABASE {database}"
        else:
            sql = f"SHOW SECRETS IN SCHEMA {self._schema}"
        try:
            rows = self._sf.query(sql)
            return [
                {
                    "name": r.get("name"),
                    "schema_name": r.get("schema_name"),
                    "database_name": r.get("database_name"),
                    "secret_type": r.get("secret_type"),
                    "fqn": f"{r.get('database_name')}.{r.get('schema_name')}.{r.get('name')}",
                }
                for r in rows
            ]
        except Exception:
            return []

    def fqn(self, connector_id: str) -> str:
        return self._fqn(connector_id)

    def _fqn(self, connector_id: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", connector_id).upper()
        if not sanitized[0:1].isalpha() and sanitized[0:1] != "_":
            sanitized = f"C_{sanitized}"
        return f"{self._schema}.{sanitized}_CREDS"

    def close(self):
        self._sf.close()
