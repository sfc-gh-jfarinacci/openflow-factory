from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ingestion_engine._snowflake_conn import SnowflakeConn
from ingestion_engine.config import EngineConfig


@dataclass
class TargetValidation:
    ok: bool
    checks: list[dict] = field(default_factory=list)


class Target:
    def __init__(self, config: EngineConfig):
        self._config = config
        self._sf = SnowflakeConn(config)

    def validate_exists(
        self,
        database: str,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> TargetValidation:
        checks = []

        try:
            rows = self._sf.query(f"SHOW DATABASES LIKE '{database}'")
            db_exists = len(rows) > 0
            checks.append({"check": "database_exists", "ok": db_exists, "database": database})
        except Exception as e:
            checks.append({"check": "database_exists", "ok": False, "database": database, "message": str(e)})
            return TargetValidation(ok=False, checks=checks)

        if schema:
            try:
                rows = self._sf.query(f"SHOW SCHEMAS LIKE '{schema}' IN DATABASE {database}")
                schema_exists = len(rows) > 0
                checks.append({"check": "schema_exists", "ok": schema_exists, "schema": f"{database}.{schema}"})
            except Exception as e:
                checks.append({"check": "schema_exists", "ok": False, "schema": f"{database}.{schema}", "message": str(e)})

        if table and schema:
            try:
                rows = self._sf.query(f"SHOW TABLES LIKE '{table}' IN {database}.{schema}")
                table_exists = len(rows) > 0
                checks.append({"check": "table_exists", "ok": table_exists, "table": f"{database}.{schema}.{table}"})
            except Exception as e:
                checks.append({"check": "table_exists", "ok": False, "table": f"{database}.{schema}.{table}", "message": str(e)})

        return TargetValidation(ok=all(c["ok"] for c in checks), checks=checks)

    def validate_permissions(
        self,
        database: str,
        schema: Optional[str] = None,
        role: Optional[str] = None,
    ) -> TargetValidation:
        checks = []
        target_role = role or self._config.snowflake_role

        try:
            rows = self._sf.query(
                f"SHOW GRANTS ON DATABASE {database}"
            )
            usage_grant = any(
                r.get("privilege") == "USAGE" and r.get("grantee_name", "").upper() == target_role.upper()
                for r in rows
            )
            checks.append({
                "check": "database_usage",
                "ok": usage_grant,
                "database": database,
                "role": target_role,
                "message": None if usage_grant else f"Role {target_role} lacks USAGE on {database}",
            })
        except Exception as e:
            checks.append({"check": "database_usage", "ok": False, "message": str(e)})

        if schema:
            try:
                rows = self._sf.query(f"SHOW GRANTS ON SCHEMA {database}.{schema}")
                write_privs = {"CREATE TABLE", "USAGE"}
                granted = {r.get("privilege") for r in rows if r.get("grantee_name", "").upper() == target_role.upper()}
                has_write = write_privs.issubset(granted)
                checks.append({
                    "check": "schema_write",
                    "ok": has_write,
                    "schema": f"{database}.{schema}",
                    "role": target_role,
                    "granted": list(granted),
                    "message": None if has_write else f"Role {target_role} lacks CREATE TABLE or USAGE on {database}.{schema}",
                })
            except Exception as e:
                checks.append({"check": "schema_write", "ok": False, "message": str(e)})

        return TargetValidation(ok=all(c["ok"] for c in checks), checks=checks)

    def close(self):
        self._sf.close()
