from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from ingestion_engine._nifi_client import NiFiClient
from ingestion_engine._snowflake_auth import mint_access_token
from ingestion_engine._snowflake_conn import SnowflakeConn
from ingestion_engine.config import EngineConfig
from ingestion_engine.exceptions import RuntimeNotFoundError

logger = logging.getLogger(__name__)

_PRIVATE_PREVIEW_MSG = (
    "This feature requires runtime lifecycle management (CREATE/ALTER/DROP OPENFLOW RUNTIME INTEGRATION) "
    "which is currently in Private Preview. Contact your Snowflake account team for access."
)


@dataclass
class DiscoveredRuntime:
    name: str
    base_uri: str
    enabled: bool = True
    comment: Optional[str] = None
    created: Optional[str] = None


@dataclass
class ValidationResult:
    ok: bool
    checks: list[dict] = field(default_factory=list)


class Runtime:
    def __init__(self, config: EngineConfig):
        self._config = config
        self._sf = SnowflakeConn(config)

    async def create(self, name: str, *, warehouse: str | None = None, comment: str | None = None) -> DiscoveredRuntime:
        logger.warning("runtime.create() — %s", _PRIVATE_PREVIEW_MSG)
        raise NotImplementedError(_PRIVATE_PREVIEW_MSG)

    async def start(self, runtime_name: str) -> None:
        logger.warning("runtime.start() — %s", _PRIVATE_PREVIEW_MSG)
        raise NotImplementedError(_PRIVATE_PREVIEW_MSG)

    async def stop(self, runtime_name: str) -> None:
        logger.warning("runtime.stop() — %s", _PRIVATE_PREVIEW_MSG)
        raise NotImplementedError(_PRIVATE_PREVIEW_MSG)

    async def delete(self, runtime_name: str) -> None:
        logger.warning("runtime.delete() — %s", _PRIVATE_PREVIEW_MSG)
        raise NotImplementedError(_PRIVATE_PREVIEW_MSG)

    async def list(self) -> list[DiscoveredRuntime]:
        rows = self._sf.query(
            "SHOW OPENFLOW RUNTIME INTEGRATIONS ->> SELECT * FROM $1"
        )
        return [
            DiscoveredRuntime(
                name=r.get("name", ""),
                base_uri=_extract_base_uri(r.get("oauth_redirect_uri", "")),
                enabled=r.get("enabled", "true") == "true",
                comment=r.get("comment"),
                created=r.get("created_on"),
            )
            for r in rows
        ]

    async def resolve(self, runtime_name: str) -> tuple[DiscoveredRuntime, NiFiClient]:
        runtimes = await self.list()
        target = runtime_name.strip().upper()
        rt = next((r for r in runtimes if r.name.strip().upper() == target), None)
        if not rt:
            available = [r.name for r in runtimes]
            raise RuntimeNotFoundError(f"Runtime '{runtime_name.strip()}' not found. Available: {available}")
        token = await mint_access_token(self._config, runtime_name)
        client = NiFiClient(base_url=rt.base_uri, auth_token=token)
        return rt, client

    async def validate_secrets(self, connector_id: str) -> ValidationResult:
        checks = []
        fqn = _secret_fqn(connector_id)
        try:
            rows = self._sf.query(f"DESCRIBE SECRET {fqn}")
            if rows:
                checks.append({"check": "secret_exists", "ok": True, "fqn": fqn})
            else:
                checks.append({"check": "secret_exists", "ok": False, "fqn": fqn, "message": f"Secret {fqn} not found"})
        except Exception as e:
            checks.append({"check": "secret_exists", "ok": False, "fqn": fqn, "message": str(e)})

        return ValidationResult(ok=all(c["ok"] for c in checks), checks=checks)

    async def validate_dependencies(self, template: str, runtime_name: str | None = None) -> ValidationResult:
        checks = []

        stage_fqn = self._config.templates_stage
        try:
            rows = self._sf.query(f"LIST @{stage_fqn}")
            stage_files = [r.get("name", "") for r in rows]
            checks.append({"check": "templates_stage", "ok": True, "file_count": len(stage_files)})
        except Exception as e:
            checks.append({"check": "templates_stage", "ok": False, "message": f"Cannot access stage {stage_fqn}: {e}"})

        try:
            rows = self._sf.query("SHOW INTEGRATIONS LIKE '%OPENFLOW%'")
            eai_names = [r.get("name", "") for r in rows]
            has_eai = len(eai_names) > 0
            checks.append({"check": "external_access_integration", "ok": has_eai, "integrations": eai_names})
        except Exception as e:
            checks.append({"check": "external_access_integration", "ok": False, "message": str(e)})

        if runtime_name:
            try:
                _, client = await self.resolve(runtime_name)
                root_id = await client.get_root_pg_id()
                await client.close()
                checks.append({"check": "runtime_reachable", "ok": True, "root_id": root_id})
            except Exception as e:
                checks.append({"check": "runtime_reachable", "ok": False, "message": str(e)})

        return ValidationResult(ok=all(c["ok"] for c in checks), checks=checks)

    def close(self):
        self._sf.close()


def _extract_base_uri(oauth_redirect_uri: str) -> str:
    import re
    return re.sub(r"/login.*$", "", oauth_redirect_uri)


def _secret_fqn(connector_id: str) -> str:
    import re
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", connector_id).upper()
    if not sanitized[0:1].isalpha() and sanitized[0:1] != "_":
        sanitized = f"C_{sanitized}"
    return f"OPENFLOW_FACTORY.SECRETS.{sanitized}_CREDS"
