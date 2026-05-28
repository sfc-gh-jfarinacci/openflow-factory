from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from ingestion_engine._snowflake_conn import SnowflakeConn
from ingestion_engine.config import EngineConfig

logger = logging.getLogger(__name__)


@dataclass
class NetworkRuleInfo:
    name: str
    host: str
    port: Optional[int] = None


@dataclass
class EAIInfo:
    name: str
    network_rules: list[str]
    secrets: list[str]


class Access:
    def __init__(self, config: EngineConfig):
        self._sf = SnowflakeConn(config)

    def create_network_rule(
        self,
        name: str,
        host: str,
        port: Optional[int] = None,
        *,
        schema: str = "OPENFLOW_FACTORY.METADATA",
    ) -> NetworkRuleInfo:
        fqn = f"{schema}.{name}"
        port_list = str(port) if port else "443"
        self._sf.execute(
            f"CREATE OR REPLACE NETWORK RULE {fqn}\n"
            f"  MODE = EGRESS\n"
            f"  TYPE = HOST_PORT\n"
            f"  VALUE_LIST = ('{host}:{port_list}')"
        )
        logger.info("Created network rule %s → %s:%s", fqn, host, port_list)
        return NetworkRuleInfo(name=fqn, host=host, port=port)

    def create_external_access_integration(
        self,
        name: str,
        network_rules: list[str],
        secrets: list[str],
        *,
        enabled: bool = True,
    ) -> EAIInfo:
        nr_list = ", ".join(network_rules)
        secrets_clause = ""
        if secrets:
            secret_pairs = ", ".join(f"'{s}' = {s}" for s in secrets)
            secrets_clause = f"\n  ALLOWED_AUTHENTICATION_SECRETS = ({secret_pairs})"
        self._sf.execute(
            f"CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {name}\n"
            f"  ALLOWED_NETWORK_RULES = ({nr_list}){secrets_clause}\n"
            f"  ENABLED = {str(enabled).upper()}"
        )
        logger.info("Created EAI %s with rules=%s secrets=%s", name, network_rules, secrets)
        return EAIInfo(name=name, network_rules=network_rules, secrets=secrets)

    def attach_to_runtime(
        self,
        runtime_name: str,
        eai_names: list[str],
        secret_fqns: list[str],
    ) -> None:
        if eai_names:
            eai_list = ", ".join(eai_names)
            self._sf.execute(
                f"ALTER OPENFLOW RUNTIME INTEGRATION {runtime_name}\n"
                f"  SET EXTERNAL_ACCESS_INTEGRATIONS = ({eai_list})"
            )
            logger.info("Attached EAIs %s to runtime %s", eai_list, runtime_name)

        if secret_fqns:
            secrets_list = ", ".join(secret_fqns)
            self._sf.execute(
                f"ALTER OPENFLOW RUNTIME INTEGRATION {runtime_name}\n"
                f"  SET ALLOWED_AUTHENTICATION_SECRETS = ({secrets_list})"
            )
            logger.info("Attached secrets %s to runtime %s", secrets_list, runtime_name)

    def close(self):
        self._sf.close()
