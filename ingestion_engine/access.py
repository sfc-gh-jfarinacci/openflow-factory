from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from ingestion_engine._snowflake_conn import SnowflakeConn
from ingestion_engine.config import EngineConfig

logger = logging.getLogger(__name__)

RULES_SCHEMA = "OPENFLOW_FACTORY.RULES"


@dataclass
class NetworkRuleInfo:
    fqn: str
    mode: str = "EGRESS"
    rule_type: str = "PRIVATE_HOST_PORT"
    values: list[str] = field(default_factory=list)


@dataclass
class EAIInfo:
    name: str
    network_rules: list[str] = field(default_factory=list)
    enabled: bool = True


class Access:
    def __init__(self, config: EngineConfig):
        self._sf = SnowflakeConn(config)

    def create_network_rule(
        self,
        name: str,
        values: list[str],
        *,
        mode: str = "EGRESS",
        rule_type: str = "PRIVATE_HOST_PORT",
        schema: str = RULES_SCHEMA,
    ) -> NetworkRuleInfo:
        fqn = f"{schema}.{name}" if "." not in name else name
        value_list = ", ".join(f"'{v}'" for v in values)
        self._sf.execute(
            f"CREATE OR REPLACE NETWORK RULE {fqn}\n"
            f"  MODE = {mode}\n"
            f"  TYPE = {rule_type}\n"
            f"  VALUE_LIST = ({value_list})"
        )
        logger.info("Created network rule %s", fqn)
        return NetworkRuleInfo(fqn=fqn, mode=mode, rule_type=rule_type, values=values)

    def alter_network_rule_add(self, name: str, values: list[str], *, schema: str = RULES_SCHEMA) -> NetworkRuleInfo:
        fqn = f"{schema}.{name}" if "." not in name else name
        existing = self._get_network_rule_values(fqn)
        merged = list(dict.fromkeys(existing + values))
        value_list = ", ".join(f"'{v}'" for v in merged)
        self._sf.execute(
            f"ALTER NETWORK RULE {fqn} SET VALUE_LIST = ({value_list})"
        )
        logger.info("Added %s to network rule %s (total: %d)", values, fqn, len(merged))
        return NetworkRuleInfo(fqn=fqn, values=merged)

    def alter_network_rule_remove(self, name: str, values: list[str], *, schema: str = RULES_SCHEMA) -> NetworkRuleInfo:
        fqn = f"{schema}.{name}" if "." not in name else name
        existing = self._get_network_rule_values(fqn)
        remaining = [v for v in existing if v not in values]
        if not remaining:
            logger.warning("Removing all values from %s would leave it empty — keeping last value", fqn)
            remaining = existing[:1]
        value_list = ", ".join(f"'{v}'" for v in remaining)
        self._sf.execute(
            f"ALTER NETWORK RULE {fqn} SET VALUE_LIST = ({value_list})"
        )
        logger.info("Removed %s from network rule %s (remaining: %d)", values, fqn, len(remaining))
        return NetworkRuleInfo(fqn=fqn, values=remaining)

    def delete_network_rule(self, name: str, *, schema: str = RULES_SCHEMA) -> None:
        fqn = f"{schema}.{name}" if "." not in name else name
        eais = self._find_eais_using_rule(fqn)
        for eai_name in eais:
            self._remove_rule_from_eai(eai_name, fqn)
            logger.info("Removed %s from EAI %s", fqn, eai_name)
        self._sf.execute(f"DROP NETWORK RULE IF EXISTS {fqn}")
        logger.info("Deleted network rule %s", fqn)

    def list_network_rules(self, schema: str = RULES_SCHEMA) -> list[NetworkRuleInfo]:
        rows = self._sf.query(f"SHOW NETWORK RULES IN SCHEMA {schema}")
        results = []
        for r in rows:
            fqn = f"{r.get('database_name', '')}.{r.get('schema_name', '')}.{r.get('name', '')}"
            results.append(NetworkRuleInfo(
                fqn=fqn,
                mode=r.get("mode", ""),
                rule_type=r.get("type", ""),
            ))
        return results

    def get_network_rule(self, name: str, *, schema: str = RULES_SCHEMA) -> NetworkRuleInfo:
        fqn = f"{schema}.{name}" if "." not in name else name
        values = self._get_network_rule_values(fqn)
        rows = self._sf.query(f"DESCRIBE NETWORK RULE {fqn}")
        mode = ""
        rule_type = ""
        for r in rows:
            prop = r.get("property", "")
            if prop == "MODE":
                mode = r.get("value", "")
            elif prop == "TYPE":
                rule_type = r.get("value", "")
        return NetworkRuleInfo(fqn=fqn, mode=mode, rule_type=rule_type, values=values)

    def create_eai(
        self,
        name: str,
        network_rules: list[str],
        *,
        enabled: bool = True,
        schema: str = RULES_SCHEMA,
    ) -> EAIInfo:
        rule_fqns = [f"{schema}.{r}" if "." not in r else r for r in network_rules]
        rule_list = ", ".join(rule_fqns)
        self._sf.execute(
            f"CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {name}\n"
            f"  ALLOWED_NETWORK_RULES = ({rule_list})\n"
            f"  ENABLED = {str(enabled).upper()}"
        )
        logger.info("Created EAI %s with rules %s", name, rule_fqns)
        return EAIInfo(name=name, network_rules=rule_fqns, enabled=enabled)

    def alter_eai_add_rules(self, name: str, rules: list[str], *, schema: str = RULES_SCHEMA) -> EAIInfo:
        rule_fqns = [f"{schema}.{r}" if "." not in r else r for r in rules]
        existing = self._get_eai_rules(name)
        merged = list(dict.fromkeys(existing + rule_fqns))
        rule_list = ", ".join(merged)
        self._sf.execute(
            f"ALTER EXTERNAL ACCESS INTEGRATION {name}\n"
            f"  SET ALLOWED_NETWORK_RULES = ({rule_list})"
        )
        logger.info("Added rules %s to EAI %s (total: %d)", rule_fqns, name, len(merged))
        return EAIInfo(name=name, network_rules=merged)

    def alter_eai_remove_rules(self, name: str, rules: list[str], *, schema: str = RULES_SCHEMA) -> EAIInfo:
        rule_fqns = [f"{schema}.{r}" if "." not in r else r for r in rules]
        existing = self._get_eai_rules(name)
        remaining = [r for r in existing if r not in rule_fqns]
        if not remaining:
            logger.warning("Cannot remove all rules from EAI %s — at least one required", name)
            remaining = existing[:1]
        rule_list = ", ".join(remaining)
        self._sf.execute(
            f"ALTER EXTERNAL ACCESS INTEGRATION {name}\n"
            f"  SET ALLOWED_NETWORK_RULES = ({rule_list})"
        )
        logger.info("Removed rules %s from EAI %s (remaining: %d)", rule_fqns, name, len(remaining))
        return EAIInfo(name=name, network_rules=remaining)

    def delete_eai(self, name: str) -> None:
        self._sf.execute(f"DROP INTEGRATION IF EXISTS {name}")
        logger.info("Deleted EAI %s", name)

    def list_eais(self) -> list[EAIInfo]:
        rows = self._sf.query("SHOW EXTERNAL ACCESS INTEGRATIONS")
        results = []
        for r in rows:
            eai_name = r.get("name", "")
            enabled = r.get("enabled", "true") == "true"
            rules = self._get_eai_rules(eai_name)
            results.append(EAIInfo(name=eai_name, network_rules=rules, enabled=enabled))
        return results

    def get_eai(self, name: str) -> EAIInfo:
        rules = self._get_eai_rules(name)
        rows = self._sf.query(f"DESCRIBE INTEGRATION {name}")
        enabled = True
        for r in rows:
            if r.get("property", "") == "ENABLED":
                enabled = r.get("property_value", "true") == "true"
        return EAIInfo(name=name, network_rules=rules, enabled=enabled)

    def attach_to_runtime(self, runtime_name: str, eai_names: list[str]) -> None:
        eai_list = ", ".join(eai_names)
        self._sf.execute(
            f"ALTER OPENFLOW RUNTIME INTEGRATION {runtime_name}\n"
            f"  SET EXTERNAL_ACCESS_INTEGRATIONS = ({eai_list})"
        )
        logger.info("Attached EAIs %s to runtime %s", eai_list, runtime_name)

    def _get_network_rule_values(self, fqn: str) -> list[str]:
        rows = self._sf.query(f"DESCRIBE NETWORK RULE {fqn}")
        for r in rows:
            if r.get("property", "") == "VALUE_LIST":
                raw = r.get("value", "")
                return [v.strip().strip("'") for v in raw.split(",") if v.strip()]
        return []

    def _get_eai_rules(self, name: str) -> list[str]:
        rows = self._sf.query(f"DESCRIBE INTEGRATION {name}")
        for r in rows:
            if "ALLOWED_NETWORK_RULES" in r.get("property", "").upper():
                raw = r.get("property_value", "")
                return [v.strip() for v in raw.strip("[]").split(",") if v.strip()]
        return []

    def _find_eais_using_rule(self, rule_fqn: str) -> list[str]:
        eais = self.list_eais()
        result = []
        for eai in eais:
            for r in eai.network_rules:
                if r.upper() == rule_fqn.upper():
                    result.append(eai.name)
                    break
        return result

    def _remove_rule_from_eai(self, eai_name: str, rule_fqn: str) -> None:
        existing = self._get_eai_rules(eai_name)
        remaining = [r for r in existing if r.upper() != rule_fqn.upper()]
        if not remaining:
            self._sf.execute(f"DROP INTEGRATION IF EXISTS {eai_name}")
            logger.info("EAI %s had no remaining rules — deleted", eai_name)
        else:
            rule_list = ", ".join(remaining)
            self._sf.execute(
                f"ALTER EXTERNAL ACCESS INTEGRATION {eai_name}\n"
                f"  SET ALLOWED_NETWORK_RULES = ({rule_list})"
            )

    def close(self):
        self._sf.close()
