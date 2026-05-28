from __future__ import annotations

import json
from typing import Optional

from ingestion_engine.config import EngineConfig


def render(
    contract: dict,
    manifest: dict,
    *,
    secret_fqn: Optional[str] = None,
    config: Optional[EngineConfig] = None,
) -> dict:
    context_name = manifest.get("parameter_context", "default")
    mappings = manifest.get("param_mapping", [])

    enriched = _enrich_contract(contract, config)

    params: dict[str, str] = {}
    for m in mappings:
        param_name = m["param"]
        sensitive = m.get("sensitive", False)

        if m.get("computed"):
            value = str(enriched.get(m.get("source", ""), m.get("default", "")))
        elif "source" in m:
            value = _resolve_path(enriched, m["source"], m.get("default", ""))
        else:
            value = m.get("default", "")

        transform = m.get("transform")
        if transform == "upper":
            value = str(value).upper()
        elif transform == "lower":
            value = str(value).lower()

        if sensitive and secret_fqn:
            value = f"${{secret('{secret_fqn}', '{param_name}')}}"

        params[param_name] = str(value)

    return {context_name: params}


def _enrich_contract(contract: dict, config: Optional[EngineConfig]) -> dict:
    enriched = dict(contract)

    source_config = contract.get("source_config", {})
    tables = source_config.get("tables", [])
    schema = source_config.get("schema", "")
    database = source_config.get("database", "")

    enriched["_tables_json"] = json.dumps([
        {
            "database": database,
            "schema": t.get("schema", schema),
            "table_name": t["name"],
            "pii_columns": t.get("pii_columns", []),
        }
        for t in tables
    ], indent=2)

    if config:
        enriched["config.snowflake_warehouse"] = config.snowflake_warehouse
        enriched["config.snowflake_account"] = config.snowflake_account
        enriched["config.snowflake_user"] = config.snowflake_user

    return enriched


def _resolve_path(data: dict, path: str, default: str = "") -> str:
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return default
        if current is None:
            return default
    return current if current is not None else default


def derive_target_fqn(domain: str, source_sgdb: str, source_schema: str, source_table: str) -> str:
    return f"{domain.upper()}.BRONZE.{source_sgdb.upper()}_{source_schema.upper()}_{source_table.upper()}"


def derive_runtime_name(domain: str, filename_stem: str) -> str:
    return f"{domain}_{filename_stem}"
