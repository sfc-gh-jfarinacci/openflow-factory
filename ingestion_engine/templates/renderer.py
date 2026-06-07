from __future__ import annotations

import json
from typing import Optional

from ingestion_engine.config import EngineConfig


def render(
    contract: dict,
    manifest: dict,
    *,
    secret_fqn: Optional[str] = None,
    secret_values: Optional[dict[str, str]] = None,
    config: Optional[EngineConfig] = None,
) -> dict:
    context_name = manifest.get("parameter_context", "default")
    mappings = manifest.get("param_mapping", [])

    secrets_map = contract.get("secrets", {})

    enriched = _enrich_contract(contract, config)

    params: dict[str, str] = {}
    for m in mappings:
        param_name = m["param"]
        sensitive = m.get("sensitive", False)

        if m.get("type") == "asset":
            continue

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

        if sensitive and param_name in secrets_map:
            if secret_values and param_name in secret_values:
                value = secret_values[param_name]
            else:
                continue
        elif sensitive and secret_fqn:
            value = f"${{secret('{secret_fqn}', '{param_name}')}}"

        params[param_name] = str(value)

    return {context_name: params}


def _enrich_contract(contract: dict, config: Optional[EngineConfig]) -> dict:
    enriched = dict(contract)

    source_config = contract.get("source_config", {})
    tables = source_config.get("tables", [])
    schema = source_config.get("schema", "")
    database = source_config.get("database", "")

    enriched_tables = []
    for t in tables:
        has_partition = bool(t.get("partition_column"))
        has_watermark = bool(t.get("watermark_columns"))
        chunk = t.get("chunk_rows", 5000)
        if not has_partition and not has_watermark:
            chunk = 0
        enriched_tables.append({
            "database": database,
            "schema": t.get("schema", schema),
            "table_name": t["name"],
            "pii_columns": t.get("pii_columns", []),
            "partition_column": t.get("partition_column"),
            "watermark_columns": t.get("watermark_columns"),
            "chunk_rows": chunk,
            "where_clause": t.get("where_clause"),
            "exclude_columns": t.get("exclude_columns", []),
        })

    enriched["_tables_json"] = json.dumps(enriched_tables, indent=2)

    global_chunk = source_config.get("chunk_rows", 5000)
    has_any_partition = any(t.get("partition_column") or t.get("watermark_columns") for t in tables)
    if not has_any_partition:
        global_chunk = 0
    enriched["chunk_rows"] = str(global_chunk)

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


def derive_runtime_name(domain: str, filename_stem: str, *, subdomain: Optional[str] = None) -> str:
    if subdomain:
        return f"{domain}_{subdomain}_{filename_stem}"
    return f"{domain}_{filename_stem}"
