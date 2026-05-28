from __future__ import annotations

from typing import Optional

from ingestion_engine.templates.loader import load_template


SGDB_STRATEGY_MAP = {
    ("postgres", "full"): "postgres_full",
    ("postgres", "cdc"): "postgres_cdc",
    ("postgres", "incremental"): "postgres_incremental",
    ("mysql", "full"): "mysql_full",
    ("mysql", "cdc"): "mysql_cdc",
    ("sqlserver", "full"): "sqlserver_full",
    ("sqlserver", "cdc"): "sqlserver_cdc",
    ("sqlserver", "incremental"): "sqlserver_incremental",
    ("mongodb", "cdc"): "mongodb_cdc",
}


def select_template(source_sgdb: str, strategy: str, version: Optional[str] = None) -> dict:
    template_id = SGDB_STRATEGY_MAP.get((source_sgdb, strategy))
    if not template_id:
        raise ValueError(f"No template registered for ({source_sgdb}, {strategy})")
    return load_template(template_id, version)
