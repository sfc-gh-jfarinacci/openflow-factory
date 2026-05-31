# Data Contracts

This directory is the source of truth for all ingestion pipelines.

## Structure

```
<domain>/
  <database>/
    <sgdb>_<strategy>[_<variant>].yaml
```

## Contract Fields

| Field | Required | Description |
|---|---|---|
| `version` | Yes | Schema version (`v1`) |
| `source_sgdb` | Yes | Source database type (`postgres`, `mysql`, `sqlserver`, etc.) |
| `type` | Yes | Ingestion strategy (`full`, `cdc`, `incremental`) |
| `cron` | Yes (full/incremental) | NiFi Quartz cron (6 fields: `sec min hour day month weekday`) |
| `secrets` | No | Maps NiFi param names to Snowflake secret FQNs |
| `assets` | No | Maps NiFi param names to asset filenames in `ingestion_engine/assets/` |
| `source_config` | Yes | Source connection details (host, port, database, schema, tables) |

## Secrets

Sensitive values are never stored in contracts. Instead, reference a Snowflake secret:

```yaml
secrets:
  Postgres Password: OPENFLOW_FACTORY.SECRETS.ECOMMERCE_PASSWORD
```

The engine's SnowflakeParameterProvider fetches the secret value inside SPCS and applies it to the flow's parameter context. Multiple contracts can reference the same secret.

## Assets

Binary dependencies (JDBC drivers, certificates) are referenced by filename:

```yaml
assets:
  Postgres Driver: postgresql-42.7.11.jar
```

Files are resolved from `ingestion_engine/assets/` and uploaded to the flow's parameter context at deploy time.

## Example

The `fraud/ecommerce/postgres_full.yaml` contract is a working reference example. Copy it as a starting point for new pipelines.

## Validation

```bash
ingestion-engine validate-contract fraud/ecommerce/postgres_full.yaml
python scripts/validate_contracts.py   # schema check (all contracts)
python scripts/lint_naming.py          # naming conventions
```
