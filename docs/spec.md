# Openflow as Code — Specification

## 1. Summary

An ingestion platform where every pipeline is described by a versioned YAML contract in git, materialized by a backend-agnostic Python engine, executed against parameterized flow templates on OpenFlow runtimes, and audited in Snowflake. Engineers add tables by opening a pull request.

---

## 2. System components

| Component | Role | Owned by |
|---|---|---|
| `data-contracts/` | Source of truth. YAML contracts declaring every pipeline. | Domain teams |
| `ingestion-engine/` | Python SDK/CLI that reads contracts, renders templates, deploys to OpenFlow. | Platform team |
| `templates/` (inside engine) | Parameterized NiFi flow exports, one per (source_sgdb, type). | Platform team |
| Orchestrator (Airflow) | Calls the engine on merge + on cron. Consumer, not part of the engine. | Platform team |

```
data-contracts (git)
       │  PR merge
       ▼
   Orchestrator (Airflow / CI)
       │  engine.deploy_from_contract(path, sha)
       ▼
   ingestion-engine (Python)
       │  selector → loader → renderer → deployer
       ▼
   OpenFlow / NiFi runtime
       │  JDBC → Parquet → COPY
       ▼
   Snowflake (BRONZE + CONTROL)
```

---

## 3. Data contract (v1 schema)

### 3.1 Example

```yaml
version: v1
source_sgdb: postgres
type: full
cron: "0 2 * * *"

source_config:
  host: your-source-host.example.com
  port: 5432
  database: ecommerce
  schema: dbo
  tables:
    - name: cliente
      pii_columns: [cpf, email, nome, sobrenome]
    - name: endereco
      pii_columns: [rua, bairro, numero]
    - name: pedido
      pii_columns: []
```

### 3.2 Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `version` | `v1` | yes | Contract schema version |
| `source_sgdb` | enum | yes | postgres, mysql, sqlserver, mongodb, kafka, s3 |
| `type` | enum | yes | full, cdc, incremental |
| `cron` | string | yes (full/incremental) | Quartz cron, ignored for cdc |
| `source_config.host` | string | yes | Source host (NLB or direct endpoint) |
| `source_config.port` | int | no | Default per sgdb (5432 for postgres) |
| `source_config.database` | string | yes | Source DB name |
| `source_config.schema` | string | yes | Default source schema |
| `source_config.tables[].name` | string | yes | Table name |
| `source_config.tables[].pii_columns` | array | yes | Columns carrying PII (drives masking) |
| `source_config.tables[].partition_column` | string | no | For chunked reads |
| `source_config.tables[].chunk_rows` | int | no | Default 500,000 |

### 3.3 Naming conventions

| Element | Convention |
|---|---|
| Domain folder | lowercase (`fraud`, `marketing`) |
| Database folder | matches source DB name |
| YAML file | `<sgdb>_<strategy>[_<variant>].yaml` |
| Runtime name | `<domain>_<filename_stem>` (derived, deterministic) |

### 3.4 What is NOT in the contract

- Connection credentials (sensitive params configured manually in OpenFlow UI after deploy; secret reference mechanism TBD)
- Snowflake destination (derived deterministically)
- Concurrency tuning (runtime-level)
- Promote mode (always SWAP for full; not configurable)

---

## 4. Landing convention in Snowflake

```
target_database = <DOMAIN>  (uppercase)
target_schema   = BRONZE
target_table    = <SOURCE_SGDB>_<SOURCE_SCHEMA>_<SOURCE_TABLE>  (uppercase)
```

Example: `fraud/ecommerce/postgres_full.yaml` → table `cliente` lands at `FRAUD.BRONZE.ECOMMERCE_DBO_CLIENTE`

### Promote strategy by type

| Type | Load target | Promote | Idempotency |
|---|---|---|---|
| full | `…__LOAD` transient | SWAP + DROP | Whole-table replace |
| incremental | live table | MERGE ON pk | Watermark column |
| cdc | live table | Append (_OP, _AT cols) | Source LSN |

All columns land as TEXT in BRONZE. Silver/gold models handle typing.

---

## 5. Engine API

```
ingestion-engine/
├── runtime/
│   ├── list()                    — Lists available OpenFlow runtimes
│   ├── resolve()                 — Resolves runtime name → NiFi client
│   ├── validate_secrets()        — Checks secrets exist in secret manager
│   └── validate_dependencies()   — Checks integrations, stages, reachability
├── flow/
│   ├── deploy_from_template()    — Instantiates a flow from a template
│   ├── validate_template()       — Checks template exists and is compatible
│   ├── trigger()                 — Triggers execution of a deployed flow
│   └── status()                  — Returns flow state (running/stopped/invalid)
├── target/
│   ├── validate_exists()         — Checks destination db/schema/table exist
│   └── validate_permissions()    — Checks runtime has write permission
└── deployer/
    ├── validate()                — Runs all validations
    ├── deploy_from_contract()    — Contract-driven deploy (idempotent, replace on change)
    ├── healthcheck()             — Checks flow is active and reachable
    └── run()                     — Healthcheck + wait + trigger
```

Note: `runtime.create()`, `runtime.start()`, `runtime.stop()`, `runtime.delete()` are stubbed but not yet available (runtime management is manual via OpenFlow UI).

---

## 6. Template system

Each template lives at `ingestion_engine/templates/<sgdb>_<strategy>/v1/`:

```
postgres_full/v1/
├── flow.json        # NiFi flow export (processors, connections, parameter refs)
└── manifest.yaml    # param_mapping: contract YAML paths → NiFi parameter names
```

**`flow.json`** — The NiFi flow definition with `#{param_name}` references instead of hardcoded values. Identical for every contract using this template.

**`manifest.yaml`** — Declares the `parameter_context` name and a `param_mapping` array that maps contract YAML paths to template parameter names:

```yaml
parameter_context: "Postgres Full Params"
param_mapping:
  - param: "Postgres Host"
    source: "source_config.host"
  - param: "Tables To Fetch"
    source: "_tables_json"
    computed: true
  - param: "Destination Database"
    source: "_domain"
    transform: "upper"
```

The renderer is generic — it reads any manifest and resolves values without template-specific code.

Supporting files:
- `STABLE.yaml` — declares current stable version per template
- `selector.py` — maps (source_sgdb, type) → template_id
- `loader.py` — reads manifest + flow.json from versioned dir
- `renderer.py` — generic function: contract + manifest → NiFi parameter context

Templates are immutable once tagged. New work goes in `v2/`.

---

## 7. Governance

### Source of truth

| Concern | Source of truth | Mutation path |
|---|---|---|
| What gets ingested | `data-contracts` repo | Pull request |
| How (flow shape) | `templates/` | Pull request |
| Where it lands | Naming function | Cannot be mutated |
| Schedule | YAML `cron:` field | Pull request |
| Run history | Snowflake CONTROL schema | Append-only by engine |

### Non-negotiable rules

1. No flow logic outside templates
2. No spec outside `data-contracts`
3. Naming is a function, not a config
4. Templates are immutable once tagged
5. The engine is idempotent (same contract + same sha = no-op)

---

## 8. Snowflake objects (per domain)

```sql
CREATE DATABASE <DOMAIN>;
CREATE SCHEMA <DOMAIN>.BRONZE;
CREATE SCHEMA <DOMAIN>.CONTROL;

-- Run tracking
CREATE TABLE <DOMAIN>.CONTROL.OPENFLOW_RUNS (
    run_id STRING PRIMARY KEY, contract_path STRING, contract_sha STRING,
    source_sgdb STRING, strategy STRING, started_at TIMESTAMP_NTZ,
    ended_at TIMESTAMP_NTZ, table_count NUMBER, success_count NUMBER,
    fail_count NUMBER, status STRING
);

-- Per-table audit
CREATE TABLE <DOMAIN>.CONTROL.OPENFLOW_LOAD_AUDIT (
    run_id STRING, contract_path STRING, source_table_fqn STRING,
    destination_fqn STRING, started_at TIMESTAMP_NTZ, ended_at TIMESTAMP_NTZ,
    loaded_row_count NUMBER, status STRING, error_message STRING
);

-- PII masking
CREATE MASKING POLICY <DOMAIN>.CONTROL.MASK_PII_STRING AS (val STRING)
RETURNS STRING -> CASE WHEN CURRENT_ROLE() IN ('<DOMAIN>_PII_READER') THEN val ELSE SHA2(val,256) END;
```

---

## 9. Current status

| Area | Status |
|---|---|
| Contract schema (v1) | Done |
| Template manifest (param_mapping) | Done |
| Renderer (generic, manifest-driven) | Done |
| First template (postgres_full/v1) | Done — deployed and tested |
| Engine CLI: deploy-contract | Done — idempotent, replace-on-change |
| Deployment log (Snowflake) | Done |
| Runtime management | Manual (OpenFlow UI) |
| Secrets in flows | Manual (configure after deploy) |
| Airflow integration | Not started |
| Additional templates (sqlserver, cdc, incremental) | Stubs only |
| Testing | Not started |

---

## 10. Open questions

1. Branch model — `main`-only or `prod/staging/dev` long-lived?
2. Multi-environment — separate accounts or `_DEV` databases?
3. CDC enablement — DBA team or self-service runbook?
4. First-run bootstrap — `RENAME __LOAD` or `CREATE OR REPLACE`?
5. Failure isolation — PARTIAL on first failure or abort?
6. Orchestration — flow cron until runtime-control PrPR, then Airflow?
7. Templates repo — inside engine or split for independent release?
8. Backend protocol — define now or wait for second backend?

---

## 11. Definition of done

A new ingestion is added by:
1. Opening one PR with one YAML file
2. Reviewing it
3. Merging it

No tickets. No NiFi UI. No Snowflake DDL. No Airflow code. No exceptions.
