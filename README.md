# Openflow as Code

Contract-driven ingestion platform for Snowflake OpenFlow. Pipelines are defined as versioned YAML contracts in git, deployed by a backend-agnostic Python engine, executed on OpenFlow runtimes.

---

## Prerequisites

Before using the engine, complete these manual setup steps in order.

### 1. Snowflake bootstrap

Run `ingestion-engine/bootstrap/bootstrap.sql` in Snowflake (as `ACCOUNTADMIN` or equivalent):

```bash
snowsql -f ingestion-engine/bootstrap/bootstrap.sql
```

This creates the `OPENFLOW_FACTORY` database, `METADATA` schema, and `DEPLOYMENT_LOG` table the engine uses for idempotency.



### 2. External Access Integration (EAI)

OpenFlow runtimes need an External Access Integration to reach your source systems outside Snowflake.

```sql
-- Create a network rule allowing outbound to your source system
CREATE OR REPLACE NETWORK RULE ecommerce_pg_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('your-rds-nlb.elb.us-west-2.amazonaws.com:5432');

-- Create the External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ecommerce_pg_eai
  ALLOWED_NETWORK_RULES = (ecommerce_pg_rule)
  ENABLED = TRUE;
```

Assign the EAI to the runtime via the OpenFlow UI (can be done at any time, not just at creation).

### 3. OpenFlow runtime (manual)

Runtimes must be created manually through the Snowflake OpenFlow UI:

1. Navigate to **Data Engineering > OpenFlow** in Snowsight
2. Create a new runtime
3. Assign the External Access Integration from step 2
4. Note the runtime integration name (e.g. `OPENFLOW_RUNTIME_AED416FD_...`)

The engine deploys **flows to existing runtimes only**. It does not create or manage runtime lifecycle as the relevant feature to do so is not avaiable yet.

### 4. Source credentials

Sensitive parameters (database passwords, private keys) are stored as Snowflake secrets and resolved at deploy time via the SnowflakeParameterProvider running inside SPCS. No credentials leave Snowflake.

```sql
-- Create a secret for the source database password
CREATE OR REPLACE SECRET OPENFLOW_FACTORY.SECRETS.ECOMMERCE_PASSWORD
  TYPE = GENERIC_STRING
  SECRET_STRING = '<password>';
```

The contract references the secret by FQN — the engine handles fetch, apply, inheritance, and property remapping automatically.

A SnowflakeParameterProvider must be configured on the runtime (one-time setup via CLI or UI):

```bash
ingestion-engine setup-snowflake-parameter-provider -r test
```

### 5. Service user key pair

The engine authenticates to Snowflake via key-pair auth:

```bash
# Generate key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out openflow_factory_svc.pem -nocrypt
openssl rsa -in openflow_factory_svc.pem -pubout -out openflow_factory_svc.pub

# Assign public key to user in Snowflake
ALTER USER OPENFLOW_FACTORY_SVC SET RSA_PUBLIC_KEY='<contents of .pub file>';
```

---

## Configuration

One `.env` at the repo root:

```
SNOWFLAKE_ACCOUNT=your-account-locator
SNOWFLAKE_USER=OPENFLOW_FACTORY_SVC
SNOWFLAKE_ROLE=OPENFLOW_ADMIN
SNOWFLAKE_WAREHOUSE=OPENFLOW_FACTORY_WH
SNOWFLAKE_DATABASE=OPENFLOW_FACTORY
SNOWFLAKE_SCHEMA=METADATA
SNOWFLAKE_PRIVATE_KEY_PATH=./openflow_factory_svc.pem
```

---

## Install

```bash
cd ingestion-engine
pip install -e ".[cli]"
```

---

## Usage

### Deploy a contract

```bash
ingestion-engine deploy-contract fraud/ecommerce/postgres_full.yaml \
  --sha $(git rev-parse HEAD) \
  -r test \
  --start
```

### Behavior on redeploy

| Scenario | Result |
|---|---|
| Same sha as last deploy | No-op ("already deployed") |
| New sha (contract changed) | Stop old flow → delete → upload new flow with updated params |
| `--force` flag | Always replace, regardless of sha |

### Other commands

```bash
# Validate a contract YAML against schema
ingestion-engine validate-contract fraud/ecommerce/postgres_full.yaml

# List available runtimes
ingestion-engine list-runtimes

# Check flow health
ingestion-engine healthcheck <process_group_id> -r test

# Trigger a run
ingestion-engine run <process_group_id> -r test

# Create secrets for a contract (prompts for values)
ingestion-engine create-contract-secrets fraud/ecommerce/postgres_full.yaml

# List parameter providers on a runtime
ingestion-engine list-parameter-providers -r test

# Setup SnowflakeParameterProvider (one-time per runtime)
ingestion-engine setup-snowflake-parameter-provider -r test
```

---

## How it works

```
data-contracts/fraud/ecommerce/postgres_full.yaml
        │
        ▼
   ┌─────────┐     manifest.yaml      ┌──────────────┐
   │ Selector │────────────────────────▶│   Renderer   │
   │(sgdb+type)│   param_mapping       │(generic,     │
   └─────────┘                         │ manifest-    │
        │                              │ driven)      │
        │ template_id                  └──────┬───────┘
        ▼                                     │ NiFi parameter context
   templates/postgres_full/v1/                │
        │                                     ▼
        │ flow.json              ┌────────────────────┐
        └───────────────────────▶│     Deployer       │
                                 │ (stop old → delete │
                                 │  → upload new)     │
                                 └─────────┬──────────┘
                                           │
                                           ▼
                                 OpenFlow Runtime (NiFi)
                                           │
                                           ▼
                                 Snowflake FRAUD.BRONZE.*
```

1. **Contract** — YAML declares source host, database, tables, PII columns, cron
2. **Selector** — Maps `(source_sgdb, type)` → template directory
3. **Renderer** — Reads manifest's `param_mapping`, resolves contract values into NiFi parameter context
4. **Deployer** — Checks deployment log for idempotency, replaces old flow if changed, uploads new flow
5. **Runtime** — NiFi executes the flow on cron schedule

---

## Repository layout

```
openflow-dag-factory/
├── .env                           # Engine config (Snowflake creds)
├── openflow_factory_svc.pem       # Service user private key (git-ignored)
├── data_contracts/                # Source of truth — YAML contracts
│   ├── schema/contract.v1.schema.json
│   ├── scripts/                   # CI validators
│   └── fraud/ecommerce/postgres_full.yaml
├── ingestion-engine/              # Python SDK + CLI
│   ├── pyproject.toml
│   ├── ingestion_engine/
│   │   ├── runtime/               # list, resolve, validate_secrets, validate_dependencies
│   │   ├── flow/                  # deploy_from_template, validate_template, trigger, status
│   │   ├── target/                # validate_exists, validate_permissions
│   │   ├── deployer/              # deploy_from_contract, healthcheck, run
│   │   └── templates/             # renderer, loader, selector + versioned template dirs
│   │       ├── postgres_full/v1/
│   │       ├── postgres_cdc/v1/
│   │       └── postgres_incremental/v1/
│   ├── bootstrap/                 # Snowflake DDL
│   └── tests/
└── docs/
    ├── project_plan.md
    └── data_contract_strategy.md
```

---

## Adding a new ingestion pipeline

1. Create `data-contracts/<domain>/<database>/<sgdb>_<strategy>.yaml`
2. Open a PR — CI validates schema + naming conventions
3. Merge — deploy via CLI or orchestrator calling `deploy-contract`

---

## Adding a new template

1. Create `ingestion_engine/templates/<sgdb>_<strategy>/v1/`
2. Author `flow.json` (NiFi export) + `manifest.yaml` (param_mapping, secrets, deps)
3. Add a fixture contract in `fixtures/valid_contract.yaml`
4. Update `STABLE.yaml`
5. Any contract with matching `(source_sgdb, type)` will auto-resolve to it

---

## Current limitations

| Area | Status |
|---|---|
| Runtime creation/deletion | Manual via OpenFlow UI |
| Runtime start/stop lifecycle | Manual (API not yet available) |
| External Access Integration | Manual setup per source |
| Secrets management | Resolved via SnowflakeParameterProvider at deploy time (values never leave Snowflake) |
| Parameter-context-only updates | Not supported (NiFi blocks updates while services reference params); engine always does full replace |
| CDC / Incremental templates | Template stubs exist; flow.json not yet authored |

---

## Python API

```python
from ingestion_engine import EngineConfig, Deployer
from pathlib import Path
import asyncio

config = EngineConfig()  # reads .env
deployer = Deployer(config)

# Contract-driven deploy
result = asyncio.run(deployer.deploy_from_contract(
    contract_path="fraud/ecommerce/postgres_full.yaml",
    sha="abc123",
    contracts_dir=Path("./data_contracts"),
    runtime_name_override="test",
    auto_start=True,
))

# Force redeploy
result = asyncio.run(deployer.deploy_from_contract(
    contract_path="fraud/ecommerce/postgres_full.yaml",
    sha="abc123",
    contracts_dir=Path("./data_contracts"),
    runtime_name_override="test",
    force=True,
    auto_start=True,
))

# Operations
health = asyncio.run(deployer.healthcheck(result.flow.process_group_id))
run_result = asyncio.run(deployer.run(result.flow.process_group_id))

deployer.close()
```

---

## Concepts

### Data contracts

A data contract is a YAML file that fully describes one ingestion pipeline. It lives in `data-contracts/<domain>/<database>/<sgdb>_<strategy>.yaml` and declares:

- **What** to ingest (source host, database, schema, tables)
- **How** to ingest it (strategy: `full`, `incremental`, or `cdc`)
- **When** to run (cron schedule)
- **What's sensitive** (PII columns for downstream masking)

```yaml
version: v1
source_sgdb: postgres
type: full
cron: "0 */5 * * * ?"

secrets:
  Postgres Password: OPENFLOW_FACTORY.SECRETS.ECOMMERCE_PASSWORD

assets:
  Postgres Driver: postgresql-42.7.11.jar

source_config:
  host: my-nlb.elb.us-west-2.amazonaws.com
  port: 5432
  database: ecommerce
  schema: dbo
  username: postgres
  tables:
    - name: cliente
      pii_columns: [cpf, email, nome, sobrenome]
    - name: endereco
      pii_columns: [rua, bairro, numero]
    - name: pedido
      pii_columns: []
```

The contract is the **only** thing an engineer writes to add a new pipeline. Everything else (template selection, parameter rendering, destination naming, deployment) is derived automatically.

The directory structure encodes ownership:
- `<domain>/` folder = business domain (drives RBAC, cost attribution)
- `<database>/` subfolder = source system database
- Filename `<sgdb>_<strategy>.yaml` determines which template is used

### Templates

A template is a versioned, parameterized NiFi flow that implements one `(source_sgdb, type)` combination. Templates live in `ingestion_engine/templates/<sgdb>_<strategy>/v1/` and contain two files:

**`flow.json`** — A NiFi flow definition exported from the OpenFlow UI. Contains the actual processor graph (JDBC fetch, convert to Parquet, stage, COPY INTO, SWAP, etc.) with parameter references (`#{Postgres Host}`, `#{Tables To Fetch}`) instead of hardcoded values. This is the "shape" of the pipeline — identical for every contract that uses this template.

**`manifest.yaml`** — Declares how contract YAML fields map to the template's NiFi parameters. The engine's renderer reads this mapping and produces the correct parameter context without any template-specific code:

```yaml
template_id: postgres_full
version: v1
parameter_context: "Postgres Full Params"

param_mapping:
  - param: "Postgres Host"
    source: "source_config.host"
  - param: "Postgres Port"
    source: "source_config.port"
    default: "5432"
  - param: "Tables To Fetch"
    source: "_tables_json"
    computed: true
  - param: "Destination Database"
    source: "_domain"
    transform: "upper"
  # ...
```

This separation means:
- Adding a table to a pipeline = edit the contract YAML (no flow knowledge needed)
- Adding support for a new source system = author a new template (no engine code changes)
- The renderer is generic — it works for any template by reading its manifest

### Why two files per template?

| File | Purpose | Changes when... |
|---|---|---|
| `flow.json` | NiFi processor graph (the "how") | Flow logic changes (new processor, different routing) |
| `manifest.yaml` | Contract-to-parameter mapping (the "glue") | New parameters added, mapping rules change |

Without the manifest, the renderer would need per-template code. With it, the renderer is a single generic function that works for `postgres_full`, `sqlserver_full`, `postgres_cdc`, or any future template.

---

## Contributing

### Roadmap

- ~~Secrets integration~~ — **Done.** SnowflakeParameterProvider fetches secrets from `OPENFLOW_FACTORY.SECRETS`, applies to a provider-owned context, inherited into the flow's param context, service properties remapped automatically
- **Runtime management** — when available, add support for runtime creation, deletion, start/stop to enable full orchestrator-driven lifecycle
- **Testing** — introduce unit tests for renderer/selector, integration tests against ephemeral runtimes
- **More templates** — author `flow.json` for `sqlserver_full`, `postgres_cdc`, `postgres_incremental`, `mysql_full`, etc.

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.

Copyright 2026 Jorge Farinacci



## APIs

https://nifi.apache.org/nifi-docs/rest-api.html