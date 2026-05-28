# Data Contracts

This directory is the source of truth for all ingestion pipelines.

## Structure

```
<domain>/
  <database>/
    <sgdb>_<strategy>[_<variant>].yaml
```

## Example

The `fraud/ecommerce/postgres_full.yaml` contract is a working reference example. Copy it as a starting point for new pipelines.

## Validation

```bash
python scripts/validate_contracts.py   # schema check
python scripts/lint_naming.py          # naming conventions
```
