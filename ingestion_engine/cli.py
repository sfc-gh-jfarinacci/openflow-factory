from __future__ import annotations

import asyncio
from typing import Optional

try:
    import typer
except ImportError:
    raise SystemExit("CLI requires typer. Install with: pip install ingestion-engine[cli]")

from ingestion_engine import EngineConfig, Runtime, Deployer

app = typer.Typer(name="ingestion-engine", help="Snowflake OpenFlow Ingestion Engine CLI")


@app.command()
def list_runtimes():
    """List available OpenFlow runtimes."""
    config = EngineConfig()
    rt = Runtime(config)
    runtimes = asyncio.run(rt.list())
    rt.close()
    for r in runtimes:
        status = "✓" if r.enabled else "✗"
        typer.echo(f"  {status} {r.name:30s} {r.base_uri}")


@app.command()
def validate(
    template: str = typer.Argument(..., help="Template name (e.g. postgresql_cdc)"),
    runtime: str = typer.Option(..., "--runtime", "-r", help="Runtime integration name"),
    connector_id: Optional[str] = typer.Option(None, "--connector", "-c"),
    target_database: Optional[str] = typer.Option(None, "--target-db"),
    target_schema: Optional[str] = typer.Option(None, "--target-schema"),
):
    """Run all pre-deploy validations."""
    config = EngineConfig()
    deployer = Deployer(config, runtime)
    results = asyncio.run(deployer.validate(
        template,
        connector_id=connector_id,
        target_database=target_database,
        target_schema=target_schema,
    ))
    deployer.close()
    all_ok = results.pop("all_ok", False)
    for key, val in results.items():
        checks = val.checks if hasattr(val, "checks") else val.get("checks", [])
        for c in checks:
            icon = "✓" if c.get("ok") else "✗"
            typer.echo(f"  {icon} [{key}] {c.get('check', '')}: {c.get('message', 'passed')}")
    raise typer.Exit(0 if all_ok else 1)


@app.command()
def deploy(
    template: str = typer.Argument(..., help="Template name"),
    name: str = typer.Option(..., "--name", "-n", help="Flow name"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
    params_file: str = typer.Option(..., "--params", "-p", help="JSON file with param values"),
    connector_id: Optional[str] = typer.Option(None, "--connector", "-c"),
    auto_start: bool = typer.Option(False, "--start"),
    skip_validation: bool = typer.Option(False, "--skip-validation"),
):
    """Deploy a flow from template."""
    import json
    from pathlib import Path

    params = json.loads(Path(params_file).read_text())
    config = EngineConfig()
    deployer = Deployer(config, runtime)
    result = asyncio.run(deployer.deploy(
        template=template,
        name=name,
        params=params,
        connector_id=connector_id,
        auto_start=auto_start,
        skip_validation=skip_validation,
    ))
    deployer.close()
    typer.echo(f"Deployed: {result.flow.process_group_name} (PG: {result.flow.process_group_id})")
    typer.echo(f"  Method: {result.flow.import_method} | Strategy: {result.flow.strategy} | Started: {result.flow.started}")


@app.command()
def healthcheck(
    pg_id: str = typer.Argument(..., help="Process group ID"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
):
    """Check flow health."""
    config = EngineConfig()
    deployer = Deployer(config, runtime)
    result = asyncio.run(deployer.healthcheck(pg_id))
    deployer.close()
    icon = "✓" if result.get("ok") else "✗"
    typer.echo(f"  {icon} state={result.get('state', 'unknown')} running={result.get('running_count', 0)} invalid={result.get('invalid_count', 0)}")
    raise typer.Exit(0 if result.get("ok") else 1)


@app.command()
def run(
    pg_id: str = typer.Argument(..., help="Process group ID"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
):
    """Trigger a flow run (healthcheck + wait + trigger)."""
    config = EngineConfig()
    deployer = Deployer(config, runtime)
    result = asyncio.run(deployer.run(pg_id))
    deployer.close()
    typer.echo(f"  Triggered: state={result.get('state')} running={result.get('running_count')}")


@app.command()
def deploy_contract(
    path: str = typer.Argument(..., help="Contract path relative to contracts dir (e.g. fraud/ecommerce/postgres_full.yaml)"),
    sha: str = typer.Option(..., "--sha", "-s", help="Git commit SHA of the contracts repo"),
    runtime: Optional[str] = typer.Option(None, "--runtime", "-r", help="Runtime integration name (overrides auto-derived name)"),
    contracts_dir: str = typer.Option("./data_contracts", "--contracts-dir", "-d"),
    connector_id: Optional[str] = typer.Option(None, "--connector", "-c"),
    auto_start: bool = typer.Option(False, "--start"),
    force: bool = typer.Option(False, "--force", "-f", help="Force full redeploy even if sha/template unchanged"),
):
    """Deploy from a data contract YAML (contract-driven deployment)."""
    from pathlib import Path

    config = EngineConfig()
    deployer = Deployer(config)
    result = asyncio.run(deployer.deploy_from_contract(
        contract_path=path,
        sha=sha,
        contracts_dir=Path(contracts_dir),
        connector_id=connector_id,
        auto_start=auto_start,
        runtime_name_override=runtime,
        force=force,
    ))
    deployer.close()
    if result.idempotent_skip:
        typer.echo(f"  No-op: {result.flow.process_group_name} already deployed at sha {sha}")
    else:
        typer.echo(f"  Deployed: {result.flow.process_group_name} (PG: {result.flow.process_group_id})")
        typer.echo(f"  Method: {result.flow.import_method} | Strategy: {result.flow.strategy} | Started: {result.flow.started}")


@app.command()
def validate_contract(
    path: str = typer.Argument(..., help="Contract path relative to contracts dir"),
    contracts_dir: str = typer.Option("./data_contracts", "--contracts-dir", "-d"),
):
    """Validate a contract YAML against the schema."""
    import json
    from pathlib import Path
    import jsonschema
    import yaml as _yaml

    contracts_root = Path(contracts_dir)
    contract_file = contracts_root / path
    if not contract_file.exists():
        typer.echo(f"  Contract not found: {contract_file}")
        raise typer.Exit(1)

    schema_path = contracts_root / "schema" / "contract.v1.schema.json"
    if not schema_path.exists():
        typer.echo(f"  Schema not found: {schema_path}")
        raise typer.Exit(1)

    schema = json.loads(schema_path.read_text())
    doc = _yaml.safe_load(contract_file.read_text())

    try:
        jsonschema.validate(doc, schema)
        typer.echo(f"  OK: {path} is valid")
    except jsonschema.ValidationError as e:
        typer.echo(f"  FAILED: {e.message}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
