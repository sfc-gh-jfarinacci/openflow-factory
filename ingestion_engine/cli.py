from __future__ import annotations

import asyncio
from pathlib import Path
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
def update_params(
    pg_id: str = typer.Argument(..., help="Process group ID of the deployed flow"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
    params_file: str = typer.Option(
        ..., "--params", "-p", help="JSON file with param values ({context_name: {key: value}})"
    ),
    auto_start: bool = typer.Option(True, "--start/--no-start", help="Restart flow after updating (default: yes)"),
):
    """Update parameters on a running flow without redeployment.

    Stops the flow, updates parameter context values, then restarts.
    """
    import json

    params = json.loads(Path(params_file).read_text())
    config = EngineConfig()
    deployer = Deployer(config, runtime)
    success = asyncio.run(deployer._update_params(pg_id, params, auto_start))
    deployer.close()
    if success:
        typer.echo(f"  Parameters updated on {pg_id}")
        if auto_start:
            typer.echo("  Flow restarted.")
        else:
            typer.echo("  Flow left stopped. Start manually when ready.")
    else:
        typer.echo("  Failed: no parameter context found for this process group.")
        raise typer.Exit(1)


@app.command()
def delete_flow(
    pg_id: str = typer.Argument(..., help="Process group ID to delete"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Delete a deployed flow from the runtime.

    Stops processors, drains queues, disables controllers, deletes the process
    group, and cleans up orphaned parameter contexts.
    """
    config = EngineConfig()
    deployer = Deployer(config, runtime)

    if not confirm:
        health = asyncio.run(deployer.healthcheck(pg_id))
        typer.echo(f"  Flow: {pg_id}")
        typer.echo(f"  State: {health.get('state', 'unknown')} (running={health.get('running_count', 0)})")
        if not typer.confirm("  Delete this flow? This cannot be undone."):
            raise typer.Abort()

    asyncio.run(deployer._replace_flow(pg_id))
    deployer.close()
    typer.echo(f"  Deleted: {pg_id}")


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


@app.command()
def list_assets(
    pg_id: str = typer.Argument(..., help="Process group ID"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
):
    """List assets attached to a flow's parameter context."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                typer.echo("  No parameter context found for this process group")
                return
            ctx_id = ctx_data.get("id") or ctx_data.get("component", {}).get("id")
            assets = await client.list_assets(ctx_id)
            if not assets:
                typer.echo("  No assets found")
                return
            for a in assets:
                typer.echo(f"  {a.get('id', '?')[:8]}  {a.get('name', '?')}")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def upload_asset(
    pg_id: str = typer.Argument(..., help="Process group ID"),
    file: str = typer.Argument(..., help="Path to the asset file (e.g. ./postgresql-42.7.4.jar)"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
    param_name: Optional[str] = typer.Option(None, "--param", "-p", help="Parameter name to update with the asset path"),
):
    """Upload an asset to a flow's parameter context (skips if same name exists)."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)
    file_path = Path(file)
    if not file_path.exists():
        typer.echo(f"  File not found: {file_path}")
        raise typer.Exit(1)

    async def _run():
        client = await flow._get_client()
        try:
            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                typer.echo("  No parameter context found")
                raise typer.Exit(1)
            ctx_id = ctx_data.get("id") or ctx_data.get("component", {}).get("id")
            asset = await client.ensure_asset(ctx_id, file_path.name, file_path.read_bytes())
            asset_id = asset.get("id", "")
            asset_name = asset.get("name", file_path.name)
            typer.echo(f"  Asset: {asset_name} (id={asset_id[:8]})")

            if param_name:
                await client.link_asset_to_parameter(ctx_id, param_name, asset_id)
                typer.echo(f"  Linked asset to param '{param_name}' as reference asset")
            else:
                typer.echo(f"  Uploaded (use --param to link to a parameter)")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def delete_asset(
    pg_id: str = typer.Argument(..., help="Process group ID"),
    asset_id: str = typer.Argument(..., help="Asset ID to delete"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
):
    """Delete an asset from a flow's parameter context."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                typer.echo("  No parameter context found")
                raise typer.Exit(1)
            ctx_id = ctx_data.get("id") or ctx_data.get("component", {}).get("id")
            await client.delete_asset(ctx_id, asset_id)
            typer.echo(f"  Deleted asset {asset_id}")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def create_secret(
    fqn: str = typer.Argument(..., help="Fully qualified secret name (e.g. OPENFLOW_FACTORY.SECRETS.MY_SECRET)"),
    value: str = typer.Option(..., "--value", "-v", prompt=True, hide_input=True, help="Secret value (prompted securely if not provided)"),
    comment: Optional[str] = typer.Option(None, "--comment"),
):
    """Create a Snowflake GENERIC_STRING secret for use with SnowflakeParameterProvider."""
    from ingestion_engine.secrets import Secrets

    config = EngineConfig()
    secrets = Secrets(config)
    info = secrets.create_named(fqn, value, comment=comment)
    secrets.close()
    typer.echo(f"  Created: {info.fqn}")


@app.command()
def create_contract_secrets(
    path: str = typer.Argument(..., help="Contract path relative to contracts dir"),
    contracts_dir: str = typer.Option("./data_contracts", "--contracts-dir", "-d"),
):
    """Create Snowflake secrets referenced in a contract. Prompts for each value.

    Reads the contract's `secrets` map (param_name → secret_fqn) and creates
    each secret as GENERIC_STRING in OPENFLOW_FACTORY.SECRETS.
    """
    import yaml as _yaml
    from ingestion_engine.secrets import Secrets

    contracts_root = Path(contracts_dir)
    contract_file = contracts_root / path
    if not contract_file.exists():
        typer.echo(f"  Contract not found: {contract_file}")
        raise typer.Exit(1)

    contract = _yaml.safe_load(contract_file.read_text())
    secrets_map = contract.get("secrets", {})
    if not secrets_map:
        typer.echo("  No secrets defined in contract")
        raise typer.Exit(0)

    config = EngineConfig()
    secrets = Secrets(config)

    for param_name, secret_fqn in secrets_map.items():
        if secrets.exists_by_fqn(secret_fqn):
            typer.echo(f"  Exists: {secret_fqn}")
            continue
        val = typer.prompt(f"  Enter value for '{param_name}' ({secret_fqn})", hide_input=True)
        info = secrets.create_named(secret_fqn, val, comment=f"Sensitive param: {param_name}")
        typer.echo(f"  Created: {info.fqn}")

    secrets.close()


@app.command()
def list_secrets(
    database: Optional[str] = typer.Option(None, "--database", "-db"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s"),
):
    """List Snowflake secrets available for parameter providers."""
    from ingestion_engine.secrets import Secrets

    config = EngineConfig()
    secrets = Secrets(config)
    rows = secrets.list_secrets(database=database, schema=schema)
    secrets.close()
    if not rows:
        typer.echo("  No secrets found")
        return
    for r in rows:
        typer.echo(f"  {r['fqn']:60s} {r['secret_type']}")


@app.command()
def list_parameter_providers(
    runtime: str = typer.Option(..., "--runtime", "-r"),
):
    """List parameter providers configured on the runtime."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            providers = await client.list_parameter_providers()
            if not providers:
                typer.echo("  No parameter providers configured")
                return
            for p in providers:
                status = p.get("validation_status", "?")
                typer.echo(f"  {p['id'][:12]}  {p['name']:40s} [{status}]")
                groups = p.get("parameter_group_configurations", [])
                for g in groups:
                    ctx_name = g.get("parameterContextName", g.get("groupName", "?"))
                    typer.echo(f"               → {ctx_name}")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def list_parameter_provider_types(
    runtime: str = typer.Option(..., "--runtime", "-r"),
):
    """List available parameter provider types on the runtime."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            types = await client.get_parameter_provider_types()
            for t in types:
                typer.echo(f"  {t['type']}")
                if t.get("description"):
                    desc = t["description"].strip().split("\n")[0][:80]
                    typer.echo(f"    {desc}")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def setup_snowflake_parameter_provider(
    runtime: str = typer.Option(..., "--runtime", "-r"),
    name: str = typer.Option("Snowflake Secrets Provider", "--name", "-n"),
    database: Optional[str] = typer.Option(None, "--database", "-db", help="Snowflake database to fetch secrets from"),
    schema_pattern: Optional[str] = typer.Option(None, "--schema-pattern", help="Regex pattern for schema filtering"),
    secret_pattern: Optional[str] = typer.Option(None, "--secret-pattern", help="Regex pattern for secret name filtering"),
):
    """Create and configure a SnowflakeParameterProvider with a SNOWFLAKE_SESSION_TOKEN connection service."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            existing = await client.list_parameter_providers()
            for p in existing:
                if p["name"] == name:
                    typer.echo(f"  Already exists: {p['id']}")
                    return

            svc = await client.create_root_controller_service(
                name=f"Snowflake Connection ({name})",
                service_type="com.snowflake.openflow.runtime.services.snowflake.SnowflakeConnectionService",
                bundle={
                    "group": "com.snowflake.openflow.runtime",
                    "artifact": "runtime-snowflake-connection-service-nar",
                    "version": "2026.5.5.19",
                },
                properties={"Authentication Strategy": "SNOWFLAKE_SESSION_TOKEN"},
            )
            svc_id = svc.get("id") or svc.get("component", {}).get("id")
            await client.enable_root_controller_service(svc_id)

            props: dict = {"Snowflake Connection Service": svc_id}
            if database:
                props["Database Name"] = database
            if schema_pattern:
                props["Schema Name Pattern"] = schema_pattern
            if secret_pattern:
                props["Secret Name Pattern"] = secret_pattern

            provider = await client.create_parameter_provider(
                name=name,
                provider_type="com.snowflake.openflow.runtime.parameter.snowflake.SnowflakeParameterProvider",
                bundle={
                    "group": "com.snowflake.openflow.runtime",
                    "artifact": "runtime-snowflake-parameter-provider-nar",
                    "version": "2026.5.5.19",
                },
                properties=props,
            )
            provider_id = provider.get("id") or provider.get("component", {}).get("id")
            validation = provider.get("component", {}).get("validationStatus", "?")
            typer.echo(f"  Created provider: {provider_id} [{validation}]")
            typer.echo(f"  Connection service: {svc_id} [ENABLED]")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def fetch_params(
    runtime: str = typer.Option(..., "--runtime", "-r"),
    provider_id: Optional[str] = typer.Option(None, "--provider-id", "-p", help="Parameter provider ID (auto-detects if only one exists)"),
):
    """Fetch parameters from a parameter provider (discovers secrets from Snowflake)."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            pid = provider_id
            if not pid:
                providers = await client.list_parameter_providers()
                if not providers:
                    typer.echo("  No parameter providers configured")
                    raise typer.Exit(1)
                if len(providers) == 1:
                    pid = providers[0]["id"]
                else:
                    typer.echo("  Multiple providers found, specify --provider-id:")
                    for p in providers:
                        typer.echo(f"    {p['id']}  {p['name']}")
                    raise typer.Exit(1)

            result = await client.fetch_parameters(pid)
            req_id = result.get("parameterProviderParameterFetchRequest", {}).get("id") or result.get("id")
            if req_id:
                final = await client.wait_for_fetch(pid, req_id)
                groups = final.get("parameterGroups") or final.get("parameterGroupConfigurations") or []
                typer.echo(f"  Fetch complete. Found {len(groups)} parameter group(s):")
                for g in groups:
                    group_name = g.get("groupName", g.get("name", "?"))
                    params = g.get("parameterSensitivities", g.get("parameters", {}))
                    typer.echo(f"    {group_name}: {len(params)} parameter(s)")
                    for pname in (params if isinstance(params, dict) else []):
                        typer.echo(f"      - {pname}")
            else:
                typer.echo(f"  Fetch initiated: {result}")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def populate_secrets(
    path: str = typer.Argument(..., help="Contract path relative to contracts dir"),
    pg_id: str = typer.Option(..., "--pg-id", help="Process group ID of the deployed flow"),
    runtime: str = typer.Option(..., "--runtime", "-r"),
    contracts_dir: str = typer.Option("./data_contracts", "--contracts-dir", "-d"),
):
    """Fetch secrets from Snowflake via ParameterProvider and apply to the flow's parameter context.

    Uses the contract's `secrets.schema` to configure the provider fetch scope,
    then applies the fetched group to the flow's bound parameter context.
    Secret names in Snowflake must match the NiFi parameter names exactly.
    """
    import yaml as _yaml
    from ingestion_engine.flow.flow import Flow

    contracts_root = Path(contracts_dir)
    contract_file = contracts_root / path
    if not contract_file.exists():
        typer.echo(f"  Contract not found: {contract_file}")
        raise typer.Exit(1)

    contract = _yaml.safe_load(contract_file.read_text())
    secrets_spec = contract.get("secrets", {})
    schema = secrets_spec.get("schema")
    if not schema:
        typer.echo("  No secrets.schema defined in contract")
        raise typer.Exit(0)

    schema_parts = schema.split(".")
    db_name = schema_parts[0] if len(schema_parts) >= 1 else None
    schema_name = schema_parts[1] if len(schema_parts) >= 2 else None

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            providers = await client.list_parameter_providers()
            pid = None
            for p in providers:
                if p.get("properties", {}).get("Database Name") == db_name:
                    schema_pat = p.get("properties", {}).get("Schema Name Pattern", "")
                    if not schema_pat or schema_name in schema_pat:
                        pid = p["id"]
                        break

            if not pid:
                if len(providers) == 1:
                    pid = providers[0]["id"]
                    typer.echo(f"  Using existing provider: {providers[0]['name']}")
                else:
                    typer.echo("  No matching parameter provider found. Run setup-snowflake-parameter-provider first.")
                    raise typer.Exit(1)

            result = await client.fetch_parameters(pid)
            req_id = result.get("parameterProviderParameterFetchRequest", {}).get("id") or result.get("id")
            if not req_id:
                typer.echo("  Fetch failed")
                raise typer.Exit(1)

            final = await client.wait_for_fetch(pid, req_id)
            groups = final.get("parameterGroups") or final.get("parameterGroupConfigurations") or []

            target_group = None
            for g in groups:
                group_name = g.get("groupName", g.get("name", ""))
                if group_name == schema or schema_name in group_name:
                    target_group = g
                    break

            if not target_group:
                typer.echo(f"  No parameter group matching '{schema}' found after fetch. Available: {[g.get('groupName') for g in groups]}")
                raise typer.Exit(1)

            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                typer.echo("  No parameter context found for this process group")
                raise typer.Exit(1)
            ctx_name = ctx_data.get("component", {}).get("name", "")

            group_name = target_group.get("groupName", target_group.get("name", ""))
            sensitivities = target_group.get("parameterSensitivities", {})

            apply_configs = [{
                "groupName": group_name,
                "parameterContextName": ctx_name,
                "parameterSensitivities": sensitivities,
                "synchronized": False,
            }]

            await client.apply_fetched_parameters(pid, apply_configs)
            typer.echo(f"  Applied secrets from '{group_name}' → context '{ctx_name}'")
            for pname in sensitivities:
                typer.echo(f"    ✓ {pname}")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def apply_params(
    runtime: str = typer.Option(..., "--runtime", "-r"),
    provider_id: Optional[str] = typer.Option(None, "--provider-id", "-p"),
    context_name: Optional[str] = typer.Option(None, "--context", "-c", help="Target parameter context name (creates if needed)"),
):
    """Fetch parameters from provider and apply them to a parameter context."""
    from ingestion_engine.flow.flow import Flow

    config = EngineConfig()
    flow = Flow(config, runtime)

    async def _run():
        client = await flow._get_client()
        try:
            pid = provider_id
            if not pid:
                providers = await client.list_parameter_providers()
                if len(providers) == 1:
                    pid = providers[0]["id"]
                else:
                    typer.echo("  Specify --provider-id")
                    raise typer.Exit(1)

            result = await client.fetch_parameters(pid)
            req_id = result.get("parameterProviderParameterFetchRequest", {}).get("id") or result.get("id")
            if not req_id:
                typer.echo("  Fetch failed")
                raise typer.Exit(1)

            final = await client.wait_for_fetch(pid, req_id)
            groups = final.get("parameterGroups") or final.get("parameterGroupConfigurations") or []

            if not groups:
                typer.echo("  No parameter groups found after fetch")
                raise typer.Exit(1)

            apply_configs = []
            for g in groups:
                group_name = g.get("groupName", g.get("name", ""))
                sensitivities = g.get("parameterSensitivities", {})
                target_ctx = context_name or group_name
                apply_configs.append({
                    "groupName": group_name,
                    "parameterContextName": target_ctx,
                    "parameterSensitivities": sensitivities,
                    "synchronized": False,
                })

            apply_result = await client.apply_fetched_parameters(pid, apply_configs)
            typer.echo(f"  Applied {len(apply_configs)} group(s) to parameter context(s)")
            for ac in apply_configs:
                typer.echo(f"    {ac['groupName']} → context '{ac['parameterContextName']}'")
        finally:
            await client.close()

    asyncio.run(_run())
    flow.close()


@app.command()
def create_network_rule(
    name: str = typer.Argument(..., help="Rule name (e.g. postgres_private_network_rule)"),
    values: list[str] = typer.Option(..., "--value", "-v", help="host:port values (repeatable)"),
    rule_type: str = typer.Option("PRIVATE_HOST_PORT", "--type", "-t"),
    mode: str = typer.Option("EGRESS", "--mode"),
):
    """Create a network rule in OPENFLOW_FACTORY.RULES."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)
    info = access.create_network_rule(name, values, mode=mode, rule_type=rule_type)
    access.close()
    typer.echo(f"  Created: {info.fqn}")
    for v in info.values:
        typer.echo(f"    {v}")


@app.command()
def alter_network_rule(
    name: str = typer.Argument(..., help="Rule name"),
    add: list[str] = typer.Option(None, "--add", "-a", help="host:port values to add"),
    remove: list[str] = typer.Option(None, "--remove", "-r", help="host:port values to remove"),
):
    """Add or remove values from a network rule."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)
    if add:
        info = access.alter_network_rule_add(name, add)
        typer.echo(f"  Added to {info.fqn}: {add}")
    if remove:
        info = access.alter_network_rule_remove(name, remove)
        typer.echo(f"  Removed from {info.fqn}: {remove}")
    if not add and not remove:
        typer.echo("  Specify --add or --remove")
    access.close()


@app.command()
def delete_network_rule(
    name: str = typer.Argument(..., help="Rule name"),
):
    """Delete a network rule. Automatically removes it from any EAI referencing it."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)
    access.delete_network_rule(name)
    access.close()
    typer.echo(f"  Deleted: {name}")


@app.command()
def create_eai(
    name: str = typer.Argument(..., help="EAI name (e.g. OPENFLOW_PRIVATE_EAI)"),
    rules: list[str] = typer.Option(..., "--rule", "-r", help="Network rule names (repeatable)"),
):
    """Create an External Access Integration with the specified network rules."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)
    info = access.create_eai(name, rules)
    access.close()
    typer.echo(f"  Created: {info.name}")
    for r in info.network_rules:
        typer.echo(f"    {r}")


@app.command()
def alter_eai(
    name: str = typer.Argument(..., help="EAI name"),
    add_rules: list[str] = typer.Option(None, "--add-rule", "-a", help="Rules to add"),
    remove_rules: list[str] = typer.Option(None, "--remove-rule", "-r", help="Rules to remove (rule is NOT deleted)"),
):
    """Add or remove network rules from an EAI. Removing a rule does NOT delete the rule itself."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)
    if add_rules:
        info = access.alter_eai_add_rules(name, add_rules)
        typer.echo(f"  Added rules to {name}: {add_rules}")
    if remove_rules:
        info = access.alter_eai_remove_rules(name, remove_rules)
        typer.echo(f"  Removed rules from {name}: {remove_rules}")
    if not add_rules and not remove_rules:
        typer.echo("  Specify --add-rule or --remove-rule")
    access.close()


@app.command()
def delete_eai(
    name: str = typer.Argument(..., help="EAI name"),
):
    """Delete an External Access Integration."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)
    access.delete_eai(name)
    access.close()
    typer.echo(f"  Deleted: {name}")


@app.command()
def list_access(
    runtime: Optional[str] = typer.Option(None, "--runtime", "-r", help="Show EAI attached to this runtime"),
):
    """List network rules and EAIs."""
    from ingestion_engine.access import Access

    config = EngineConfig()
    access = Access(config)

    typer.echo("Network Rules:")
    rules = access.list_network_rules()
    if not rules:
        typer.echo("  (none)")
    for r in rules:
        typer.echo(f"  {r.fqn:50s} {r.mode} {r.rule_type}")

    typer.echo("\nExternal Access Integrations:")
    eais = access.list_eais()
    if not eais:
        typer.echo("  (none)")
    for e in eais:
        status = "enabled" if e.enabled else "disabled"
        typer.echo(f"  {e.name:40s} [{status}]")
        for rule in e.network_rules:
            typer.echo(f"    {rule}")

    access.close()


if __name__ == "__main__":
    app()
