from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from ingestion_engine._snowflake_conn import SnowflakeConn
from ingestion_engine.config import EngineConfig
from ingestion_engine.exceptions import DeployError, ValidationError
from ingestion_engine.flow.flow import Flow, FlowDeployResult
from ingestion_engine.runtime.runtime import Runtime
from ingestion_engine.target.target import Target
from ingestion_engine.templates.renderer import render, derive_runtime_name
from ingestion_engine.templates.selector import select_template


@dataclass
class DeployResult:
    flow: FlowDeployResult
    validations: dict = field(default_factory=dict)
    idempotent_skip: bool = False


class Deployer:
    def __init__(self, config: EngineConfig, runtime_name: Optional[str] = None):
        self._config = config
        self._runtime_name = runtime_name or ""
        self._runtime = Runtime(config)
        self._flow = Flow(config, runtime_name or "") if runtime_name else None
        self._target = Target(config)
        self._sf = SnowflakeConn(config)

    async def validate(
        self,
        template: str,
        *,
        connector_id: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
    ) -> dict:
        results: dict[str, any] = {}

        runtime_deps = await self._runtime.validate_dependencies(template, self._runtime_name or None)
        results["runtime_dependencies"] = runtime_deps

        if connector_id:
            secrets_result = await self._runtime.validate_secrets(connector_id)
            results["secrets"] = secrets_result

        if self._flow:
            template_result = self._flow.validate_template(template)
            results["template"] = template_result

        if target_database:
            target_result = self._target.validate_exists(target_database, target_schema)
            results["target_exists"] = target_result
            perms_result = self._target.validate_permissions(target_database, target_schema)
            results["target_permissions"] = perms_result

        all_ok = all(
            (v.ok if hasattr(v, "ok") else v.get("ok", False))
            for v in results.values()
        )
        results["all_ok"] = all_ok
        return results

    async def deploy(
        self,
        template: str,
        name: str,
        params: dict[str, dict[str, str]],
        *,
        connector_id: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        auto_start: bool = False,
        skip_validation: bool = False,
    ) -> DeployResult:
        validations = {}
        if not skip_validation:
            validations = await self.validate(
                template,
                connector_id=connector_id,
                target_database=target_database,
                target_schema=target_schema,
            )
            if not validations.get("all_ok"):
                failed = {k: v for k, v in validations.items() if k != "all_ok" and hasattr(v, "ok") and not v.ok}
                raise ValidationError([
                    {"check": k, "ok": False, "message": str(v.checks if hasattr(v, "checks") else v)}
                    for k, v in failed.items()
                ])

        flow_result = await self._flow.deploy_from_template(
            template=template,
            name=name,
            params=params,
            connector_id=connector_id,
            auto_start=auto_start,
        )

        return DeployResult(flow=flow_result, validations=validations)
    async def healthcheck(self, process_group_id: str) -> dict:
        try:
            status = await self._flow.status(process_group_id)
            reachable = True
            healthy = status.state == "running" and status.invalid_count == 0
        except Exception as e:
            return {"ok": False, "reachable": False, "error": str(e)}

        return {
            "ok": healthy,
            "reachable": reachable,
            "state": status.state,
            "running_count": status.running_count,
            "stopped_count": status.stopped_count,
            "invalid_count": status.invalid_count,
            "queued_count": status.queued_count,
        }

    async def run(
        self,
        process_group_id: str,
        *,
        wait_for_idle: bool = True,
        idle_timeout: float = 60.0,
    ) -> dict:
        health = await self.healthcheck(process_group_id)
        if not health.get("reachable"):
            raise DeployError(f"Flow not reachable: {health.get('error')}")

        if wait_for_idle and health.get("state") == "running":
            elapsed = 0.0
            interval = 2.0
            while elapsed < idle_timeout:
                await asyncio.sleep(interval)
                elapsed += interval
                status = await self._flow.status(process_group_id)
                if status.state != "running" or status.queued_count == 0:
                    break

        await self._flow.trigger(process_group_id)
        status = await self._flow.status(process_group_id)
        return {
            "triggered": True,
            "state": status.state,
            "running_count": status.running_count,
            "queued_count": status.queued_count,
        }

    async def deploy_from_contract(
        self,
        contract_path: str,
        sha: str,
        contracts_dir: Path,
        *,
        connector_id: Optional[str] = None,
        auto_start: bool = False,
        runtime_name_override: Optional[str] = None,
        force: bool = False,
    ) -> DeployResult:
        contract_file = contracts_dir / contract_path
        if not contract_file.exists():
            raise DeployError(f"Contract not found: {contract_file}")

        contract = yaml.safe_load(contract_file.read_text())
        parts = Path(contract_path).parts
        domain = parts[0]
        filename_stem = contract_file.stem

        runtime_name = runtime_name_override or derive_runtime_name(domain, filename_stem)
        self._runtime_name = runtime_name
        self._flow = Flow(self._config, runtime_name)

        contract["_domain"] = domain
        contract["_contract_path"] = contract_path

        tpl = select_template(contract["source_sgdb"], contract["type"])
        manifest = tpl["manifest"]
        secret_fqn = None
        if connector_id:
            import re
            sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", connector_id).upper()
            if not sanitized[0:1].isalpha() and sanitized[0:1] != "_":
                sanitized = f"C_{sanitized}"
            secret_fqn = f"OPENFLOW_FACTORY.SECRETS.{sanitized}_CREDS"

        params = render(contract, manifest, secret_fqn=secret_fqn, config=self._config)

        prev = self._get_last_deployment(runtime_name)

        if not force and prev and prev.get("CONTRACT_SHA") == sha:
            return DeployResult(
                flow=FlowDeployResult(
                    process_group_id=prev.get("PROCESS_GROUP_ID", ""),
                    process_group_name=runtime_name,
                    import_method="skipped",
                    strategy=contract.get("type", "full"),
                ),
                idempotent_skip=True,
            )

        if prev and prev.get("PROCESS_GROUP_ID"):
            await self._replace_flow(prev["PROCESS_GROUP_ID"])

        flow_result = await self._flow.deploy_from_template(
            template=tpl["template_id"],
            name=runtime_name,
            params=params,
            connector_id=connector_id,
            auto_start=auto_start,
        )

        self._record_deployment(
            runtime_name=runtime_name,
            contract_paths=[contract_path],
            template_id=tpl["template_id"],
            template_version=tpl["version"],
            sha=sha,
            process_group_id=flow_result.process_group_id,
        )

        return DeployResult(flow=flow_result)

    async def _update_params(self, pg_id: str, params: dict[str, dict[str, str]], auto_start: bool) -> bool:
        client = await self._flow._get_client()
        try:
            await client.stop_process_group(pg_id)
            await asyncio.sleep(2)
            await client.disable_controller_services(pg_id)
            await asyncio.sleep(3)

            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                return False

            ctx_id = ctx_data.get("id") or ctx_data.get("component", {}).get("id")
            revision = ctx_data.get("revision", {}).get("version", 0)

            flat_params = {}
            for ctx_name, values in params.items():
                if isinstance(values, dict):
                    flat_params.update(values)

            await client.update_parameter_context(ctx_id, revision, flat_params)
            await asyncio.sleep(2)

            if auto_start:
                await client.enable_controller_services(pg_id)
                await asyncio.sleep(1)
                await client.start_process_group(pg_id)

            return True
        finally:
            await client.close()

    async def _replace_flow(self, pg_id: str) -> None:
        client = await self._flow._get_client()
        try:
            await client.stop_process_group(pg_id)
            await asyncio.sleep(2)
            await client.drain_queues(pg_id)
            await client.disable_controller_services(pg_id)
            await asyncio.sleep(3)
            await client.delete_process_group(pg_id)
        finally:
            await client.close()

    def _get_last_deployment(self, runtime_name: str) -> Optional[dict]:
        try:
            row = self._sf.query_one(
                "SELECT contract_sha, template_id, template_version, process_group_id "
                "FROM OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "WHERE runtime_name = %s ORDER BY deployed_at DESC LIMIT 1",
                [runtime_name],
            )
            return row
        except Exception:
            return None

    def _record_deployment(
        self,
        runtime_name: str,
        contract_paths: list[str],
        template_id: str,
        template_version: str,
        sha: str,
        process_group_id: str,
    ):
        try:
            self._sf.execute(
                "INSERT INTO OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "(runtime_name, contract_paths, template_id, template_version, contract_sha, process_group_id) "
                "SELECT %s, PARSE_JSON(%s), %s, %s, %s, %s",
                [runtime_name, json.dumps(contract_paths), template_id, template_version, sha, process_group_id],
            )
        except Exception:
            pass

    def close(self):
        self._runtime.close()
        if self._flow:
            self._flow.close()
        self._target.close()
        self._sf.close()
