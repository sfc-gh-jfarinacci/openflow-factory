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


ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"


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
        subdomain = parts[1] if len(parts) > 2 else None
        filename_stem = contract_file.stem

        flow_name = derive_runtime_name(domain, filename_stem, subdomain=subdomain)
        runtime_target = runtime_name_override or flow_name
        self._runtime_name = runtime_target
        self._flow = Flow(self._config, runtime_target)

        contract["_domain"] = domain
        contract["_contract_path"] = contract_path

        tpl = select_template(contract["source_sgdb"], contract["type"])
        manifest = tpl["manifest"]

        params = render(contract, manifest, config=self._config)

        prev = self._get_last_deployment(flow_name)

        if not force and prev and prev.get("CONTRACT_SHA") == sha:
            return DeployResult(
                flow=FlowDeployResult(
                    process_group_id=prev.get("PROCESS_GROUP_ID", ""),
                    process_group_name=flow_name,
                    import_method="skipped",
                    strategy=contract.get("type", "full"),
                ),
                idempotent_skip=True,
            )

        if prev and prev.get("PROCESS_GROUP_ID"):
            await self._replace_flow(prev["PROCESS_GROUP_ID"])

        flow_result = await self._flow.deploy_from_template(
            template=tpl["template_id"],
            name=flow_name,
            params=params,
            connector_id=connector_id,
            auto_start=False,
        )

        await self._apply_rendered_params(flow_result.process_group_id, params)

        await self._resolve_assets(
            flow_result.process_group_id,
            manifest,
            template_dir=tpl.get("template_dir"),
            contract_assets=contract.get("assets"),
        )

        await self._resolve_secrets(
            flow_result.process_group_id,
            contract,
            manifest,
        )

        if auto_start:
            client = await self._flow._get_client()
            try:
                await self._flow._robust_start(client, flow_result.process_group_id)
                flow_result.started = True
            finally:
                await client.close()

        self._record_deployment(
            runtime_name=flow_name,
            contract_paths=[contract_path],
            template_id=tpl["template_id"],
            template_version=tpl["version"],
            sha=sha,
            process_group_id=flow_result.process_group_id,
        )

        return DeployResult(flow=flow_result)

    async def _apply_rendered_params(self, pg_id: str, params: dict[str, dict[str, str]]) -> None:
        client = await self._flow._get_client()
        try:
            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                return
            ctx_id = ctx_data.get("component", {}).get("id") or ctx_data.get("id")
            revision = ctx_data.get("revision", {}).get("version", 0)
            inherited = ctx_data.get("component", {}).get("inheritedParameterContexts", [])

            flat_params = {}
            for ctx_name, values in params.items():
                if isinstance(values, dict):
                    flat_params.update(values)

            if not flat_params:
                return

            existing_params = ctx_data.get("component", {}).get("parameters", [])
            updated = []
            for p in existing_params:
                param = p.get("parameter", {})
                name = param.get("name", "")
                has_asset = bool(param.get("referencedAssets"))
                if has_asset:
                    continue
                if name in flat_params:
                    updated.append({
                        "parameter": {
                            "name": name,
                            "value": flat_params[name],
                            "sensitive": param.get("sensitive", False),
                        }
                    })
                else:
                    updated.append(p)

            try:
                await client._put(f"parameter-contexts/{ctx_id}", {
                    "revision": {"version": revision},
                    "id": ctx_id,
                    "component": {
                        "id": ctx_id,
                        "inheritedParameterContexts": inherited,
                        "parameters": updated,
                    },
                })
            except Exception:
                pass
        finally:
            await client.close()

    async def _resolve_secrets(
        self,
        pg_id: str,
        contract: dict,
        manifest: dict,
    ) -> None:
        secrets_map = contract.get("secrets", {})
        if not secrets_map:
            return

        client = await self._flow._get_client()
        try:
            providers = await client.list_parameter_providers()
            provider_id = None
            if providers:
                for p in providers:
                    if p.get("validation_status") == "VALID":
                        provider_id = p["id"]
                        break
                if not provider_id:
                    provider_id = await self._fix_or_create_parameter_provider(client, providers)
            else:
                provider_id = await self._ensure_parameter_provider(client)

            if not provider_id:
                return

            await client.fetch_parameters(provider_id)
            await asyncio.sleep(2)

            provider = await client._get(f"parameter-providers/{provider_id}")
            groups = provider.get("component", {}).get("parameterGroupConfigurations", [])

            secret_fqns = list(secrets_map.values())
            if not secret_fqns:
                return
            first_fqn = secret_fqns[0]
            parts = first_fqn.split(".")
            target_schema = f"{parts[0]}.{parts[1]}" if len(parts) >= 3 else ""

            target_group = None
            for g in groups:
                if g.get("groupName", "") == target_schema:
                    target_group = g
                    break

            if not target_group:
                return

            secret_names = [fqn.split(".")[-1] for fqn in secret_fqns]
            sensitivities = {name: "SENSITIVE" for name in secret_names}

            ctx_name = f"Secrets - {contract.get('_domain', 'default')}"

            all_ctxs = await client.get_parameter_contexts()
            secrets_ctx_id = None
            for c in all_ctxs:
                if c.get("component", {}).get("name") == ctx_name:
                    secrets_ctx_id = c.get("component", {}).get("id")
                    break

            if not secrets_ctx_id:
                await client.apply_fetched_parameters(provider_id, [{
                    "groupName": target_group["groupName"],
                    "parameterContextName": ctx_name,
                    "parameterSensitivities": sensitivities,
                    "synchronized": True,
                }])

                all_ctxs = await client.get_parameter_contexts()
                for c in all_ctxs:
                    if c.get("component", {}).get("name") == ctx_name:
                        secrets_ctx_id = c.get("component", {}).get("id")
                        break

            if not secrets_ctx_id:
                return

            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                return
            flow_ctx_id = ctx_data.get("component", {}).get("id") or ctx_data.get("id")

            await client.inherit_parameter_context(flow_ctx_id, secrets_ctx_id)

            source_sgdb = contract.get("source_sgdb", "").lower()
            for param_name, secret_fqn in secrets_map.items():
                secret_obj_name = secret_fqn.split(".")[-1]
                svcs = await client.list_controller_services(pg_id)
                for s in svcs:
                    comp = s.get("component", {})
                    svc_type = comp.get("type", "").lower()
                    svc_name = comp.get("name", "").lower()
                    if "snowflake" in svc_type or "snowflake" in svc_name:
                        continue
                    if source_sgdb not in svc_type and source_sgdb not in svc_name:
                        if "dbcp" not in svc_type and "hikari" not in svc_type and "connection" not in svc_name:
                            continue
                    descs = comp.get("descriptors", {})
                    for prop_key, desc in descs.items():
                        if desc.get("sensitive"):
                            keyword = param_name.split()[-1].lower()
                            if keyword in prop_key.lower():
                                await client.update_controller_service_properties(
                                    comp["id"], {prop_key: f"#{{{secret_obj_name}}}"}
                                )
        finally:
            await client.close()

    async def _fix_or_create_parameter_provider(self, client, providers: list[dict]) -> str:
        existing_provider = providers[0]
        provider_id = existing_provider["id"]

        root_svcs = await client.get_root_controller_services()
        svc_id = None
        for s in root_svcs:
            comp = s.get("component", {})
            if "snowflake" in comp.get("type", "").lower() and comp.get("state") == "ENABLED":
                svc_id = comp["id"]
                break

        if not svc_id:
            svc_bundle = await self._discover_bundle(client, "controller-service", "SnowflakeConnectionService")
            svc = await client.create_root_controller_service(
                name="Snowflake Connection (Secrets Provider)",
                service_type="com.snowflake.openflow.runtime.services.snowflake.SnowflakeConnectionService",
                bundle=svc_bundle,
                properties={"Authentication Strategy": "SNOWFLAKE_SESSION_TOKEN"},
            )
            svc_id = svc.get("id") or svc.get("component", {}).get("id")
            await client.enable_root_controller_service(svc_id)
            await asyncio.sleep(3)

        provider_data = await client._get(f"parameter-providers/{provider_id}")
        prov_rev = provider_data.get("revision", {}).get("version", 0)
        await client._put(f"parameter-providers/{provider_id}", {
            "revision": {"version": prov_rev},
            "component": {
                "id": provider_id,
                "properties": {"Snowflake Connection Service": svc_id},
            },
        })
        await asyncio.sleep(2)
        return provider_id

    async def _ensure_parameter_provider(self, client) -> str:
        svc_bundle = await self._discover_bundle(client, "controller-service", "SnowflakeConnectionService")
        svc = await client.create_root_controller_service(
            name="Snowflake Connection (Secrets Provider)",
            service_type="com.snowflake.openflow.runtime.services.snowflake.SnowflakeConnectionService",
            bundle=svc_bundle,
            properties={"Authentication Strategy": "SNOWFLAKE_SESSION_TOKEN"},
        )
        svc_id = svc.get("id") or svc.get("component", {}).get("id")
        await client.enable_root_controller_service(svc_id)
        await asyncio.sleep(3)

        prov_bundle = await self._discover_bundle(client, "parameter-provider", "SnowflakeParameterProvider")
        provider = await client.create_parameter_provider(
            name="Snowflake Secrets Provider",
            provider_type="com.snowflake.openflow.runtime.parameter.snowflake.SnowflakeParameterProvider",
            bundle=prov_bundle,
            properties={"Snowflake Connection Service": svc_id},
        )
        provider_id = provider.get("id") or provider.get("component", {}).get("id")
        await asyncio.sleep(2)
        return provider_id

    async def _discover_bundle(self, client, component_type: str, name_fragment: str) -> dict:
        if component_type == "controller-service":
            data = await client._get("flow/controller-service-types")
            items = data.get("controllerServiceTypes", [])
        elif component_type == "parameter-provider":
            data = await client._get("flow/parameter-provider-types")
            items = data.get("parameterProviderTypes", [])
        else:
            items = []
        for t in items:
            if name_fragment in t.get("type", ""):
                return t["bundle"]
        raise DeployError(f"Cannot find bundle for {name_fragment} on runtime")

    async def _update_params(
        self,
        pg_id: str,
        params: dict[str, dict[str, str]],
        auto_start: bool,
        *,
        contract: Optional[dict] = None,
        manifest: Optional[dict] = None,
    ) -> bool:
        client = await self._flow._get_client()
        try:
            await client.stop_process_group(pg_id)
            await self._wait_threads_drained(client, pg_id)
            await client.disable_controller_services(pg_id)
            await asyncio.sleep(3)

            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                return False

            ctx_id = ctx_data.get("id") or ctx_data.get("component", {}).get("id")
            revision = ctx_data.get("revision", {}).get("version", 0)

            flat_params = {}
            skip_params = set()
            if manifest:
                for m in manifest.get("param_mapping", []):
                    if m.get("type") == "asset":
                        skip_params.add(m["param"])
            if contract:
                if contract.get("assets"):
                    skip_params.update(contract["assets"].keys())
                if contract.get("secrets"):
                    skip_params.update(contract["secrets"].keys())

            for ctx_name, values in params.items():
                if isinstance(values, dict):
                    for k, v in values.items():
                        if k not in skip_params:
                            flat_params[k] = v

            await client.update_parameter_context(ctx_id, revision, flat_params)
            await asyncio.sleep(2)
        finally:
            await client.close()

        try:
            if contract and contract.get("assets"):
                await self._resolve_assets(
                    pg_id,
                    manifest or {},
                    contract_assets=contract.get("assets"),
                )

            if contract and contract.get("secrets"):
                await self._resolve_secrets(pg_id, contract, manifest or {})

            if auto_start:
                client = await self._flow._get_client()
                try:
                    await self._flow._robust_start(client, pg_id)
                finally:
                    await client.close()
        except Exception as e:
            try:
                client = await self._flow._get_client()
                try:
                    await self._flow._robust_start(client, pg_id)
                finally:
                    await client.close()
            except Exception:
                pass
            raise DeployError(f"Update failed (attempted rollback restart): {e}") from e

        self._record_param_update(pg_id)
        return True

    async def _wait_threads_drained(self, client, pg_id: str, timeout: float = 30.0) -> None:
        elapsed = 0.0
        interval = 2.0
        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval
            status = await client.get_process_group_status(pg_id)
            if status.get("active_thread_count", 0) == 0:
                return
        return

    async def _replace_flow(self, pg_id: str) -> None:
        client = await self._flow._get_client()
        try:
            try:
                ctx_data = await client.get_parameter_context_by_pg(pg_id)
                ctx_id = None
                if ctx_data:
                    ctx_id = ctx_data.get("component", {}).get("id") or ctx_data.get("id")
                    ctx_rev = ctx_data.get("revision", {}).get("version", 0)
                    inherited = ctx_data.get("component", {}).get("inheritedParameterContexts", [])
                    if inherited:
                        await client._put(f"parameter-contexts/{ctx_id}", {
                            "revision": {"version": ctx_rev},
                            "id": ctx_id,
                            "component": {"id": ctx_id, "inheritedParameterContexts": []},
                        })

                await client.stop_process_group(pg_id)
                await asyncio.sleep(2)
                await client.drain_queues(pg_id)
                await client.disable_controller_services(pg_id)
                await asyncio.sleep(3)
                await client.delete_process_group(pg_id)

                if ctx_id:
                    try:
                        ctx_fresh = await client._get(f"parameter-contexts/{ctx_id}")
                        fresh_rev = ctx_fresh.get("revision", {}).get("version", 0)
                        bound = ctx_fresh.get("component", {}).get("boundProcessGroups", [])
                        if not bound:
                            await client._delete(f"parameter-contexts/{ctx_id}?version={fresh_rev}")
                    except Exception:
                        pass
            except Exception as e:
                if "404" in str(e) or "Unable to locate" in str(e):
                    pass
                else:
                    raise
        finally:
            await client.close()
        self._record_deletion(pg_id)

    async def _resolve_assets(
        self,
        pg_id: str,
        manifest: dict,
        template_dir: Optional[str] = None,
        asset_paths: Optional[dict[str, str]] = None,
        contract_assets: Optional[dict[str, str]] = None,
    ) -> dict[str, str]:
        asset_params = [m for m in manifest.get("param_mapping", []) if m.get("type") == "asset"]
        if not asset_params and not contract_assets:
            return {}

        client = await self._flow._get_client()
        try:
            ctx_data = await client.get_parameter_context_by_pg(pg_id)
            if not ctx_data:
                return {}
            ctx_id = ctx_data.get("id") or ctx_data.get("component", {}).get("id")
            if not ctx_id:
                return {}

            resolved: dict[str, str] = {}

            all_asset_params: dict[str, Optional[str]] = {}
            for m in asset_params:
                all_asset_params[m["param"]] = None
            if contract_assets:
                for param_name, file_name in contract_assets.items():
                    all_asset_params[param_name] = file_name

            for param_name, contract_file in all_asset_params.items():
                file_bytes = None
                file_name = None

                if contract_file:
                    search_paths = [ASSETS_DIR / contract_file]
                    if template_dir:
                        search_paths.insert(0, Path(template_dir) / "assets" / contract_file)
                    for sp in search_paths:
                        if sp.exists():
                            file_name = sp.name
                            file_bytes = sp.read_bytes()
                            break
                elif asset_paths and param_name in asset_paths:
                    p = Path(asset_paths[param_name])
                    if p.exists():
                        file_name = p.name
                        file_bytes = p.read_bytes()
                else:
                    dep_file = None
                    for dep in manifest.get("backend_dependencies", []):
                        if dep.endswith(".jar") or dep.endswith(".pem"):
                            dep_file = dep
                            break
                    if dep_file:
                        search_paths = []
                        if template_dir:
                            search_paths.append(Path(template_dir) / "assets" / dep_file)
                        search_paths.append(ASSETS_DIR / dep_file)
                        for sp in search_paths:
                            if sp.exists():
                                file_name = sp.name
                                file_bytes = sp.read_bytes()
                                break

                asset_id = None
                if file_bytes and file_name:
                    asset = await client.ensure_asset(ctx_id, file_name, file_bytes)
                    asset_id = asset.get("id")
                else:
                    existing = await client.list_assets(ctx_id)
                    for a in existing:
                        if contract_file and contract_file in (a.get("name") or ""):
                            asset_id = a.get("id")
                            break

                if asset_id:
                    await client.link_asset_to_parameter(ctx_id, param_name, asset_id)
                    resolved[param_name] = asset_id

            return resolved
        finally:
            await client.close()

    def _get_last_deployment(self, runtime_name: str) -> Optional[dict]:
        try:
            row = self._sf.query_one(
                "SELECT contract_sha, template_id, template_version, process_group_id "
                "FROM OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "WHERE runtime_name = %s AND action = 'DEPLOY' ORDER BY deployed_at DESC LIMIT 1",
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
                "(runtime_name, contract_paths, template_id, template_version, contract_sha, action, process_group_id) "
                "SELECT %s, PARSE_JSON(%s), %s, %s, %s, 'DEPLOY', %s",
                [runtime_name, json.dumps(contract_paths), template_id, template_version, sha, process_group_id],
            )
        except Exception:
            pass

    def _record_param_update(self, process_group_id: str, contract_path: Optional[str] = None):
        try:
            self._sf.execute(
                "INSERT INTO OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "(runtime_name, contract_paths, template_id, template_version, contract_sha, action, process_group_id) "
                "SELECT runtime_name, contract_paths, template_id, template_version, NULL, 'PARAM_UPDATE', %s "
                "FROM OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "WHERE process_group_id = %s AND action = 'DEPLOY' ORDER BY deployed_at DESC LIMIT 1",
                [process_group_id, process_group_id],
            )
        except Exception:
            pass

    def _record_deletion(self, process_group_id: str):
        try:
            self._sf.execute(
                "INSERT INTO OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "(runtime_name, contract_paths, template_id, template_version, contract_sha, action, process_group_id) "
                "SELECT runtime_name, contract_paths, template_id, template_version, NULL, 'DELETED', %s "
                "FROM OPENFLOW_FACTORY.METADATA.DEPLOYMENT_LOG "
                "WHERE process_group_id = %s AND action = 'DEPLOY' ORDER BY deployed_at DESC LIMIT 1",
                [process_group_id, process_group_id],
            )
        except Exception:
            pass

    def close(self):
        self._runtime.close()
        if self._flow:
            self._flow.close()
        self._target.close()
        self._sf.close()
