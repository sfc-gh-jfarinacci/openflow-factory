from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ingestion_engine._nifi_client import NiFiClient
from ingestion_engine.config import EngineConfig
from ingestion_engine.exceptions import DeployError, TemplateNotFoundError
from ingestion_engine.runtime.runtime import Runtime

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


@dataclass
class FlowDeployResult:
    process_group_id: str
    process_group_name: str
    import_method: str
    strategy: str
    runtime_node_map: dict = field(default_factory=dict)
    started: bool = False


@dataclass
class FlowStatus:
    process_group_id: str
    state: str
    running_count: int = 0
    stopped_count: int = 0
    invalid_count: int = 0
    queued_count: int = 0


class Flow:
    def __init__(self, config: EngineConfig, runtime_name: str):
        self._config = config
        self._runtime_name = runtime_name
        self._runtime = Runtime(config)

    async def _get_client(self) -> NiFiClient:
        _, client = await self._runtime.resolve(self._runtime_name)
        return client

    def validate_template(self, template: str) -> dict:
        spec = self._load_template_spec(template)
        checks = []
        checks.append({"check": "template_exists", "ok": True, "template": template})

        has_canonical = "canonicalFlow" in spec
        has_params = "parameterContexts" in spec
        checks.append({"check": "has_flow_definition", "ok": has_canonical or has_params})

        return {"ok": all(c["ok"] for c in checks), "checks": checks}

    async def deploy_from_template(
        self,
        template: str,
        name: str,
        params: dict[str, dict[str, str]],
        *,
        connector_id: Optional[str] = None,
        auto_start: bool = False,
    ) -> FlowDeployResult:
        spec = self._load_template_spec(template)
        strategy = spec.get("id", template).split("_")[-1] if "_" in spec.get("id", template) else "cdc"

        secret_fqn = self._resolve_secret_fqn(connector_id) if connector_id else None
        substituted_params = self._substitute_secrets(spec, params, secret_fqn)

        client = await self._get_client()
        try:
            root_id = await client.get_root_pg_id()

            if "canonicalFlow" in spec:
                flow = json.loads(json.dumps(spec["canonicalFlow"]))
            elif "flowContents" in spec:
                flow = json.loads(json.dumps(spec))
            else:
                raise DeployError(
                    f"Template '{template}' has no canonicalFlow or flowContents. "
                    f"This template may require programmatic build (not yet supported in Python SDK). "
                    f"Available top-level keys: {list(spec.keys())[:10]}"
                )
            if flow.get("flowContents"):
                flow["flowContents"]["name"] = name

            self._inject_params(flow, substituted_params, spec)

            pg_id, import_method = await self._import_flow(client, root_id, name, flow)

            await self._ensure_param_context_linked(client, pg_id, flow, substituted_params)

            runtime_node_map = self._build_node_map(flow)

            started = False
            if auto_start:
                try:
                    await client.enable_controller_services(pg_id)
                    await asyncio.sleep(1.5)
                    await client.start_process_group(pg_id)
                    started = True
                except Exception:
                    pass

            return FlowDeployResult(
                process_group_id=pg_id,
                process_group_name=name,
                import_method=import_method,
                strategy=strategy,
                runtime_node_map=runtime_node_map,
                started=started,
            )
        finally:
            await client.close()

    async def trigger(self, process_group_id: str) -> None:
        client = await self._get_client()
        try:
            await client.start_process_group(process_group_id)
        finally:
            await client.close()

    async def status(self, process_group_id: str) -> FlowStatus:
        client = await self._get_client()
        try:
            data = await client.get_process_group_status(process_group_id)
            running = data.get("running_count", 0)
            stopped = data.get("stopped_count", 0)
            invalid = data.get("invalid_count", 0)

            if invalid > 0:
                state = "invalid"
            elif running > 0 and stopped == 0:
                state = "running"
            elif stopped > 0 and running == 0:
                state = "stopped"
            elif running > 0 and stopped > 0:
                state = "partially_running"
            else:
                state = "unknown"

            return FlowStatus(
                process_group_id=process_group_id,
                state=state,
                running_count=running,
                stopped_count=stopped,
                invalid_count=invalid,
                queued_count=data.get("queued_count", 0),
            )
        finally:
            await client.close()

    def _load_template_spec(self, template: str) -> dict:
        from ingestion_engine.templates.loader import load_template as _load
        template_id = template.replace("template_", "").replace("postgresql_", "postgres_")
        try:
            tpl = _load(template_id)
            return tpl["flow"]
        except FileNotFoundError:
            pass
        candidates = [
            TEMPLATES_DIR / template_id / "v1" / "flow.json",
            TEMPLATES_DIR / f"{template}.json",
        ]
        for p in candidates:
            if p.exists():
                return json.loads(p.read_text())
        raise TemplateNotFoundError(f"Template '{template}' not found. Searched: {template_id}/v1/flow.json")

    def _resolve_secret_fqn(self, connector_id: str) -> Optional[str]:
        import re
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", connector_id).upper()
        if not sanitized[0:1].isalpha() and sanitized[0:1] != "_":
            sanitized = f"C_{sanitized}"
        return f"OPENFLOW_FACTORY.SECRETS.{sanitized}_CREDS"

    def _substitute_secrets(
        self, spec: dict, params: dict[str, dict[str, str]], secret_fqn: Optional[str]
    ) -> dict[str, dict[str, str]]:
        if not secret_fqn:
            return params
        secret_keys: set[str] = set()
        for ctx in spec.get("parameterContexts", []):
            for p in ctx.get("params", []):
                if p.get("type") == "secret":
                    secret_keys.add(p["key"])
        out = json.loads(json.dumps(params))
        for ctx_id in out:
            for k in out[ctx_id]:
                if k in secret_keys:
                    out[ctx_id][k] = f"${{secret('{secret_fqn}', '{k}')}}"
        return out

    def _inject_params(self, flow: dict, params: dict[str, dict[str, str]], spec: dict):
        param_contexts = flow.get("parameterContexts") or {}

        for ctx_name, values in params.items():
            if not values or not isinstance(values, dict):
                continue
            ctx = param_contexts.get(ctx_name)
            if not ctx:
                continue
            ctx["parameters"] = [
                {**p, "value": values[p["name"]]} if p["name"] in values else p
                for p in (ctx.get("parameters") or [])
            ]

    async def _import_flow(self, client: NiFiClient, root_id: str, name: str, flow: dict) -> tuple[str, str]:
        try:
            created = await client.import_flow_definition(root_id, name, flow)
            pg_id = created.get("id") or created.get("component", {}).get("id")
            if not pg_id:
                raise DeployError("NiFi response did not contain a process group id")
            return pg_id, "import"
        except Exception as import_err:
            try:
                created = await client.upload_flow_definition(root_id, name, flow)
                pg_id = created.get("id") or created.get("component", {}).get("id")
                if not pg_id:
                    raise DeployError("NiFi response did not contain a process group id")
                return pg_id, "upload"
            except Exception as upload_err:
                raise DeployError(f"Both /import and /upload failed.\n  /import: {import_err}\n  /upload: {upload_err}")

    async def _ensure_param_context_linked(
        self,
        client: NiFiClient,
        pg_id: str,
        flow: dict,
        params: dict[str, dict[str, str]],
    ) -> Optional[str]:
        existing = await client.get_parameter_context_by_pg(pg_id)
        if existing:
            ctx_id = existing.get("id") or existing.get("component", {}).get("id")
            return ctx_id

        param_contexts = flow.get("parameterContexts") or {}
        if not param_contexts:
            return None

        ctx_name = next(iter(param_contexts))
        ctx_def = param_contexts[ctx_name]
        raw_params = ctx_def.get("parameters", [])

        param_values = params.get(ctx_name, {})
        nifi_params = []
        for p in raw_params:
            name_val = p.get("name", "")
            value = param_values.get(name_val, p.get("value", ""))
            nifi_params.append({
                "name": name_val,
                "value": value,
                "sensitive": p.get("sensitive", False),
            })

        ctx = await client.ensure_parameter_context(ctx_name, nifi_params)
        ctx_id = ctx.get("id", "")
        if ctx_id:
            await client.link_parameter_context(pg_id, ctx_id)
        return ctx_id

    def _build_node_map(self, flow: dict) -> dict:
        node_map: dict[str, dict] = {}

        def walk(node: dict, kind: str, parent_id: Optional[str] = None):
            tpl_id = node.get("identifier") or node.get("component", {}).get("identifier")
            rt_id = node.get("id") or node.get("component", {}).get("id")
            nm = node.get("component", {}).get("name") or node.get("name")
            if tpl_id and rt_id:
                node_map[tpl_id] = {"runtimeId": rt_id, "kind": kind, "name": nm, "parentRuntimeId": parent_id}
            contents = node.get("flowContents") or node.get("component", {}).get("flowSnapshot", {}).get("flowContents")
            if contents:
                for pg in contents.get("processGroups", []):
                    walk(pg, "pg", rt_id)
                for proc in contents.get("processors", []):
                    walk(proc, "processor", rt_id)
                for conn in contents.get("connections", []):
                    walk(conn, "connection", rt_id)

        walk(flow, "pg")
        return node_map

    def close(self):
        self._runtime.close()
