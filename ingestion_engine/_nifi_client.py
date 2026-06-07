from __future__ import annotations

from typing import Any, Optional

import httpx

from ingestion_engine.exceptions import NiFiClientError


class NiFiClient:
    def __init__(self, base_url: str, auth_token: str, timeout: float = 30.0):
        url = base_url.rstrip("/")
        if not url.endswith("/nifi-api"):
            url = f"{url}/nifi-api"
        self._base_url = url
        self._client = httpx.AsyncClient(
            base_url=url,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {auth_token}",
            },
            timeout=timeout,
        )

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def _get(self, path: str) -> Any:
        r = await self._client.get(path)
        if r.status_code >= 400:
            raise NiFiClientError("GET", f"{self._base_url}/{path}", r.status_code, r.text)
        return r.json()

    async def _post(self, path: str, body: Any = None) -> Any:
        r = await self._client.post(path, json=body)
        if r.status_code >= 400:
            raise NiFiClientError("POST", f"{self._base_url}/{path}", r.status_code, r.text)
        return r.json()

    async def _put(self, path: str, body: Any = None) -> Any:
        r = await self._client.put(path, json=body)
        if r.status_code >= 400:
            raise NiFiClientError("PUT", f"{self._base_url}/{path}", r.status_code, r.text)
        return r.json()

    async def _delete(self, path: str) -> None:
        r = await self._client.delete(path)
        if r.status_code >= 400:
            raise NiFiClientError("DELETE", f"{self._base_url}/{path}", r.status_code, r.text)

    async def get_root_pg_id(self) -> str:
        data = await self._get("flow/process-groups/root")
        return data["processGroupFlow"]["id"]

    async def list_process_groups(self, parent_id: str) -> list[dict]:
        data = await self._get(f"flow/process-groups/{parent_id}")
        pgs = data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
        return [
            {
                "id": pg["id"],
                "name": pg.get("component", {}).get("name"),
                "status": pg.get("status"),
                "running_count": pg.get("runningCount"),
                "stopped_count": pg.get("stoppedCount"),
                "invalid_count": pg.get("invalidCount"),
            }
            for pg in pgs
        ]

    async def get_process_group_status(self, pg_id: str) -> dict:
        data = await self._get(f"process-groups/{pg_id}")
        return {
            "id": pg_id,
            "name": data.get("component", {}).get("name"),
            "running_count": data.get("runningCount", 0),
            "stopped_count": data.get("stoppedCount", 0),
            "invalid_count": data.get("invalidCount", 0),
            "disabled_count": data.get("disabledCount", 0),
            "active_thread_count": data.get("status", {}).get("aggregateSnapshot", {}).get("activeThreadCount", 0),
            "queued_count": data.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0),
        }

    async def get_controller_services_status(self, pg_id: str) -> list[dict]:
        data = await self._get(
            f"flow/process-groups/{pg_id}/controller-services?includeAncestorGroups=false&includeDescendantGroups=true"
        )
        svcs = data.get("controllerServices", [])
        return [
            {
                "id": s.get("id"),
                "name": s.get("component", {}).get("name"),
                "state": s.get("component", {}).get("state"),
                "validation_status": s.get("component", {}).get("validationStatus"),
                "validation_errors": s.get("component", {}).get("validationErrors", []),
            }
            for s in svcs
        ]

    async def get_bulletins(self, pg_id: str) -> list[dict]:
        data = await self._get(f"flow/process-groups/{pg_id}?uiOnly=true")
        pgs = data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
        bulletins = []
        for pg in pgs:
            for b in pg.get("bulletins", []):
                bulletin = b.get("bulletin", {})
                bulletins.append({
                    "level": bulletin.get("level", ""),
                    "message": bulletin.get("message", ""),
                    "source_name": bulletin.get("sourceName", ""),
                    "timestamp": bulletin.get("timestamp", ""),
                })
        root_bulletins = data.get("processGroupFlow", {}).get("flow", {}).get("bulletins", [])
        for b in root_bulletins:
            bulletin = b.get("bulletin", {})
            bulletins.append({
                "level": bulletin.get("level", ""),
                "message": bulletin.get("message", ""),
                "source_name": bulletin.get("sourceName", ""),
                "timestamp": bulletin.get("timestamp", ""),
            })
        return bulletins

    async def start_process_group(self, pg_id: str) -> None:
        await self._put(f"flow/process-groups/{pg_id}", {"id": pg_id, "state": "RUNNING"})

    async def stop_process_group(self, pg_id: str) -> None:
        await self._put(f"flow/process-groups/{pg_id}", {"id": pg_id, "state": "STOPPED"})

    async def import_flow_definition(self, parent_id: str, name: str, versioned_flow_snapshot: dict) -> dict:
        if "flowContents" in versioned_flow_snapshot:
            versioned_flow_snapshot["flowContents"]["name"] = name
        r = await self._client.post(
            f"process-groups/{parent_id}/process-groups/import",
            json=versioned_flow_snapshot,
        )
        if r.status_code >= 400:
            raise NiFiClientError("POST", f"{self._base_url}/process-groups/{parent_id}/process-groups/import", r.status_code, r.text)
        return r.json()

    async def upload_flow_definition(self, parent_id: str, name: str, versioned_flow_snapshot: dict) -> dict:
        import json as _json
        flow_bytes = _json.dumps(versioned_flow_snapshot).encode("utf-8")
        upload_client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": self._client.headers["Authorization"],
                "Accept": "application/json",
            },
            timeout=self._client.timeout,
        )
        try:
            r = await upload_client.post(
                f"process-groups/{parent_id}/process-groups/upload",
                data={"groupName": name, "positionX": "0", "positionY": "0", "clientId": "ingestion-engine"},
                files={"file": (f"{name}.json", flow_bytes, "application/json")},
            )
            if r.status_code >= 400:
                raise NiFiClientError("POST", f"{self._base_url}/process-groups/{parent_id}/process-groups/upload", r.status_code, r.text)
            return r.json()
        finally:
            await upload_client.aclose()

    async def enable_controller_services(self, pg_id: str) -> None:
        await self._put(f"flow/process-groups/{pg_id}/controller-services", {
            "id": pg_id,
            "state": "ENABLED",
        })

    async def disable_controller_services(self, pg_id: str) -> None:
        await self._put(f"flow/process-groups/{pg_id}/controller-services", {
            "id": pg_id,
            "state": "DISABLED",
        })

    async def list_controller_services(self, pg_id: str) -> list[dict]:
        data = await self._get(
            f"flow/process-groups/{pg_id}/controller-services?includeAncestorGroups=false&includeDescendantGroups=true"
        )
        return data.get("controllerServices", [])

    async def create_parameter_context(
        self,
        name: str,
        parameters: list[dict],
        inherited_context_ids: Optional[list[str]] = None,
    ) -> dict:
        return await self._post("parameter-contexts", {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "parameters": [
                    {"parameter": {"name": p["name"], "value": p.get("value"), "sensitive": p.get("sensitive", False)}}
                    for p in parameters
                ],
                "inheritedParameterContexts": [{"id": cid} for cid in (inherited_context_ids or [])],
            },
        })

    async def link_parameter_context(self, pg_id: str, context_id: str) -> dict:
        pg = await self._get(f"process-groups/{pg_id}")
        revision = pg.get("revision", {}).get("version", 0)
        return await self._put(f"process-groups/{pg_id}", {
            "revision": {"version": revision},
            "id": pg_id,
            "component": {
                "id": pg_id,
                "parameterContext": {"id": context_id},
            },
        })

    async def ensure_parameter_context(self, name: str, parameters: list[dict]) -> dict:
        existing = await self.get_parameter_contexts()
        for ctx in existing:
            if ctx.get("component", {}).get("name") == name:
                return ctx.get("component", {})
        result = await self.create_parameter_context(name, parameters)
        return result.get("component", result)

    async def delete_process_group(self, pg_id: str) -> None:
        pg = await self._get(f"process-groups/{pg_id}")
        version = pg["revision"]["version"]
        await self._delete(f"process-groups/{pg_id}?version={version}")

    async def list_connections(self, pg_id: str) -> list[dict]:
        data = await self._get(f"process-groups/{pg_id}/connections")
        return data.get("connections", [])

    async def drop_queue(self, connection_id: str) -> None:
        drop = await self._post(f"flowfile-queues/{connection_id}/drop-requests")
        req_id = drop.get("dropRequest", {}).get("id")
        if not req_id:
            return
        import asyncio
        for _ in range(30):
            status = await self._get(f"flowfile-queues/{connection_id}/drop-requests/{req_id}")
            if status.get("dropRequest", {}).get("finished"):
                await self._delete(f"flowfile-queues/{connection_id}/drop-requests/{req_id}")
                return
            await asyncio.sleep(1)

    async def drain_queues(self, pg_id: str) -> None:
        connections = await self.list_connections(pg_id)
        for conn in connections:
            conn_id = conn.get("id")
            if conn_id:
                await self.drop_queue(conn_id)

    async def get_parameter_contexts(self) -> list[dict]:
        data = await self._get("flow/parameter-contexts")
        return data.get("parameterContexts", [])

    async def get_parameter_context_by_pg(self, pg_id: str) -> Optional[dict]:
        pg = await self._get(f"process-groups/{pg_id}")
        ctx_ref = pg.get("component", {}).get("parameterContext")
        if not ctx_ref:
            return None
        ctx_id = ctx_ref.get("id")
        if not ctx_id:
            return None
        return await self._get(f"parameter-contexts/{ctx_id}")

    async def list_assets(self, context_id: str) -> list[dict]:
        data = await self._get(f"parameter-contexts/{context_id}/assets")
        raw = data.get("assets", [])
        return [a.get("asset", a) if "asset" in a else a for a in raw]

    async def upload_asset(self, context_id: str, file_name: str, file_bytes: bytes) -> dict:
        upload_client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": self._client.headers["Authorization"],
            },
            timeout=self._client.timeout,
        )
        try:
            r = await upload_client.post(
                f"parameter-contexts/{context_id}/assets",
                headers={
                    "Content-Type": "application/octet-stream",
                    "Filename": file_name,
                },
                content=file_bytes,
            )
            if r.status_code >= 400:
                raise NiFiClientError("POST", f"{self._base_url}/parameter-contexts/{context_id}/assets", r.status_code, r.text)
            return r.json().get("asset", r.json())
        finally:
            await upload_client.aclose()

    async def get_asset(self, context_id: str, asset_id: str) -> dict:
        return await self._get(f"parameter-contexts/{context_id}/assets/{asset_id}")

    async def delete_asset(self, context_id: str, asset_id: str) -> None:
        await self._delete(f"parameter-contexts/{context_id}/assets/{asset_id}")

    async def ensure_asset(self, context_id: str, file_name: str, file_bytes: bytes) -> dict:
        existing = await self.list_assets(context_id)
        for asset in existing:
            if asset.get("name") == file_name:
                return asset
        return await self.upload_asset(context_id, file_name, file_bytes)

    async def link_asset_to_parameter(self, context_id: str, param_name: str, asset_id: str) -> dict:
        ctx = await self._get(f"parameter-contexts/{context_id}")
        revision = ctx.get("revision", {}).get("version", 0)
        inherited = ctx.get("component", {}).get("inheritedParameterContexts", [])
        return await self._put(f"parameter-contexts/{context_id}", {
            "revision": {"version": revision},
            "id": context_id,
            "component": {
                "id": context_id,
                "inheritedParameterContexts": inherited,
                "parameters": [
                    {
                        "parameter": {
                            "name": param_name,
                            "value": None,
                            "referencedAssets": [{"id": asset_id}],
                        }
                    }
                ],
            },
        })

    async def list_parameter_providers(self) -> list[dict]:
        data = await self._get("flow/parameter-providers")
        providers = data.get("parameterProviders", [])
        return [
            {
                "id": p.get("id"),
                "name": p.get("component", {}).get("name"),
                "type": p.get("component", {}).get("type"),
                "validation_status": p.get("component", {}).get("validationStatus"),
                "properties": p.get("component", {}).get("properties", {}),
                "parameter_group_configurations": p.get("component", {}).get("parameterGroupConfigurations", []),
            }
            for p in providers
        ]

    async def get_parameter_provider_types(self) -> list[dict]:
        data = await self._get("flow/parameter-provider-types")
        return data.get("parameterProviderTypes", [])

    async def create_parameter_provider(
        self,
        name: str,
        provider_type: str,
        bundle: dict,
        properties: Optional[dict] = None,
    ) -> dict:
        body = {
            "revision": {"version": 0},
            "component": {
                "type": provider_type,
                "bundle": bundle,
                "name": name,
            },
        }
        if properties:
            body["component"]["properties"] = properties
        return await self._post("controller/parameter-providers", body)

    async def update_parameter_provider(
        self, provider_id: str, revision: int, properties: dict
    ) -> dict:
        return await self._put(f"parameter-providers/{provider_id}", {
            "revision": {"version": revision},
            "component": {"id": provider_id, "properties": properties},
        })

    async def fetch_parameters(self, provider_id: str) -> dict:
        provider = await self._get(f"parameter-providers/{provider_id}")
        revision = provider.get("revision", {}).get("version", 0)
        return await self._post(f"parameter-providers/{provider_id}/parameters/fetch-requests", {
            "id": provider_id,
            "revision": {"version": revision},
        })

    async def get_fetch_request(self, provider_id: str, request_id: str) -> dict:
        return await self._get(f"parameter-providers/{provider_id}/parameters/fetch-requests/{request_id}")

    async def apply_fetched_parameters(
        self,
        provider_id: str,
        parameter_group_configurations: list[dict],
    ) -> dict:
        provider = await self._get(f"parameter-providers/{provider_id}")
        revision = provider.get("revision", {}).get("version", 0)
        result = await self._post(f"parameter-providers/{provider_id}/apply-parameters-requests", {
            "revision": {"version": revision},
            "id": provider_id,
            "parameterGroupConfigurations": parameter_group_configurations,
        })
        req = result.get("request", result)
        req_id = req.get("requestId", req.get("id", ""))
        if req_id and not req.get("complete"):
            import asyncio as _asyncio
            for _ in range(30):
                await _asyncio.sleep(2)
                poll = await self._get(f"parameter-providers/{provider_id}/apply-parameters-requests/{req_id}")
                poll_req = poll.get("request", poll)
                if poll_req.get("complete"):
                    return poll_req
        return req

    async def wait_for_fetch(self, provider_id: str, request_id: str, timeout: float = 30.0) -> dict:
        import asyncio as _asyncio
        elapsed = 0.0
        while elapsed < timeout:
            result = await self.get_fetch_request(provider_id, request_id)
            req = result.get("parameterProviderParameterFetchRequest", result)
            if req.get("complete"):
                return req
            await _asyncio.sleep(1.0)
            elapsed += 1.0
        return result

    async def create_root_controller_service(
        self,
        name: str,
        service_type: str,
        bundle: dict,
        properties: Optional[dict] = None,
    ) -> dict:
        body = {
            "revision": {"version": 0},
            "component": {
                "type": service_type,
                "bundle": bundle,
                "name": name,
            },
        }
        if properties:
            body["component"]["properties"] = properties
        return await self._post("controller/controller-services", body)

    async def enable_root_controller_service(self, service_id: str) -> dict:
        svc = await self._get(f"controller-services/{service_id}")
        revision = svc.get("revision", {}).get("version", 0)
        return await self._put(f"controller-services/{service_id}/run-status", {
            "revision": {"version": revision},
            "state": "ENABLED",
        })

    async def get_root_controller_services(self) -> list[dict]:
        data = await self._get("flow/controller/controller-services")
        return data.get("controllerServices", [])

    async def inherit_parameter_context(self, ctx_id: str, inherited_ctx_id: str) -> dict:
        ctx = await self._get(f"parameter-contexts/{ctx_id}")
        revision = ctx.get("revision", {}).get("version", 0)
        inherited = ctx.get("component", {}).get("inheritedParameterContexts", [])
        already = any(
            (h.get("id") or h.get("component", {}).get("id")) == inherited_ctx_id
            for h in inherited
        )
        if already:
            return ctx
        inherited.append({"component": {"id": inherited_ctx_id}})
        return await self._put(f"parameter-contexts/{ctx_id}", {
            "revision": {"version": revision},
            "id": ctx_id,
            "component": {
                "id": ctx_id,
                "inheritedParameterContexts": inherited,
            },
        })

    async def update_controller_service_properties(self, service_id: str, properties: dict[str, str]) -> dict:
        svc = await self._get(f"controller-services/{service_id}")
        revision = svc.get("revision", {}).get("version", 0)
        return await self._put(f"controller-services/{service_id}", {
            "revision": {"version": revision},
            "component": {"id": service_id, "properties": properties},
        })

    async def find_services_with_sensitive_property(self, pg_id: str, property_keyword: str) -> list[dict]:
        svcs = await self.list_controller_services(pg_id)
        results = []
        for s in svcs:
            comp = s.get("component", {})
            descs = comp.get("descriptors", {})
            for prop_key, desc in descs.items():
                if desc.get("sensitive") and property_keyword.lower() in prop_key.lower():
                    results.append({"id": comp["id"], "name": comp.get("name"), "property": prop_key})
        return results

    async def update_parameter_context(self, context_id: str, revision_version: int, parameters: dict[str, str]) -> dict:
        existing = await self._get(f"parameter-contexts/{context_id}")
        existing_params = existing.get("component", {}).get("parameters", [])

        updated_params = []
        matched_keys = set()
        for p in existing_params:
            param_obj = p.get("parameter", {})
            name = param_obj.get("name", "")
            if param_obj.get("referencedAssets"):
                continue
            if name in parameters:
                matched_keys.add(name)
                updated_params.append({
                    "parameter": {
                        "name": name,
                        "value": parameters[name],
                        "sensitive": param_obj.get("sensitive", False),
                    }
                })
            else:
                updated_params.append(p)

        unmatched = set(parameters.keys()) - matched_keys
        if unmatched:
            inherited = existing.get("component", {}).get("inheritedParameterContexts", [])
            for inh_ref in inherited:
                inh_id = inh_ref.get("id") or inh_ref.get("component", {}).get("id")
                if not inh_id or not unmatched:
                    break
                inh_ctx = await self._get(f"parameter-contexts/{inh_id}")
                inh_revision = inh_ctx.get("revision", {}).get("version", 0)
                inh_params = inh_ctx.get("component", {}).get("parameters", [])

                inh_updates = []
                inh_matched = set()
                for p in inh_params:
                    param_obj = p.get("parameter", {})
                    name = param_obj.get("name", "")
                    if name in unmatched:
                        inh_matched.add(name)
                        inh_updates.append({
                            "parameter": {
                                "name": name,
                                "value": parameters[name],
                                "sensitive": param_obj.get("sensitive", False),
                            }
                        })
                    else:
                        inh_updates.append(p)

                if inh_matched:
                    await self._put(f"parameter-contexts/{inh_id}", {
                        "revision": {"version": inh_revision},
                        "id": inh_id,
                        "component": {
                            "id": inh_id,
                            "parameters": inh_updates,
                        },
                    })
                    unmatched -= inh_matched

        return await self._put(f"parameter-contexts/{context_id}", {
            "revision": {"version": revision_version},
            "id": context_id,
            "component": {
                "id": context_id,
                "parameters": updated_params,
            },
        })
