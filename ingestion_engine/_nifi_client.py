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
        data = await self._get(f"flow/process-groups/{pg_id}/status")
        snap = data.get("processGroupStatus", {}).get("aggregateSnapshot", {})
        return {
            "id": pg_id,
            "name": snap.get("name"),
            "running_count": snap.get("runningCount", 0),
            "stopped_count": snap.get("stoppedCount", 0),
            "invalid_count": snap.get("invalidCount", 0),
            "active_thread_count": snap.get("activeThreadCount", 0),
            "queued_count": snap.get("flowFilesQueued", 0),
            "bytes_in": snap.get("bytesIn", 0),
            "bytes_out": snap.get("bytesOut", 0),
        }

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

    async def update_parameter_context(self, context_id: str, revision_version: int, parameters: dict[str, str]) -> dict:
        existing = await self._get(f"parameter-contexts/{context_id}")
        existing_params = existing.get("component", {}).get("parameters", [])

        updated_params = []
        for p in existing_params:
            param_obj = p.get("parameter", {})
            name = param_obj.get("name", "")
            if name in parameters:
                updated_params.append({
                    "parameter": {
                        "name": name,
                        "value": parameters[name],
                        "sensitive": param_obj.get("sensitive", False),
                    }
                })
            else:
                updated_params.append(p)

        return await self._put(f"parameter-contexts/{context_id}", {
            "revision": {"version": revision_version},
            "id": context_id,
            "component": {
                "id": context_id,
                "parameters": updated_params,
            },
        })
