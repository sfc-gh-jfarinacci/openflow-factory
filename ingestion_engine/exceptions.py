from __future__ import annotations


class IngestionEngineError(Exception):
    pass


class ValidationError(IngestionEngineError):
    def __init__(self, checks: list[dict]):
        self.checks = checks
        failed = [c for c in checks if not c.get("ok")]
        msg = f"{len(failed)} validation(s) failed: " + "; ".join(c.get("message", "") for c in failed)
        super().__init__(msg)


class DeployError(IngestionEngineError):
    pass


class RuntimeNotFoundError(IngestionEngineError):
    pass


class TemplateNotFoundError(IngestionEngineError):
    pass


class NiFiClientError(IngestionEngineError):
    def __init__(self, method: str, url: str, status: int, body: str = ""):
        self.method = method
        self.url = url
        self.status = status
        self.body = body
        super().__init__(f"{method} {url} → {status}: {body[:200]}")


class StartError(IngestionEngineError):
    def __init__(self, message: str, bulletins: list[dict] | None = None, status: dict | None = None):
        self.bulletins = bulletins or []
        self.status = status or {}
        super().__init__(message)


class SnowflakeConnectionError(IngestionEngineError):
    pass
