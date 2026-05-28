from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings


class EngineConfig(BaseSettings):
    snowflake_account: str
    snowflake_user: str
    snowflake_role: str = "OPENFLOW_ADMIN"
    snowflake_warehouse: str = "OPENFLOW_FACTORY_WH"
    snowflake_database: str = "OPENFLOW_FACTORY"
    snowflake_schema: str = "METADATA"
    snowflake_private_key_path: Optional[Path] = None
    snowflake_private_key_passphrase: Optional[str] = None
    snowflake_host: Optional[str] = None
    snowflake_oauth_token_path: Optional[Path] = None
    templates_stage: str = "OPENFLOW_FACTORY.METADATA.TEMPLATES_STAGE"

    model_config = {"env_prefix": "", "env_file": ".env", "extra": "ignore"}

    @property
    def is_spcs(self) -> bool:
        return bool(self.snowflake_host) and (
            self.snowflake_oauth_token_path is not None
            and self.snowflake_oauth_token_path.exists()
        )
