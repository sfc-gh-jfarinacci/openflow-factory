from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

TEMPLATES_ROOT = Path(__file__).resolve().parent


def load_template(template_id: str, version: Optional[str] = None) -> dict:
    if version is None:
        stable_path = TEMPLATES_ROOT / "STABLE.yaml"
        if stable_path.exists():
            stable = yaml.safe_load(stable_path.read_text())
            version = stable.get(template_id, "v1")
        else:
            version = "v1"

    template_dir = TEMPLATES_ROOT / template_id / version
    if not template_dir.exists():
        raise FileNotFoundError(f"Template not found: {template_dir}")

    manifest_path = template_dir / "manifest.yaml"
    flow_path = template_dir / "flow.json"

    import json
    manifest = yaml.safe_load(manifest_path.read_text()) if manifest_path.exists() else {}
    flow = json.loads(flow_path.read_text()) if flow_path.exists() else {}

    return {
        "template_id": template_id,
        "version": version,
        "manifest": manifest,
        "flow": flow,
        "template_dir": str(template_dir),
    }
