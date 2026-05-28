import json
import sys
from pathlib import Path

import jsonschema
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "schema" / "contract.v1.schema.json"
SKIP_DIRS = {".github", "schema", "scripts", ".git"}


def main():
    schema = json.loads(SCHEMA_PATH.read_text())
    errors = []

    for yaml_file in REPO_ROOT.rglob("*.yaml"):
        rel = yaml_file.relative_to(REPO_ROOT)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue

        try:
            doc = yaml.safe_load(yaml_file.read_text())
        except yaml.YAMLError as e:
            errors.append(f"{rel}: YAML parse error: {e}")
            continue

        try:
            jsonschema.validate(doc, schema)
        except jsonschema.ValidationError as e:
            errors.append(f"{rel}: {e.message}")

    if errors:
        print(f"FAILED: {len(errors)} contract(s) invalid\n")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        count = sum(1 for _ in REPO_ROOT.rglob("*.yaml") if not any(p in SKIP_DIRS for p in _.relative_to(REPO_ROOT).parts))
        print(f"OK: {count} contract(s) validated successfully")


if __name__ == "__main__":
    main()
