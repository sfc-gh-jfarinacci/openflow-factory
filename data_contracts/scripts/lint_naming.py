import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {".github", "schema", "scripts", ".git"}

VALID_SGDBS = {"postgres", "mysql", "sqlserver", "mongodb", "kafka", "s3"}
VALID_STRATEGIES = {"full", "cdc", "incremental"}
FILE_PATTERN = re.compile(r"^([a-z0-9]+)_(full|cdc|incremental)(_[a-z0-9]+)?$")


def main():
    errors = []

    for yaml_file in REPO_ROOT.rglob("*.yaml"):
        rel = yaml_file.relative_to(REPO_ROOT)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue

        parts = rel.parts
        if len(parts) < 3:
            errors.append(f"{rel}: must be under <domain>/<database>/<file>.yaml")
            continue

        domain = parts[0]
        database = parts[1]
        stem = yaml_file.stem

        if not re.match(r"^[a-z][a-z0-9_]*$", domain):
            errors.append(f"{rel}: domain folder '{domain}' must be lowercase alphanumeric")

        if not re.match(r"^[a-z][a-z0-9_]*$", database):
            errors.append(f"{rel}: database folder '{database}' must be lowercase alphanumeric")

        m = FILE_PATTERN.match(stem)
        if not m:
            errors.append(f"{rel}: filename '{stem}' must match <sgdb>_<strategy>[_<variant>]")
            continue

        file_sgdb = m.group(1)
        file_strategy = m.group(2)

        doc = yaml.safe_load(yaml_file.read_text())
        if doc.get("source_sgdb") != file_sgdb:
            errors.append(f"{rel}: source_sgdb '{doc.get('source_sgdb')}' doesn't match filename sgdb '{file_sgdb}'")
        if doc.get("type") != file_strategy:
            errors.append(f"{rel}: type '{doc.get('type')}' doesn't match filename strategy '{file_strategy}'")

    if errors:
        print(f"FAILED: {len(errors)} naming issue(s)\n")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("OK: all naming conventions pass")


if __name__ == "__main__":
    main()
