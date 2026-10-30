#!/usr/bin/env python3
"""Reject non-synthetic handles in recorded JSON fixtures."""

import argparse
import json
from pathlib import Path
from typing import Any

SYNTHETIC_SUFFIX = ".example.test"


def find_handles(value: Any, location: str = "$"):
    if isinstance(value, dict):
        for key, child in value.items():
            child_location = f"{location}.{key}"
            if key == "handle" and isinstance(child, str):
                yield child_location, child
            yield from find_handles(child, child_location)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from find_handles(child, f"{location}[{index}]")


def check_fixtures(directory: Path) -> list[str]:
    errors = []
    fixtures = sorted(directory.rglob("*.json"))
    for fixture in fixtures:
        try:
            data = json.loads(fixture.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            errors.append(f"{fixture}: cannot read JSON ({error})")
            continue
        for location, handle in find_handles(data):
            if not handle.endswith(SYNTHETIC_SUFFIX) or handle == SYNTHETIC_SUFFIX:
                errors.append(f"{fixture}:{location}: non-synthetic handle {handle!r}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "fixture_dir",
        nargs="?",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "tests" / "fixtures",
        help="directory to scan (defaults to tests/fixtures)",
    )
    args = parser.parse_args()

    errors = check_fixtures(args.fixture_dir)
    if errors:
        print("Synthetic fixture check failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print(f"Synthetic fixture check passed: {args.fixture_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
