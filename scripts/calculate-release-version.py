#!/usr/bin/env python3
"""Calculate the next release version based on bump type and prerelease settings.

Port of getsentry/vitest-evals' calculate-release-version.mjs for Python projects.
"""

from __future__ import annotations

import re
import sys
from typing import TypedDict


class Version(TypedDict):
    major: int
    minor: int
    patch: int
    prerelease: list[str]


ALLOWED_BUMPS = {"patch", "minor", "major"}
ALLOWED_PRERELEASE_IDS = {"beta", "rc", "alpha"}

VERSION_RE = re.compile(
    r"^(\d+)\.(\d+)\.(\d+)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


def parse_version(value: str) -> Version | None:
    m = VERSION_RE.match(value)
    if not m:
        return None
    return Version(
        major=int(m.group(1)),
        minor=int(m.group(2)),
        patch=int(m.group(3)),
        prerelease=m.group(4).split(".") if m.group(4) else [],
    )


def format_version(v: Version, prerelease_parts: list[str] | None = None) -> str:
    base = f"{v['major']}.{v['minor']}.{v['patch']}"
    if prerelease_parts:
        return f"{base}-{'.'.join(prerelease_parts)}"
    return base


def bump_stable_base(v: Version, bump_type: str) -> Version:
    if bump_type == "major":
        return Version(major=v["major"] + 1, minor=0, patch=0, prerelease=[])
    elif bump_type == "minor":
        return Version(major=v["major"], minor=v["minor"] + 1, patch=0, prerelease=[])
    else:  # patch
        return Version(major=v["major"], minor=v["minor"], patch=v["patch"] + 1, prerelease=[])


def next_stable_version(v: Version, bump_type: str) -> str:
    # If currently a prerelease, strip the suffix to get the stable release.
    if v["prerelease"]:
        return format_version(v)
    return format_version(bump_stable_base(v, bump_type))


def next_prerelease_version(v: Version, bump_type: str, pre_id: str) -> str:
    # From stable: bump base then add prerelease suffix.
    if not v["prerelease"]:
        return format_version(bump_stable_base(v, bump_type), [pre_id, "0"])

    current_id = v["prerelease"][0]
    last_part = v["prerelease"][-1]

    # Different prerelease id or non-numeric suffix: reset to .0
    if current_id != pre_id or not re.match(r"^\d+$", last_part):
        return format_version(v, [pre_id, "0"])

    # Same prerelease id: increment the numeric suffix.
    return format_version(v, v["prerelease"][:-1] + [str(int(last_part) + 1)])


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: calculate-release-version.py <current> <patch|minor|major> [true|false] [prerelease-id]",
            file=sys.stderr,
        )
        sys.exit(1)

    current = sys.argv[1]
    bump = sys.argv[2]
    prerelease = len(sys.argv) > 3 and sys.argv[3].lower() == "true"
    prerelease_id = sys.argv[4] if len(sys.argv) > 4 else "beta"

    if bump not in ALLOWED_BUMPS:
        print(f"Invalid bump: {bump}", file=sys.stderr)
        sys.exit(1)

    if prerelease_id not in ALLOWED_PRERELEASE_IDS:
        print(f"Invalid prerelease id: {prerelease_id}", file=sys.stderr)
        sys.exit(1)

    version = parse_version(current)
    if version is None:
        print(f"Invalid current version: {current}", file=sys.stderr)
        sys.exit(1)

    if prerelease:
        result = next_prerelease_version(version, bump, prerelease_id)
    else:
        result = next_stable_version(version, bump)

    print(result)


if __name__ == "__main__":
    main()
