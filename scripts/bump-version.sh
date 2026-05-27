#!/bin/bash
set -euo pipefail

OLD_VERSION="${1}"
NEW_VERSION="${2}"

uv version "${NEW_VERSION}"
uv lock
