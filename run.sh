#!/usr/bin/env bash
set -euo pipefail
uvicorn app.rbi_api:app --reload --port 8000
