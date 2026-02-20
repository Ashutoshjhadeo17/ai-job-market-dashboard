# ======================================================
# Pipeline Status Tracking (Production Style)
# ======================================================

from pathlib import Path
import json
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]

STATUS_FILE = PROJECT_ROOT / "output" / "pipeline_status.json"


def write_status(status: str, run_id: str, details: dict = None):
    payload = {
        "status": status,
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "details": details or {}
    }

    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(STATUS_FILE, "w") as f:
        json.dump(payload, f, indent=2)


def read_status():
    if not STATUS_FILE.exists():
        return None

    try:
        with open(STATUS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None
