# ======================================================
# ONE COMMAND REAL-TIME PIPELINE
# ======================================================

import sys
from pathlib import Path
import time
import uuid

# ------------------------------------------------------
# Ensure project root on path (robust execution)
# ------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.ingest import run_ingestion
from pipeline.clean import run_cleaning
from pipeline.analytics import run_analytics

RUN_ID = str(uuid.uuid4())[:8]


def timed_step(name, fn):
    print("\n" + "=" * 60)
    print(f"🚀 [{RUN_ID}] {name}")
    print("=" * 60)

    start = time.time()
    fn()
    print(f"✅ {name} finished in {round(time.time()-start,2)}s")


if __name__ == "__main__":

    print(f"🔥 PIPELINE STARTED | RUN ID: {RUN_ID}")

    timed_step("Ingestion", run_ingestion)
    timed_step("Cleaning", run_cleaning)
    timed_step("Analytics", run_analytics)

    print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY")
