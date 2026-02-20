# ==========================================================
# Continuous Production Pipeline Runner (Resilient Version)
# ==========================================================

import time
import traceback
from uuid import uuid4
from datetime import datetime

from pipeline.status import write_status
from pipeline.ingest import run_ingestion
from pipeline.clean import run_cleaning
from pipeline.analytics import run_analytics


# ==========================================================
# CONFIGURATION
# ==========================================================

BASE_INTERVAL = 600            # 10 minutes normal run
MAX_BACKOFF = 1800             # 30 minutes max delay
MAX_FAILURES_BEFORE_DEGRADED = 5


# ==========================================================
# PIPELINE EXECUTION
# ==========================================================

def run_pipeline():
    run_id = uuid4().hex[:8]

    print(f"\n🚀 [{run_id}] Pipeline Started")

    run_ingestion()
    run_cleaning()
    run_analytics()

    write_status("success", run_id)
    print(f"🎉 [{run_id}] Pipeline Completed Successfully")


# ==========================================================
# CONTINUOUS SCHEDULER WITH BACKOFF
# ==========================================================

if __name__ == "__main__":

    print("🛰️ Continuous Pipeline Scheduler Started")

    consecutive_failures = 0

    while True:
        try:
            run_pipeline()

            # Reset on success
            consecutive_failures = 0
            sleep_time = BASE_INTERVAL

        except Exception:
            consecutive_failures += 1

            error_message = traceback.format_exc()
            run_id = uuid4().hex[:8]

            print(f"❌ [{run_id}] Pipeline Failed")
            print(error_message)

            write_status(
                "failed",
                run_id,
                {
                    "error": error_message,
                    "consecutive_failures": consecutive_failures,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

            # Exponential backoff
            sleep_time = min(
                BASE_INTERVAL * (2 ** consecutive_failures),
                MAX_BACKOFF
            )

            if consecutive_failures >= MAX_FAILURES_BEFORE_DEGRADED:
                print("⚠️ SYSTEM DEGRADED — Too many consecutive failures")

        print(f"⏳ Sleeping {sleep_time} seconds...\n")
        time.sleep(sleep_time)
