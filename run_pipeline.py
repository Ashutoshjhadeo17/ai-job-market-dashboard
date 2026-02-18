# ======================================================
# ONE-COMMAND DATA PIPELINE RUNNER
# ======================================================

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# ------------------------------------------------------
# Pipeline Steps
# ------------------------------------------------------
PIPELINE_STEPS = [
    ("Collect Job Data", "python api_job_collector.py"),
    ("Clean Data", "python processing/pandas_cleaner.py"),
    ("Spark Analytics", "python spark/job_market_analysis.py"),
    ("Salary Analysis", "python analytics/salary_analysis.py"),
    ("Skill Salary Analysis", "python analytics/skill_salary_analysis.py"),
    ("Salary Segmentation", "python analytics/salary_segmentation.py"),
]

# ------------------------------------------------------
# Runner
# ------------------------------------------------------
def run_step(name, command):
    print("\n" + "=" * 60)
    print(f"🚀 STEP: {name}")
    print("=" * 60)

    result = subprocess.run(
        command,
        shell=True,
        cwd=PROJECT_ROOT
    )

    if result.returncode != 0:
        print(f"\n❌ FAILED at step: {name}")
        sys.exit(1)

    print(f"✅ Completed: {name}")

# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
if __name__ == "__main__":

    print("🔥 STARTING FULL AI JOB MARKET PIPELINE")

    for name, cmd in PIPELINE_STEPS:
        run_step(name, cmd)

    print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY")
    print("Now run:")
    print("   streamlit run dashboards/app.py")
