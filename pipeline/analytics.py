import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

STEPS = [
    "python spark/job_market_analysis.py",
    "python analytics/salary_analysis.py",
    "python analytics/skill_salary_analysis.py",
    "python analytics/salary_segmentation.py",
]

def run_analytics():
    for cmd in STEPS:
        print("⚙️ Running:", cmd)
        subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT, check=True)

if __name__ == "__main__":
    run_analytics()
