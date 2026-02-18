import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def run_cleaning():
    subprocess.run(
        "python processing/pandas_cleaner.py",
        shell=True,
        cwd=PROJECT_ROOT,
        check=True
    )

if __name__ == "__main__":
    run_cleaning()
