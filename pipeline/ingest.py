# ======================================================
# INDUSTRY-STYLE INGESTION PIPELINE
# ======================================================

from pathlib import Path
import pandas as pd
import requests
from datetime import datetime

from pipeline.utils import generate_job_id, ensure_dir, safe_read_parquet

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "raw" / "bronze"
BRONZE_FILE = BRONZE_PATH / "jobs_raw.parquet"


# ------------------------------------------------------
# Source 1 — RemoteOK
# ------------------------------------------------------
def fetch_remoteok():
    url = "https://remoteok.com/api"
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(url, headers=headers, timeout=30)
    data = r.json()[1:]

    rows = []
    for j in data:
        rows.append({
            "job_title": j.get("position"),
            "company": j.get("company"),
            "location": j.get("location"),
            "skills": ",".join(j.get("tags", [])),
            "salary_raw": j.get("salary"),
            "source": "remoteok",
        })

    return pd.DataFrame(rows)


# ------------------------------------------------------
# Source 2 — Arbeitnow
# ------------------------------------------------------
def fetch_arbeitnow():
    url = "https://www.arbeitnow.com/api/job-board-api"

    r = requests.get(url, timeout=30)
    data = r.json().get("data", [])

    rows = []
    for j in data:
        rows.append({
            "job_title": j.get("title"),
            "company": j.get("company_name"),
            "location": j.get("location"),
            "skills": ",".join(j.get("tags", [])),
            "salary_raw": None,
            "source": "arbeitnow",
        })

    return pd.DataFrame(rows)


# ------------------------------------------------------
# Main Ingestion
# ------------------------------------------------------
def run_ingestion():

    print("🌐 Fetching latest jobs...")

    df_new = pd.concat(
        [fetch_remoteok(), fetch_arbeitnow()],
        ignore_index=True
    )

    df_new["ingested_at"] = datetime.utcnow()
    df_new["job_id"] = df_new.apply(generate_job_id, axis=1)

    ensure_dir(BRONZE_PATH)

    df_existing = safe_read_parquet(BRONZE_FILE)

    df_all = pd.concat([df_existing, df_new], ignore_index=True)

    df_all = df_all.drop_duplicates(subset=["job_id"])

    df_all.to_parquet(BRONZE_FILE, index=False)

    print(f"✅ Bronze table updated: {len(df_all)} jobs")


if __name__ == "__main__":
    run_ingestion()
