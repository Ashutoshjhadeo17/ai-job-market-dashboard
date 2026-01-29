import requests
import pandas as pd
from datetime import datetime
import uuid

url = "https://remoteok.com/api"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)
data = response.json()

jobs_raw = data[1:]  # first element is metadata

jobs = []

for job in jobs_raw:
    jobs.append({
        "job_id": str(uuid.uuid4()),
        "job_title": job.get("position"),
        "company": job.get("company"),
        "location": job.get("location"),
        "skills": ", ".join(job.get("tags", [])),
        "date_posted": job.get("date"),
        "salary_raw": job.get("salary"),
        "job_type": job.get("employment_type"),
        "source": "RemoteOK_API",
        "ingested_at": datetime.utcnow()
    })

df = pd.DataFrame(jobs)

df.to_csv("data/raw/jobs_api.csv", index=False)

print(f"✅ Collected {len(df)} real job postings via API")
