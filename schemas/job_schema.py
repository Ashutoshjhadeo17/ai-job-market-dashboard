from typing import Dict

JOB_SCHEMA: Dict[str, str] = {
    "job_id": "string",
    "job_title": "string",
    "company": "string",
    "location": "string",
    "skills": "string",
    "date_posted": "datetime64[ns]",
    "job_type": "string",
    "source": "string",
    "salary_min": "int64",
    "salary_max": "int64",
    "ingested_at": "datetime64[ns]"
}
