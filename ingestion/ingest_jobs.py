import uuid
import pandas as pd
from datetime import datetime
from schemas.job_schema import JOB_SCHEMA

def normalize_jobs(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()

    df["job_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df["ingested_at"] = datetime.utcnow()

    for col, dtype in JOB_SCHEMA.items():
        if col not in df.columns:
            df[col] = None

    df = df[list(JOB_SCHEMA.keys())]

    return df
