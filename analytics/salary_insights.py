import pandas as pd


# --------------------------------------------------
# CLEAN SALARY DATA
# --------------------------------------------------
def prepare_salary_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a safe salary dataset.
    Removes junk values and computes midpoint salary.
    """

    if df.empty:
        return df

    required = {"salary_min", "salary_max", "job_title", "location"}

    if not required.issubset(df.columns):
        return pd.DataFrame()

    salary_df = df.copy()

    # Remove invalid rows
    salary_df = salary_df[
        (salary_df["salary_min"] > 0) &
        (salary_df["salary_max"] > 0)
    ]

    if salary_df.empty:
        return salary_df

    salary_df["avg_salary"] = (
        salary_df["salary_min"] + salary_df["salary_max"]
    ) / 2

    return salary_df


# --------------------------------------------------
# TOP PAYING JOBS
# --------------------------------------------------
def highest_paying_roles(df: pd.DataFrame, n=15):

    if df.empty:
        return df

    return (
        df.groupby("job_title")["avg_salary"]
        .mean()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )


# --------------------------------------------------
# TOP PAYING LOCATIONS
# --------------------------------------------------
def highest_paying_locations(df: pd.DataFrame, n=15):

    if df.empty:
        return df

    return (
        df.groupby("location")["avg_salary"]
        .mean()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )


# --------------------------------------------------
# SALARY DISTRIBUTION
# --------------------------------------------------
def salary_distribution(df: pd.DataFrame):

    if df.empty:
        return df

    return df["avg_salary"]
