import pandas as pd
import random

df = pd.read_csv("data/raw/jobs_api.csv")

locations = ["Gurgaon", "Bangalore", "Pune", "Hyderabad", "Remote"]
companies = ["TCS", "Infosys", "Samsung SDS", "Wipro", "Accenture"]

big_data = []

for i in range(1000):
    row = df.sample(1).iloc[0].to_dict()
    row["location"] = random.choice(locations)
    row["company"] = random.choice(companies)
    big_data.append(row)

big_df = pd.DataFrame(big_data)
big_df.to_csv("data/raw/jobs_big.csv", index=False)

print(f"✅ Simulated dataset size: {len(big_df)} rows")
