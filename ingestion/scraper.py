import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

url = "https://www.indeed.com/jobs?q=data+engineer&l="

headers = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print("Failed to retrieve page")
    exit()

soup = BeautifulSoup(response.text, "html.parser")
job_cards = soup.find_all("div", class_="job_seen_beacon")

jobs = []

for job in job_cards:
    title = job.find("h2", class_="jobTitle")
    company = job.find("span", class_="companyName")
    location = job.find("div", class_="companyLocation")

    jobs.append({
        "Job Title": title.text.strip() if title else "NA",
        "Company": company.text.strip() if company else "NA",
        "Location": location.text.strip() if location else "NA"
    })

df = pd.DataFrame(jobs)
df.to_csv("jobs.csv", index=False)

print(f"Scraped {len(df)} job postings successfully.")