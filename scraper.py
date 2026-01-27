import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.indeed.com/jobs?q=data+engineer"
response = requests.get(url)
html_content = response.text
soup = BeautifulSoup(html_content, "html.parser")
job_titles = []
for job in soup.find_all("h2"):
    job_titles.append(job.text.strip())

df = pd.DataFrame(job_titles, columns=["Job Title"])
df.to_csv("jobs.csv", index=False)

print("Scrapping completed. jobs.csv created.")