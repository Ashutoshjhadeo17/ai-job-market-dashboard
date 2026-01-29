from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

QUERY = "data engineer"
PAGES = 3
BASE_URL = "https://www.indeed.com/jobs?q={}&start={}"

options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

jobs = []

def scroll_page():
    for _ in range(3):
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        time.sleep(2)

for page in range(PAGES):
    url = BASE_URL.format(QUERY.replace(" ", "+"), page * 10)
    driver.get(url)

    time.sleep(5)  # allow full JS load
    scroll_page()

    job_cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='job-card']")

    if not job_cards:
        job_cards = driver.find_elements(By.CLASS_NAME, "job_seen_beacon")

    print(f"Page {page+1}: Found {len(job_cards)} job cards")

    for job in job_cards:
        try:
            title = job.find_element(By.CSS_SELECTOR, "h2 span").text
        except:
            title = None

        try:
            company = job.find_element(By.CSS_SELECTOR, "[data-testid='company-name']").text
        except:
            company = None

        try:
            location = job.find_element(By.CSS_SELECTOR, "[data-testid='text-location']").text
        except:
            location = None

        try:
            summary = job.find_element(By.CLASS_NAME, "job-snippet").text.replace("\n", " ")
        except:
            summary = None

        if title and company:
            jobs.append({
                "Job Title": title,
                "Company": company,
                "Location": location,
                "Summary": summary
            })

driver.quit()

df = pd.DataFrame(jobs)
df.to_csv("jobs.csv", index=False)

print(f"\n✅ Successfully scraped {len(df)} REAL jobs")