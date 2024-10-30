import requests
import pandas as pd
import time
import os
from tqdm import tqdm

# Configuration
BASE_URL = "https://www.ebi.ac.uk/metagenomics/api/v1/samples"
OUTPUT_FILE = "output_data.csv"
TOTAL_PAGES = 17007


# Function to fetch data from a specific page
def fetch_page_data(page):
    response = requests.get(BASE_URL, params={'page': page})
    response.raise_for_status()
    return response.json()['data']


# Load last fetched page (if exists)
start_page = 1
if os.path.exists("progress_tracker.txt"):
    with open("progress_tracker.txt", "r") as f:
        start_page = int(f.read().strip()) + 1

# Create or load the CSV file
if os.path.exists(OUTPUT_FILE) and start_page > 1:
    # Load the existing CSV file
    data_df = pd.read_csv(OUTPUT_FILE)
else:
    # Create a new CSV file with column names
    data_df = pd.DataFrame(columns=["id", "attributes", "relationships"])

# Progress bar setup
progress_bar = tqdm(total=TOTAL_PAGES, initial=start_page - 1, desc="Fetching data")

# Fetch data from each page
for page in range(start_page, TOTAL_PAGES + 1):
    try:
        # Fetch and store data for the current page
        data = fetch_page_data(page)
        temp_df = pd.json_normalize(data)

        # Append data to main dataframe and save
        data_df = pd.concat([data_df, temp_df], ignore_index=True)
        data_df.to_csv(OUTPUT_FILE, index=False)

        # Save current progress
        with open("progress_tracker.txt", "w") as f:
            f.write(str(page))

        # Update progress bar
        progress_bar.update(1)
        progress_bar.set_postfix({
            "Page": page,
            "Completed": f"{page * 100 / TOTAL_PAGES:.2f}%",
            "Fetched": page,
            "Remaining": TOTAL_PAGES - page
        })

    except requests.RequestException as e:
        print(f"Request failed on page {page}. Retrying in 5 seconds...")
        time.sleep(5)  # Wait before retrying
        continue

progress_bar.close()
print("Data fetching complete.")

# Cleanup progress tracker after completion
if os.path.exists("progress_tracker.txt"):
    os.remove("progress_tracker.txt")