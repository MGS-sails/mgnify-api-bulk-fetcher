import requests
import pandas as pd
import time
import os
from tqdm import tqdm

# Configuration
BASE_URL = "https://www.ebi.ac.uk/metagenomics/api/v1/samples?experiment_type=assembly&page_size=1"
CSV_OUTPUT_FILE = "output_data.csv"
TOTAL_PAGES = 361

# Function to fetch data from a specific page
def fetch_page_data(page):
    response = requests.get(BASE_URL, params={'page': page})
    response.raise_for_status()
    return response.json()['data']

# Load last fetched page (if exists)
start_page = 1
if os.path.exists("progress_tracker.txt"):
    with open("progress_tracker.txt", "r") as f:
        last_page = f.read().strip()
        if last_page:
            start_page = int(last_page) + 1

# Initialize CSV headers and data storage
columns = ["id"]
data_list = []

# Progress bar setup
progress_bar = tqdm(total=TOTAL_PAGES, initial=start_page - 1, desc="Fetching data")

# Fetch data from each page
for page in range(start_page, TOTAL_PAGES + 1):
    try:
        # Fetch data for the current page
        data = fetch_page_data(page)

        # Process each sample in the data
        for sample in data:
            sample_id = sample.get("id")
            metadata = sample["attributes"].get("sample-metadata", [])

            # Prepare a dictionary for the row data
            row_data = {"id": sample_id}

            # Extract sample-metadata as columns dynamically
            for item in metadata:
                key = item["key"]
                value = item["value"]

                # Add column if it's new
                if key not in columns:
                    columns.append(key)

                # Add data to row
                row_data[key] = value

            # Append row data to list
            data_list.append(row_data)

        # Convert to DataFrame and save CSV after each page
        data_df = pd.DataFrame(data_list, columns=columns)
        data_df.to_csv(CSV_OUTPUT_FILE, index=False)

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
