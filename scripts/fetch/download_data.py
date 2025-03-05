import os
import requests
import tempfile
import zipfile
import pandas as pd

DATA_PATH = "/opt/airflow/data/"
RAW_FILE = os.path.join(DATA_PATH, "temperature_data.csv")


KNMI_URL = "https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_260.zip"
TEMP_DIR = os.path.join(tempfile.gettempdir(), "knmi")
ZIP_FILE = os.path.join(TEMP_DIR, "etmgeg_260.zip")
EXTRACT_DIR = os.path.join(TEMP_DIR, "etmgeg_260")
EXTRACT_TXT = os.path.join(EXTRACT_DIR, "etmgeg_260.txt")
EXTRACT_CSV = "data/raw/raw_meteo_data.csv"

os.makedirs(TEMP_DIR, exist_ok=True)

def get_daily_zip_file():
    print("Downloading all available data from the De Bilt weather station...")
    r = requests.get(KNMI_URL, stream=True)
    if r.status_code == 200:
        with open(ZIP_FILE, "wb") as file:
            for chunk in r.iter_content(chunk_size=128):
                file.write(chunk)
        print(f"Downloaded: {ZIP_FILE}")
    else:
        print("Failed to download file:", r.status_code)
        return False
    return True

def extract_daily_zip_file():
    print("Extracting data...")
    with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)
    print(f"Extracted to: {EXTRACT_DIR}")

def convert_txt_to_csv():
    print("Converting TXT file to CSV...")

    # Find the correct starting row (column headers)
    with open(EXTRACT_TXT, "r") as file:
        lines = file.readlines()

    # Find the line that starts with "# STN" (column headers)
    for i, line in enumerate(lines):
        if line.startswith("# STN"):
            header_index = i
            break

    # Read headers separately, removing `#` and whitespace
    headers = [col.strip() for col in lines[header_index].replace("#", "").strip().split(",")]

    # Read the data while skipping metadata and using extracted headers
    df = pd.read_csv(EXTRACT_TXT, skiprows=header_index + 1, sep=",", names=headers, na_values=["     ", "", " "])
    print("Columns in the dataset:", df.columns)
    # Save as CSV
    df.to_csv(EXTRACT_CSV, index=False)
    print(f"CSV file saved at: {EXTRACT_CSV}")


if __name__ == "__main__":
    get_daily_zip_file()
    extract_daily_zip_file()
    convert_txt_to_csv()