import os
import requests
import tempfile
import zipfile
from pyspark.sql import SparkSession

KNMI_URL = "https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_260.zip"

# Temporary intermediate directories
TEMP_DIR = os.path.join(tempfile.gettempdir(), "knmi")
ZIP_FILE = os.path.join(TEMP_DIR, "etmgeg_260.zip")
EXTRACT_DIR = os.path.join(TEMP_DIR, "etmgeg_260")
EXTRACT_TXT = os.path.join(EXTRACT_DIR, "etmgeg_260.txt")

# Target directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
EXTRACT_CSV = os.path.join(BASE_DIR, "data/raw/raw_meteo_data.csv")

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(EXTRACT_CSV), exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("download_data") \
    .getOrCreate()

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

    # Locate the header row
    header_index = next(i for i, line in enumerate(lines) if line.startswith("# STN"))

    # Extract headers and remove `#`
    headers = [col.strip() for col in lines[header_index].replace("# ", "").strip().split(",")]

    # Create a new cleaned file (without metadata)
    CLEANED_TXT = os.path.join(os.path.join(EXTRACT_DIR, "cleaned_etmgeg_260.txt"))
    with open(CLEANED_TXT, "w") as file:
        file.writelines(lines[header_index + 1:])  # Write only data rows, skipping metadata

    # Read the data while skipping metadata and using extracted headers
    df = spark.read.option("delimiter", ",") \
        .option("header", "false") \
        .option("inferSchema", "true") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(CLEANED_TXT)\
            .toDF(*headers)

    # Return latest entry for visual inspection
    spark.createDataFrame(df.tail(1)).show()

    # Save as CSV
    df.write.mode("overwrite").option("header", "true").csv(EXTRACT_CSV)
    print(f"CSV file saved at: {EXTRACT_CSV}")


if __name__ == "__main__":
    get_daily_zip_file()
    extract_daily_zip_file()
    convert_txt_to_csv()