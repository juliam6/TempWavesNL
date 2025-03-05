import pandas as pd
import os

# Define source and target file paths
RAW_CSV = "/app/data/raw/raw_meteo_data.csv"
CLEAN_CSV = "/app/data/processed/clean_meteo_data.csv"

# Ensure target path exists
os.makedirs(os.path.dirname(CLEAN_CSV), exist_ok=True)

def clean_metro_data():
    print("Loading raw meteorological data...")

    # Load data
    df = pd.read_csv(RAW_CSV)
    print("Columns in the dataset:", df.columns)
    # Select date, max temp and min temp
    df = df[["YYYYMMDD", "TX", "TN"]]

    df.rename(columns={
        "YYYYMMDD": "Date",
        "TX": "Max_Temperature_C",
        "TN": "Min_Temperature_C"
    }, inplace=True)

    # Convert Date and filter from 2003 and onwards
    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
    df = df[df["Date"].dt.year >= 2003]

    # Convert temperatures from 0.1 of a degree to 1C
    df["Max_Temperature_C"] = df["Max_Temperature_C"] / 10
    df["Min_Temperature_C"] = df["Min_Temperature_C"] / 10

    # Save cleaned data
    df.to_csv(CLEAN_CSV, index=False)
    print(f"Cleaned meteorological data saved at: {CLEAN_CSV}")
    return df


if __name__ == "__main__":
    df = clean_metro_data()
    print(df.head())