import pandas as pd
import os

# Define source path
CLEAN_CSV = "data/processed/clean_meteo_data.csv"

# Define target paths
HEATWAVES_CSV = "data/results/heatwaves.csv"
COLDWAVES_CSV = "data/results/coldwaves.csv"
WAVES_TIMESERIES_CSV = "data/results/waves_timeseries.csv"

# Ensure target path exists
os.makedirs(os.path.dirname(HEATWAVES_CSV), exist_ok=True)

def detect_heatwaves(df):
    """Detects heatwaves and returns a DataFrame of valid heatwave events."""

    # Mark days >= 25 degrees as potential candidates
    df["heatwave_candidate"] = df["Max_Temperature_C"] >= 25

    # Create blocks of consecutive heatwave days
    df["block"] = (df["heatwave_candidate"] != df["heatwave_candidate"].shift()).cumsum()

    # Group by and validate whether the blocks meet the requirements
    valid_heatwaves = []
    heatwave_blocks = df[df["heatwave_candidate"]].groupby("block")
    for block_id, group in heatwave_blocks:
        if len(group) >= 5 and (group["Max_Temperature_C"] >= 30).sum() >= 3:
            valid_heatwaves.append([
                group["Date"].min(),  # Start date
                group["Date"].max(),  # End date
                len(group),  # Duration
                (group["Max_Temperature_C"] >= 30).sum(),  # Number of tropical days
                group["Max_Temperature_C"].max()  # Maximum temperature
            ])

    # Return as Dataframe
    return pd.DataFrame(valid_heatwaves, columns=["From Date", "To Date incl", "Duration in days", "Tropical Days", "Max Temperature"])

def detect_coldwaves(df):
    """Detects coldwaves and returns a DataFrame of valid coldwave events."""

    # Mark days < 0 degrees as potential candidates
    df["coldwave_candidate"] = df["Max_Temperature_C"] < 0

    # Create blocks of consecutive coldwave days
    df["block"] = (df["coldwave_candidate"] != df["coldwave_candidate"].shift()).cumsum()

    # Group by and validate whether the blocks meet the requirements
    valid_coldwaves = []
    coldwave_blocks = df[df["coldwave_candidate"]].groupby("block")
    for block_id, group in coldwave_blocks:
        if len(group) >= 5 and (group["Min_Temperature_C"] < -10).sum() >= 3:
            valid_coldwaves.append([
                group["Date"].min(),  # Start date
                group["Date"].max(),  # End date
                len(group),  # Duration
                (group["Min_Temperature_C"] < -10).sum(),  # High frost days
                group["Max_Temperature_C"].max(),  # Max temperature
                group["Min_Temperature_C"].min()  # Min temperature
            ])

    # Return as Dataframe
    return pd.DataFrame(valid_coldwaves, columns=["From Date", "To Date incl", "Duration in days", "High Frost Days", "Max Temperature", "Min Temperature"])


def detect_waves():
    """Loads clean data, detects heatwaves/coldwaves, and outputs results."""

    # Load clean data
    df = pd.read_csv(CLEAN_CSV, parse_dates=["Date"])

    # Detect heatwaves/coldwaves
    heatwaves = detect_heatwaves(df)
    coldwaves = detect_coldwaves(df)

    # Save detected heatwaves/coldwaves
    heatwaves.to_csv(HEATWAVES_CSV, index=False)
    coldwaves.to_csv(COLDWAVES_CSV, index=False)
    print(f"Saved heatwaves to {HEATWAVES_CSV}")
    print(f"Saved coldwaves to {COLDWAVES_CSV}")

    # Mark heatwaves and coldwaves in full time series
    df["Event Type"] = "No Extreme Event"
    
    for _, row in heatwaves.iterrows():
        df.loc[df["Date"].between(row["From Date"], row["To Date incl"]), "Event Type"] = "Heatwave"
    
    for _, row in coldwaves.iterrows():
        df.loc[df["Date"].between(row["From Date"], row["To Date incl"]), "Event Type"] = "Coldwave"

    # Save time series
    df.to_csv(WAVES_TIMESERIES_CSV, index=False)
    print(f"Saved time series to {WAVES_TIMESERIES_CSV}")

if __name__ == "__main__":
    detect_waves()