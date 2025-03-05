from fastapi import FastAPI
import pandas as pd
import os

app = FastAPI()

# Define file paths
RESULTS_DIR = "/app/data/results"
HEATWAVES_FILE = os.path.join(RESULTS_DIR, "heatwaves.csv")
COLDWAVES_FILE = os.path.join(RESULTS_DIR, "coldwaves.csv")
WAVES_TIMESERIES_FILE = os.path.join(RESULTS_DIR, "waves_timeseries.csv")

def read_csv(filepath):
    """Read a CSV file and return data as a list of dictionaries"""
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        return df.to_dict(orient="records")
    return {"error": "File not found"}

@app.get("/")
def home():
    return {"message": "TempWavesNL API is running. /heatwaves /coldwaves /waves_timeseries"}

@app.get("/heatwaves")
def get_heatwaves():
    """Return heatwave dataset"""
    return read_csv(HEATWAVES_FILE)

@app.get("/coldwaves")
def get_coldwaves():
    """Return coldwave dataset"""
    return read_csv(COLDWAVES_FILE)

@app.get("/waves_timeseries")
def get_waves_timeseries():
    """Return waves time series dataset"""
    return read_csv(WAVES_TIMESERIES_FILE)