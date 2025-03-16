import os
from pyspark.sql import SparkSession
from fastapi import FastAPI

app = FastAPI()

# Define base directory (two levels up)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Define file paths
RESULTS_DIR = os.path.join(BASE_DIR, "data/results")
HEATWAVES_FILE = os.path.join(RESULTS_DIR, "heatwaves.csv")
COLDWAVES_FILE = os.path.join(RESULTS_DIR, "coldwaves.csv")

# Initialize Spark session
spark = SparkSession.builder.appName("detect_waves").getOrCreate()

def read_csv(filepath):
    """Read a CSV file with Spark and return data as a list of dictionaries"""
    if os.path.exists(filepath):
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        return df.limit(1000).toPandas().to_dict(orient="records")  # Limit rows to prevent API overload
    return {"error": "File not found"}

@app.get("/")
def home():
    return {"message": "TempWavesNL API is running. /heatwaves /coldwaves"}

@app.get("/heatwaves")
def get_heatwaves():
    """Return heatwave dataset"""
    return read_csv(HEATWAVES_FILE)

@app.get("/coldwaves")
def get_coldwaves():
    """Return coldwave dataset"""
    return read_csv(COLDWAVES_FILE)