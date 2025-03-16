import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Define base directory (two levels up)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Define source and target file paths
RAW_CSV = os.path.join(BASE_DIR, "data/raw/raw_meteo_data.csv")
CLEAN_CSV = os.path.join(BASE_DIR, "data/processed/clean_meteo_data.csv")

# Ensure target path exists
os.makedirs(os.path.dirname(CLEAN_CSV), exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder.appName("Clean Meteo Data").getOrCreate()

def clean_metro_data():
    print("Loading raw meteorological data...")

    # Load data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_CSV)

    # Select date, max temp and min temp
    df = df.select(
        col("YYYYMMDD").alias("Date"),
        col("TX").alias("Max_Temperature_C"),
        col("TN").alias("Min_Temperature_C")
    )

    # Convert Date column to actual date format
    df = df.withColumn("Date", to_date(col("Date").cast("string"), "yyyyMMdd"))

    # Filter out records before 2003
    df = df.filter(col("Date") >= "2003-01-01")

    # Convert temperatures from 0.1 degrees to 1C
    df = df.withColumn("Max_Temperature_C", col("Max_Temperature_C") / 10)
    df = df.withColumn("Min_Temperature_C", col("Min_Temperature_C") / 10)

    # Save cleaned data
    df.write.mode("overwrite").option("header", "true").csv(CLEAN_CSV)
    print(f"Cleaned meteorological data saved at: {CLEAN_CSV}")
    return df


if __name__ == "__main__":
    df = clean_metro_data()
    print(df.head())