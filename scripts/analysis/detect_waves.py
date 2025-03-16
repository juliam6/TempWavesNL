import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as spark_sum, max as spark_max, min as spark_min, when
from pyspark.sql.window import Window

# Define base directory (two levels up from the current file)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Define data directories
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
RESULTS_DIR = os.path.join(DATA_DIR, "results")

# Define source path
CLEAN_CSV = os.path.join(PROCESSED_DIR, "clean_meteo_data.csv")

# Define target paths
HEATWAVES_CSV = os.path.join(RESULTS_DIR, "heatwaves.csv")
COLDWAVES_CSV = os.path.join(RESULTS_DIR, "coldwaves.csv")

# Ensure necessary directories exist
os.makedirs(RESULTS_DIR, exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder.appName("detect_waves").getOrCreate()

def detect_heatwaves(df):
    """Detects heatwaves and returns a DataFrame of valid heatwave events."""

    # Mark heatwave candidate days (Max_Temperature_C >= 25)
    df = df.withColumn("heatwave_candidate", (col("Max_Temperature_C") >= 25).cast("int"))

    # Create a block ID using window functions
    window_spec = Window.orderBy("Date")
    df = df.withColumn("prev_candidate", lag("heatwave_candidate", 1, 0).over(window_spec))
    df = df.withColumn("block", spark_sum((col("heatwave_candidate") != col("prev_candidate")).cast("int")).over(window_spec))

    # Aggregate by block and filter for valid heatwaves
    heatwave_df = df.filter(col("heatwave_candidate") == 1).groupBy("block").agg(
        spark_min("Date").alias("From Date"),
        spark_max("Date").alias("To Date incl"),
        spark_sum("heatwave_candidate").alias("Duration in days"),
        spark_sum((col("Max_Temperature_C") >= 30).cast("int")).alias("Tropical Days"),
        spark_max("Max_Temperature_C").alias("Max Temperature")
    ).filter((col("Duration in days") >= 5) & (col("Tropical Days") >= 3))

    return heatwave_df

def detect_coldwaves(df):
    """Detects coldwaves and returns a DataFrame of valid coldwave events."""

    # Mark coldwave candidate days (Max_Temperature_C < 0)
    df = df.withColumn("coldwave_candidate", (col("Max_Temperature_C") < 0).cast("int"))

    # Create a block ID using window functions
    window_spec = Window.orderBy("Date")
    df = df.withColumn("prev_candidate", lag("coldwave_candidate", 1, 0).over(window_spec))
    df = df.withColumn("block", spark_sum((col("coldwave_candidate") != col("prev_candidate")).cast("int")).over(window_spec))

    # Aggregate by block and filter for valid coldwaves
    coldwave_df = df.filter(col("coldwave_candidate") == 1).groupBy("block").agg(
        spark_min("Date").alias("From Date"),
        spark_max("Date").alias("To Date incl"),
        spark_sum("coldwave_candidate").alias("Duration in days"),
        spark_sum((col("Min_Temperature_C") < -10).cast("int")).alias("High Frost Days"),
        spark_max("Max_Temperature_C").alias("Max Temperature"),
        spark_min("Min_Temperature_C").alias("Min Temperature")
    ).filter((col("Duration in days") >= 5) & (col("High Frost Days") >= 3))

    return coldwave_df


def detect_waves():
    """Loads clean data, detects heatwaves/coldwaves, and outputs results."""

    # Load clean data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CLEAN_CSV)

    # Detect heatwaves/coldwaves
    heatwaves = detect_heatwaves(df)
    coldwaves = detect_coldwaves(df)

    # Save detected heatwaves/coldwaves
    heatwaves.write.mode("overwrite").option("header", "true").csv(HEATWAVES_CSV)
    coldwaves.write.mode("overwrite").option("header", "true").csv(COLDWAVES_CSV)
    print(f"Saved heatwaves to {HEATWAVES_CSV}")
    print(f"Saved coldwaves to {COLDWAVES_CSV}")


if __name__ == "__main__":
    detect_waves()