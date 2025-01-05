import requests
import os
from pyspark.sql import SparkSession

# FTP Configuration
FTP_URL = "https://ftp.goit.study/neoversity/"
LOCAL_DIR = "./data/landing/"
BRONZE_DIR = "./data/bronze/"

def download_data(filename):
    """Download file from FTP server."""
    url = f"{FTP_URL}{filename}.csv"
    local_path = os.path.join(LOCAL_DIR, f"{filename}.csv")
    os.makedirs(LOCAL_DIR, exist_ok=True)
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {url} -> {local_path}")
    else:
        raise Exception(f"Failed to download {url}: {response.status_code}")
    return local_path

def process_to_bronze(filename):
    """Process raw data to Bronze Zone."""
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    local_path = os.path.join(LOCAL_DIR, f"{filename}.csv")
    bronze_path = os.path.join(BRONZE_DIR, filename)
    os.makedirs(bronze_path, exist_ok=True)
    df = spark.read.option("header", "true").csv(local_path)
    df.write.parquet(bronze_path, mode="overwrite")
    print(f"Saved to Bronze Zone: {bronze_path}")

if __name__ == "__main__":
    files = ["athlete_bio", "athlete_event_results"]
    for file in files:
        download_data(file)
        process_to_bronze(file)
