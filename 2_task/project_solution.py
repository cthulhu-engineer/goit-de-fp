import requests
import os

from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, avg, current_timestamp, col
from pyspark.sql.types import StringType
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# FTP Configuration
FTP_URL = "https://ftp.goit.study/neoversity/"
LOCAL_DIR = "./data/landing/"
BRONZE_DIR = "./data/bronze/"
SILVER_DIR = "./data/silver/"
GOLD_DIR = "./data/gold/"

# Utility functions
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
    df.show()

def process_to_silver(filename):
    """Process Bronze data to Silver Zone."""
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    bronze_path = os.path.join(BRONZE_DIR, filename)
    silver_path = os.path.join(SILVER_DIR, filename)
    os.makedirs(silver_path, exist_ok=True)

    def clean_text(text):
        """Remove non-alphanumeric characters."""
        import re
        return re.sub(r"[^a-zA-Z0-9,.\'\" ]", "", str(text))

    clean_text_udf = udf(clean_text, StringType())
    df = spark.read.parquet(bronze_path)
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))
    df = df.dropDuplicates()
    df.write.parquet(silver_path, mode="overwrite")
    print(f"Saved to Silver Zone: {silver_path}")
    df.show()

def process_to_gold():
    """Process data from silver to gold."""
    athlete_bio = spark.read.format("parquet").load("silver/athlete_bio")
    athlete_event_results = spark.read.format("parquet").load("silver/athlete_event_results")

    joined = athlete_event_results.alias("events").join(
        athlete_bio.alias("bio"),
        col("events.athlete_id") == col("bio.athlete_id")
    )

    selected = joined.select(
        col("events.sport").alias("sport"),
        col("events.medal").alias("medal"),
        col("bio.sex").alias("sex"),
        col("bio.country_noc").alias("country_noc"),
        col("bio.height").alias("height"),
        col("bio.weight").alias("weight")
    )

    aggregated = selected.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

    aggregated.show()

    aggregated.write.mode("overwrite").parquet("gold/avg_stats")


# Airflow DAG
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="data_lake_etl",
    default_args=default_args,
    description="ETL pipeline for Data Lake",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def landing_to_bronze():
        files = ["athlete_bio", "athlete_event_results"]
        for file in files:
            download_data(file)
            process_to_bronze(file)

    def bronze_to_silver():
        files = ["athlete_bio", "athlete_event_results"]
        for file in files:
            process_to_silver(file)

    def silver_to_gold():
        process_to_gold()

    task_landing_to_bronze = PythonOperator(
        task_id="landing_to_bronze",
        python_callable=landing_to_bronze,
    )

    task_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
    )

    task_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    task_landing_to_bronze >> task_bronze_to_silver >> task_silver_to_gold
