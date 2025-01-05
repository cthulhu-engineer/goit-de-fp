import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

# Directory paths
SILVER_DIR = "./data/silver/"
GOLD_DIR = "./data/gold/"

def process_to_gold():
    """Aggregate data and save to Gold Zone."""
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    athlete_bio_path = os.path.join(SILVER_DIR, "athlete_bio")
    athlete_event_path = os.path.join(SILVER_DIR, "athlete_event_results")
    gold_path = os.path.join(GOLD_DIR, "avg_stats")
    os.makedirs(gold_path, exist_ok=True)

    bio_df = spark.read.parquet(athlete_bio_path)
    event_df = spark.read.parquet(athlete_event_path)

    result_df = event_df.join(bio_df, "athlete_id").groupBy(
        "sport", "medal", "sex", "country_noc"
    ).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )
    result_df.write.parquet(gold_path, mode="overwrite")
    print(f"Saved to Gold Zone: {gold_path}")

if __name__ == "__main__":
    process_to_gold()
