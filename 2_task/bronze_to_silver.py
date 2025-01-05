import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Directory paths
BRONZE_DIR = "./data/bronze/"
SILVER_DIR = "./data/silver/"

def clean_text(text):
    """Remove non-alphanumeric characters."""
    import re
    return re.sub(r"[^a-zA-Z0-9,.\'\" ]", "", str(text))

def process_to_silver(filename):
    """Process Bronze data to Silver Zone."""
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    bronze_path = os.path.join(BRONZE_DIR, filename)
    silver_path = os.path.join(SILVER_DIR, filename)
    os.makedirs(silver_path, exist_ok=True)

    clean_text_udf = udf(clean_text, StringType())
    df = spark.read.parquet(bronze_path)
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))
    df = df.dropDuplicates()
    df.write.parquet(silver_path, mode="overwrite")
    print(f"Saved to Silver Zone: {silver_path}")

if __name__ == "__main__":
    files = ["athlete_bio", "athlete_event_results"]
    for file in files:
        process_to_silver(file)
